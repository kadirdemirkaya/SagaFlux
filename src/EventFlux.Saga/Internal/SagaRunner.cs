using System.Diagnostics;
using EventFlux.Saga.Concurrency;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.Internal;

internal interface ISagaRunner
{
    bool Handles(Type eventType);

    Task RunAsync(object @event, CancellationToken ct);
}

internal sealed class SagaRunner<TSaga, TState>(
    TSaga saga,
    ISagaRepository<TState> repository,
    ISagaTransport transport,
    ISagaTimeoutStore timeouts,
    TimeProvider time,
    IEnumerable<ISagaTransitionObserver> observers,
    SagaFluxOptions options,
    ISagaLockProvider locks,
    ISagaRequester requester,
    ILogger<SagaRunner<TSaga, TState>> logger,
    ISagaOutbox? outbox = null) : ISagaRunner
    where TSaga : Saga<TState>
    where TState : class, ISagaState, new()
{
    public bool Handles(Type eventType) => saga.Handles(eventType);

    public async Task RunAsync(object @event, CancellationToken ct)
    {
        var correlationId = saga.GetCorrelationId(@event);
        var sagaName = typeof(TSaga).Name;
        var eventName = @event.GetType().Name;

        using var activity = SagaFluxDiagnostics.Source.StartActivity($"{sagaName} {eventName}");
        activity?.SetTag(SagaFluxDiagnostics.TagSaga, sagaName);
        activity?.SetTag(SagaFluxDiagnostics.TagEvent, eventName);
        activity?.SetTag(SagaFluxDiagnostics.TagCorrelationId, correlationId.ToString());

        try
        {
            IReadOnlyList<(SagaPublishedEvent Event, Guid? OutboxId)> published;
            var handle = await locks.AcquireAsync(sagaName, correlationId, ct).ConfigureAwait(false);
            await using (handle.ConfigureAwait(false))
            {
                published = await TransitionAsync(correlationId, @event, activity, ct).ConfigureAwait(false);
            }

            // Sent outside the lock: an in-process transport may route these back to this same instance.
            await SendAsync(published, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (activity is not null)
        {
            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
            activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                ["exception.type"] = ex.GetType().FullName,
                ["exception.message"] = ex.Message,
            }));
            throw;
        }
    }

    private async Task SendAsync(IReadOnlyList<(SagaPublishedEvent Event, Guid? OutboxId)> published, CancellationToken ct)
    {
        if (outbox is null)
        {
            foreach (var (e, _) in published)
                await e.SendAsync(transport, ct).ConfigureAwait(false);
            return;
        }

        // With an outbox the events are already saved: a send failure is left to the outbox dispatcher.
        var sent = new List<Guid>(published.Count);
        try
        {
            foreach (var (e, id) in published)
            {
                await e.SendAsync(transport, ct).ConfigureAwait(false);
                sent.Add(id!.Value);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            logger.LogWarning(ex, "Saga {Saga} could not send {Count} outbox message(s) now; the outbox dispatcher will retry.",
                typeof(TSaga).Name, published.Count - sent.Count);
        }

        if (sent.Count > 0)
            await outbox.CompleteAsync(sent, ct).ConfigureAwait(false);
    }

    private async Task<IReadOnlyList<(SagaPublishedEvent Event, Guid? OutboxId)>> TransitionAsync(Guid correlationId, object @event, Activity? activity, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            var existing = await repository.FindAsync(correlationId, ct).ConfigureAwait(false);
            var result = await saga.ExecuteAsync(existing, @event, requester, ct).ConfigureAwait(false);
            activity?.SetTag(SagaFluxDiagnostics.TagHandled, result.Handled);
            activity?.SetTag(SagaFluxDiagnostics.TagFromState, result.FromState);
            activity?.SetTag(SagaFluxDiagnostics.TagAttempts, attempt);
            if (!result.Handled)
            {
                logger.LogDebug("Saga {Saga} ignored {Event} for {CorrelationId} in state {State}.",
                    typeof(TSaga).Name, @event.GetType().Name, correlationId, result.FromState);
                return [];
            }

            var published = result.Published
                .Select(p => (Event: p, OutboxId: outbox is null ? (Guid?)null : Guid.NewGuid()))
                .ToList();
            if (outbox is not null && published.Count > 0)
            {
                var now = time.GetUtcNow();
                outbox.Stage(published.Select(p => new SagaOutboxMessage(p.OutboxId!.Value, p.Event.Event, now)).ToList());
            }

            try
            {
                await repository.SaveAsync(result.State!, ct).ConfigureAwait(false);
            }
            catch (SagaConcurrencyException) when (attempt < options.MaxConcurrencyRetries)
            {
                outbox?.Discard();
                logger.LogDebug("Saga {Saga} instance {CorrelationId} changed concurrently, retrying ({Attempt}).",
                    typeof(TSaga).Name, correlationId, attempt);
                continue;
            }
            catch
            {
                outbox?.Discard();
                throw;
            }

            logger.LogDebug("Saga {Saga} {CorrelationId}: {From} -> {To} on {Event}.",
                typeof(TSaga).Name, correlationId, result.FromState, result.ToState, @event.GetType().Name);

            var removed = result.ToState == SagaStates.Final.Name && saga.RemovesWhenFinal;
            if (removed)
                await repository.RemoveAsync(correlationId, ct).ConfigureAwait(false);

            if (activity is not null)
            {
                activity.SetTag(SagaFluxDiagnostics.TagToState, result.ToState);
                activity.SetTag(SagaFluxDiagnostics.TagVersion, result.State!.Version);
                if (result.Fault is { } fault) activity.SetTag(SagaFluxDiagnostics.TagFault, fault.GetType().Name);
                if (removed) activity.SetTag(SagaFluxDiagnostics.TagRemoved, true);
            }

            await ApplyTimeoutsAsync(result, ct).ConfigureAwait(false);
            await NotifyAsync(result, @event, removed, ct).ConfigureAwait(false);
            return published;
        }
    }

    private async Task NotifyAsync(SagaExecutionResult<TState> result, object @event, bool removed, CancellationToken ct)
    {
        SagaTransitionRecord? record = null;
        foreach (var observer in observers)
        {
            record ??= new SagaTransitionRecord(
                result.State!.CorrelationId,
                typeof(TSaga).Name,
                @event.GetType().Name,
                result.FromState,
                result.ToState,
                result.Created,
                result.State.Version,
                result.Published.Select(p => p.Event.GetType().Name).ToArray(),
                result.Compensated.Select(c => c.CompensationActionKey).ToArray(),
                result.Scheduled.Select(s => s.Key).ToArray(),
                result.Unscheduled.ToArray(),
                time.GetUtcNow())
            {
                Fault = result.Fault is { } fault ? $"{fault.GetType().Name}: {fault.Message}" : null,
                Removed = removed,
            };
            try
            {
                await observer.OnTransitionAsync(record, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Saga transition observer {Observer} failed for {CorrelationId}.",
                    observer.GetType().Name, record.CorrelationId);
            }
        }
    }

    private async Task ApplyTimeoutsAsync(SagaExecutionResult<TState> result, CancellationToken ct)
    {
        var correlationId = result.State!.CorrelationId;
        if (result.ToState == SagaStates.Final.Name)
        {
            await timeouts.CancelAllAsync(correlationId, ct).ConfigureAwait(false);
            return;
        }

        foreach (var key in result.Unscheduled)
            await timeouts.CancelAsync(correlationId, key, ct).ConfigureAwait(false);

        var now = time.GetUtcNow();
        foreach (var scheduled in result.Scheduled)
        {
            var timeout = new SagaTimeout(Guid.NewGuid(), correlationId, scheduled.Key, now + scheduled.Delay, scheduled.Event);
            await timeouts.ScheduleAsync(timeout, ct).ConfigureAwait(false);
        }
    }
}
