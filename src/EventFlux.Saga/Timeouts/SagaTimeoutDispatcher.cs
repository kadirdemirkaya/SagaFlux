using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.Timeouts;

internal sealed class SagaTimeoutDispatcher(
    IServiceScopeFactory scopeFactory,
    TimeProvider time,
    SagaFluxOptions options,
    ILogger<SagaTimeoutDispatcher> logger) : ISagaTimeoutDispatcher
{
    public async Task<int> DispatchDueAsync(CancellationToken ct = default)
    {
        IReadOnlyList<SagaTimeout> due;
        await using (var scope = scopeFactory.CreateAsyncScope())
        {
            var store = scope.ServiceProvider.GetRequiredService<ISagaTimeoutStore>();
            due = await store.GetDueAsync(time.GetUtcNow(), options.TimeoutBatchSize, ct).ConfigureAwait(false);
        }

        var delivered = 0;
        foreach (var timeout in due)
        {
            ct.ThrowIfCancellationRequested();
            await using var scope = scopeFactory.CreateAsyncScope();
            var store = scope.ServiceProvider.GetRequiredService<ISagaTimeoutStore>();
            try
            {
                await scope.ServiceProvider.GetRequiredService<ISagaEventRouter>().RouteAsync(timeout.Event, ct).ConfigureAwait(false);
                await store.CompleteAsync(timeout.Id, ct).ConfigureAwait(false);
                delivered++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                logger.LogError(ex, "Saga timeout {Key} for {CorrelationId} failed; retrying in {Delay}.",
                    timeout.Key, timeout.CorrelationId, options.TimeoutRetryDelay);
                var retry = timeout with { Id = Guid.NewGuid(), DueAt = time.GetUtcNow() + options.TimeoutRetryDelay };
                await store.ScheduleAsync(retry, CancellationToken.None).ConfigureAwait(false);
            }
        }

        return delivered;
    }
}
