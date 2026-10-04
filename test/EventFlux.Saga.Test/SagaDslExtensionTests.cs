using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Repositories;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class SagaDslExtensionTests
{
    public sealed record TicketOpened(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record TicketEscalated(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record TicketClosed(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record TicketPinged(Guid CorrelationId, string Mode) : ICorrelatedEvent;
    public sealed record TicketAudit(Guid TicketId, string Note) : IEventRequest;

    public sealed class TicketState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int Pings { get; set; }
    }

    public sealed class TicketSaga : Saga<TicketState>
    {
        public static readonly SagaState Open = SagaStates.Define(nameof(Open));
        public static readonly SagaState Escalated = SagaStates.Define(nameof(Escalated));
        public static readonly SagaState Broken = SagaStates.Define(nameof(Broken));

        public TicketSaga()
        {
            RemoveWhenFinal();

            During(SagaStates.Initial, When<TicketOpened>().TransitionTo(Open));
            During(Open, When<TicketEscalated>().TransitionTo(Escalated));
            // State-specific transition for TicketClosed in Escalated: must win over the active one.
            During(Escalated, When<TicketClosed>().Publish(ctx => new TicketAudit(ctx.CorrelationId, "closed after escalation")).Finalize());

            DuringActive(
                When<TicketClosed>().Publish(ctx => new TicketAudit(ctx.CorrelationId, "closed")).Finalize(),
                When<TicketPinged>()
                    .Then(ctx => ctx.State.Pings++)
                    .Then(ctx =>
                    {
                        if (ctx.Event.Mode == "invalid") throw new InvalidOperationException("invalid ping");
                        if (ctx.Event.Mode == "timeout") throw new TimeoutException("slow");
                        if (ctx.Event.Mode == "cancel") ctx.CancellationToken.ThrowIfCancellationRequested();
                    })
                    .OnFault<InvalidOperationException>(f => f
                        .Publish(ctx => new TicketAudit(ctx.CorrelationId, ctx.Fault!.Message))
                        .TransitionTo(Broken))
                    .OnFault<OperationCanceledException>(f => f.TransitionTo(Broken)));
        }
    }

    public sealed class Records : ISagaTransitionObserver
    {
        public ConcurrentQueue<SagaTransitionRecord> All { get; } = new();

        public Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct)
        {
            All.Enqueue(record);
            return Task.CompletedTask;
        }
    }

    public sealed class AuditHandler(Recorder recorder) : IEventHandler<TicketAudit>
    {
        public Task Handle(TicketAudit @event, CancellationToken cancellationToken)
        {
            recorder.Events.Enqueue(@event);
            return Task.CompletedTask;
        }
    }

    private static List<TicketAudit> Audits(IServiceProvider provider)
        => provider.GetRequiredService<Recorder>().Events.OfType<TicketAudit>().ToList();

    private static ServiceProvider Build(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Records>();
        services.AddSingleton<ISagaTransitionObserver>(sp => sp.GetRequiredService<Records>());
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(SagaDslExtensionTests).Assembly);
        services.AddSagaFlux().AddSaga<TicketSaga, TicketState>();
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task A_state_specific_transition_wins_over_an_active_one()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var escalated = Guid.NewGuid();
        var open = Guid.NewGuid();

        foreach (var e in new object[] { new TicketOpened(escalated), new TicketEscalated(escalated), new TicketClosed(escalated), new TicketOpened(open), new TicketClosed(open) })
            await dispatcher.PublishAsync((IEventRequest)e);

        var audits = Audits(provider);
        Assert.Equal("closed after escalation", audits.Single(a => a.TicketId == escalated).Note);
        Assert.Equal("closed", audits.Single(a => a.TicketId == open).Note);
    }

    [Fact]
    public async Task RemoveWhenFinal_removes_the_instance_and_the_timeline_keeps_its_history()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var repository = (InMemorySagaRepository<TicketState>)provider.GetRequiredService<ISagaRepository<TicketState>>();
        var id = Guid.NewGuid();

        await dispatcher.PublishAsync(new TicketOpened(id));
        Assert.NotNull(await repository.FindAsync(id, default));
        await dispatcher.PublishAsync(new TicketClosed(id));

        Assert.Null(await repository.FindAsync(id, default));
        Assert.Equal(0, repository.Count);
        var records = provider.GetRequiredService<Records>().All.Where(r => r.CorrelationId == id).ToList();
        Assert.Equal(new[] { false, true }, records.Select(r => r.Removed));
        Assert.Equal(SagaStates.Final.Name, records[^1].ToState);
    }

    [Fact]
    public async Task A_handled_fault_is_saved_and_recorded()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();
        await dispatcher.PublishAsync(new TicketOpened(id));

        await dispatcher.PublishAsync(new TicketPinged(id, "invalid"));

        var state = (await provider.GetRequiredService<ISagaRepository<TicketState>>().FindAsync(id, default))!;
        Assert.Equal(TicketSaga.Broken.Name, state.CurrentState);
        Assert.Equal(1, state.Pings); // state changes of the failed steps are kept
        Assert.Equal("invalid ping", Audits(provider).Single().Note);
        Assert.Equal("InvalidOperationException: invalid ping", provider.GetRequiredService<Records>().All.Last().Fault);
    }

    [Fact]
    public async Task An_unhandled_fault_fails_the_event_and_saves_nothing()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();
        await dispatcher.PublishAsync(new TicketOpened(id));

        await Assert.ThrowsAsync<TimeoutException>(() => dispatcher.PublishAsync(new TicketPinged(id, "timeout")));

        var state = (await provider.GetRequiredService<ISagaRepository<TicketState>>().FindAsync(id, default))!;
        Assert.Equal((TicketSaga.Open.Name, 0, 1), (state.CurrentState, state.Pings, state.Version));
    }

    [Fact]
    public async Task Cancellation_of_the_event_is_never_handled_as_a_fault()
    {
        var saga = new TicketSaga();
        var state = new TicketState { CorrelationId = Guid.NewGuid(), CurrentState = TicketSaga.Open.Name, Version = 1 };
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => saga.ExecuteAsync(state, new TicketPinged(state.CorrelationId, "cancel"), cts.Token));
    }

    private sealed class KeepingRepository : ISagaRepository<TicketState>
    {
        private readonly InMemorySagaRepository<TicketState> _inner = new();

        public Task<TicketState?> FindAsync(Guid correlationId, CancellationToken ct) => _inner.FindAsync(correlationId, ct);

        public Task SaveAsync(TicketState state, CancellationToken ct) => _inner.SaveAsync(state, ct);
    }

    [Fact]
    public async Task A_repository_that_cannot_remove_reports_it_clearly()
    {
        await using var provider = Build(s => s.AddSagaFlux().UseRepository<TicketState, KeepingRepository>(ServiceLifetime.Singleton));
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();
        await dispatcher.PublishAsync(new TicketOpened(id));

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => dispatcher.PublishAsync(new TicketClosed(id)));

        Assert.Contains("RemoveWhenFinal", ex.Message);
    }

    public sealed record Go(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class CompensableState : ICompensableSagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public List<CompensationEntry> Compensations { get; set; } = new();
    }

    private sealed class UnregisteredKeyInBranchSaga : Saga<CompensableState>
    {
        public UnregisteredKeyInBranchSaga() =>
            During(SagaStates.Initial,
                When<Go>().Branch(_ => true, b => b.OnCompensate("Missing")));
    }

    [Fact]
    public void Validate_sees_compensation_keys_recorded_inside_branches()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new UnregisteredKeyInBranchSaga().Validate());
        Assert.Contains("'Missing'", ex.Message);
    }
}
