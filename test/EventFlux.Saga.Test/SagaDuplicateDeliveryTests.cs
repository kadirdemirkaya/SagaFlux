using EventFlux.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class SagaDuplicateDeliveryTests
{
    public sealed record TransitionRequested(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record ActivePulse(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class DeliveryState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int TransitionActions { get; set; }
        public int ActiveActions { get; set; }
    }

    public sealed class DeliverySaga : Saga<DeliveryState>
    {
        public static readonly SagaState Active = SagaStates.Define(nameof(Active));

        public DeliverySaga()
        {
            During(SagaStates.Initial,
                When<TransitionRequested>()
                    .Then(ctx => ctx.State.TransitionActions++)
                    .TransitionTo(Active));

            DuringActive(
                When<ActivePulse>()
                    .Then(ctx => ctx.State.ActiveActions++));
        }
    }

    [Fact]
    public async Task A_state_changing_transition_ignores_the_same_event_when_delivered_again()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSagaFlux().AddSaga<DeliverySaga, DeliveryState>();
        await using var provider = services.BuildServiceProvider();
        var correlationId = Guid.NewGuid();
        var @event = new TransitionRequested(correlationId);
        var router = provider.GetRequiredService<ISagaEventRouter>();

        await router.RouteAsync(@event, default);
        await router.RouteAsync(@event, default);

        var state = await provider.GetRequiredService<ISagaRepository<DeliveryState>>().FindAsync(correlationId, default);
        Assert.NotNull(state);
        Assert.Equal(DeliverySaga.Active.Name, state.CurrentState);
        Assert.Equal(1, state.TransitionActions);
    }

    [Fact]
    public async Task An_in_state_transition_runs_again_when_the_same_event_is_delivered_again()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSagaFlux().AddSaga<DeliverySaga, DeliveryState>();
        await using var provider = services.BuildServiceProvider();
        var correlationId = Guid.NewGuid();
        var router = provider.GetRequiredService<ISagaEventRouter>();

        await router.RouteAsync(new TransitionRequested(correlationId), default);
        var @event = new ActivePulse(correlationId);
        await router.RouteAsync(@event, default);
        await router.RouteAsync(@event, default);

        var state = await provider.GetRequiredService<ISagaRepository<DeliveryState>>().FindAsync(correlationId, default);
        Assert.NotNull(state);
        Assert.Equal(DeliverySaga.Active.Name, state.CurrentState);
        Assert.Equal(2, state.ActiveActions);
    }
}
