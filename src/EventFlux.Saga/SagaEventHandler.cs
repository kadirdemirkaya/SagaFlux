using EventFlux.Abstractions;
using EventFlux.Saga.Internal;

namespace EventFlux.Saga;

/// <summary>
/// EventFlux handler registered by <c>AddSaga</c> for every notification a saga reacts to.
/// </summary>
/// <remarks>
/// When the event is published through <see cref="IEventBus"/> (including events delivered by a broker consumer),
/// this handler feeds it to the sagas. When it is published through <see cref="IEventDispatcher"/>, the
/// <see cref="SagaCorrelationBehavior{TEvent}"/> does that after all handlers ran, and this handler does nothing,
/// so one dispatch reaches the sagas once rather than once per path. (Broker transports still deliver at least once.)
/// </remarks>
/// <typeparam name="TEvent">The notification type.</typeparam>
/// <param name="router">The saga router.</param>
public sealed class SagaEventHandler<TEvent>(ISagaEventRouter router) : IEventHandler<TEvent> where TEvent : IEventRequest
{
    /// <inheritdoc />
    public Task Handle(TEvent @event, CancellationToken cancellationToken)
        => SagaPipelineScope.IsRouting(@event) ? Task.CompletedTask : router.RouteAsync(@event, cancellationToken);
}
