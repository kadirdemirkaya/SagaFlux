using EventFlux.Abstractions;
using EventFlux.Saga.Internal;

namespace EventFlux.Saga;

/// <summary>
/// The context passed to a compensation action.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class SagaCompensationContext<TState> where TState : class, ISagaState
{
    private readonly SagaEffects _effects;

    internal SagaCompensationContext(TState state, CompensationEntry entry, SagaEffects effects, CancellationToken cancellationToken)
    {
        State = state;
        Entry = entry;
        _effects = effects;
        CancellationToken = cancellationToken;
    }

    /// <summary>
    /// Gets the saga instance data.
    /// </summary>
    public TState State { get; }

    /// <summary>
    /// Gets the compensation entry being executed.
    /// </summary>
    public CompensationEntry Entry { get; }

    /// <summary>
    /// Gets the correlation id of the instance.
    /// </summary>
    public Guid CorrelationId => State.CorrelationId;

    /// <summary>
    /// Gets the cancellation token of the dispatch.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// Queues an event (typically an undo command) to be published once the instance has been saved.
    /// </summary>
    /// <typeparam name="TOut">The published EventFlux notification type.</typeparam>
    /// <param name="event">The event to publish.</param>
    public void Publish<TOut>(TOut @event) where TOut : IEventRequest
    {
        ArgumentNullException.ThrowIfNull(@event);
        _effects.Published.Add(SagaPublishedEvent.Create(@event));
    }
}
