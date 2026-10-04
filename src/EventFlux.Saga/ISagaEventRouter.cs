namespace EventFlux.Saga;

/// <summary>
/// Hands an event to every registered saga that reacts to its type.
/// </summary>
/// <remarks>
/// <see cref="SagaCorrelationBehavior{TEvent}"/> calls this for events published through the EventFlux dispatcher.
/// Call it directly to feed sagas from other sources (for example a broker consumer or an <c>IEventBus</c> handler).
/// </remarks>
public interface ISagaEventRouter
{
    /// <summary>
    /// Applies <paramref name="event"/> to every saga that reacts to its runtime type.
    /// </summary>
    /// <param name="event">The event.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task that completes once every saga has saved its instance and published its events.</returns>
    Task RouteAsync(object @event, CancellationToken ct);
}
