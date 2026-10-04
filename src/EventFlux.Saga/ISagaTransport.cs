using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// Physically delivers the events a saga publishes.
/// </summary>
/// <remarks>
/// Saga definitions never know which implementation is in use (in-process, RabbitFlow, RedisFlow); switching the
/// broker only changes the DI registration. Events are plain EventFlux <see cref="IEventRequest"/> contracts —
/// SagaFlux does not introduce its own message format.
/// </remarks>
public interface ISagaTransport
{
    /// <summary>
    /// Publishes an event produced by a saga.
    /// </summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="event">The event to publish.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the publish operation.</returns>
    Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest;
}
