using EventFlux.Abstractions;

namespace EventFlux.Saga.Transports;

/// <summary>
/// The default <see cref="ISagaTransport"/>: publishes saga events in-process through the EventFlux
/// <see cref="IEventDispatcher"/>, so pipeline behaviors (including other sagas) see them. No broker is involved.
/// </summary>
/// <param name="dispatcher">The EventFlux dispatcher.</param>
public sealed class InProcessSagaTransport(IEventDispatcher dispatcher) : ISagaTransport
{
    /// <inheritdoc />
    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        => dispatcher.PublishAsync(@event, ct);
}
