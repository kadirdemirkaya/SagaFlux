using EventFlux.Abstractions;
using EventFlux.RedisFlow.Redis;

namespace EventFlux.Saga.RedisFlow;

/// <summary>
/// <see cref="ISagaTransport"/> that appends saga events to the Redis stream of EventFlux.RedisFlow through its
/// <see cref="IRedisStreamPublisher"/>.
/// </summary>
/// <remarks>
/// The typed <see cref="IRedisStreamPublisher.PublishAsync{TEvent}(TEvent, CancellationToken)"/> writes the same type
/// name and JSON payload the RedisFlow worker reads; SagaFlux adds no message format of its own.
/// </remarks>
/// <param name="publisher">The EventFlux.RedisFlow stream publisher.</param>
public sealed class RedisFlowSagaTransport(IRedisStreamPublisher publisher) : ISagaTransport
{
    /// <inheritdoc />
    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
    {
        ArgumentNullException.ThrowIfNull(@event);
        ct.ThrowIfCancellationRequested();
        return publisher.PublishAsync(@event, ct);
    }
}
