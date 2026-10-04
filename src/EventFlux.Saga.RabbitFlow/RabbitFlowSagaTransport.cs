using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;

namespace EventFlux.Saga.RabbitFlow;

/// <summary>
/// <see cref="ISagaTransport"/> that publishes saga events through the EventFlux.RabbitFlow <see cref="IEventBroker"/>.
/// </summary>
/// <remarks>
/// The broker applies its own serialization and routing (the event type name is the routing key); SagaFlux adds
/// no message format of its own.
/// </remarks>
/// <param name="broker">The EventFlux.RabbitFlow broker.</param>
/// <param name="options">The transport options.</param>
public sealed class RabbitFlowSagaTransport(IEventBroker broker, RabbitFlowSagaOptions options) : ISagaTransport
{
    /// <inheritdoc />
    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
    {
        ArgumentNullException.ThrowIfNull(@event);
        ct.ThrowIfCancellationRequested();
        return options.ExchangeName is null
            ? broker.PublishAsync(@event, ct)
            : broker.PublishAsync(@event, options.ExchangeName, ct);
    }
}
