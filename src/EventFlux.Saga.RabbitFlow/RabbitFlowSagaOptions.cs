namespace EventFlux.Saga.RabbitFlow;

/// <summary>
/// Options of the RabbitFlow saga transport.
/// </summary>
public sealed class RabbitFlowSagaOptions
{
    /// <summary>
    /// Gets or sets the exchange saga events are published to and consumed from.
    /// <c>null</c> (default) uses the EventFlux.RabbitFlow default exchange.
    /// </summary>
    public string? ExchangeName { get; set; }

    /// <summary>
    /// Gets or sets whether the notifications the registered sagas react to are subscribed on startup, so that
    /// sagas also receive events published by other services. Default: <c>true</c>.
    /// </summary>
    public bool SubscribeToSagaEvents { get; set; } = true;
}
