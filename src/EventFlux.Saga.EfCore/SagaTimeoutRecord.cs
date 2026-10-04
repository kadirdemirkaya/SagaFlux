namespace EventFlux.Saga.EfCore;

/// <summary>
/// A pending saga timeout as stored by <see cref="EfCoreSagaTimeoutStore"/>. Map it with
/// <see cref="SagaModelBuilderExtensions.ConfigureSagaTimeouts"/>.
/// </summary>
public sealed class SagaTimeoutRecord
{
    /// <summary>
    /// Gets or sets the id of this scheduling.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets or sets the saga instance.
    /// </summary>
    public Guid CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the timeout key, unique per instance.
    /// </summary>
    public string Key { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the due time as UTC ticks (provider-independent ordering and comparison).
    /// </summary>
    public long DueAtUtcTicks { get; set; }

    /// <summary>
    /// Gets or sets the timeout event type as <c>"Namespace.Type, Assembly"</c> (no version, so assembly upgrades
    /// keep resolving).
    /// </summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the timeout event as JSON.
    /// </summary>
    public string Payload { get; set; } = string.Empty;
}
