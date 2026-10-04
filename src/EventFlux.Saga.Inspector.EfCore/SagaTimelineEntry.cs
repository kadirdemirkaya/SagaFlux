namespace EventFlux.Saga.Inspector.EfCore;

/// <summary>
/// A saga transition as stored by <see cref="EfCoreSagaTimelineStore"/>. Map it with
/// <see cref="SagaTimelineModelBuilderExtensions.ConfigureSagaTimeline"/>.
/// </summary>
public sealed class SagaTimelineEntry
{
    /// <summary>Gets or sets the entry id.</summary>
    public Guid Id { get; set; }

    /// <summary>Gets or sets the saga instance.</summary>
    public Guid CorrelationId { get; set; }

    /// <summary>Gets or sets the saga type name.</summary>
    public string SagaType { get; set; } = string.Empty;

    /// <summary>Gets or sets the event type name.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>Gets or sets the state before the transition.</summary>
    public string FromState { get; set; } = string.Empty;

    /// <summary>Gets or sets the state after the transition.</summary>
    public string ToState { get; set; } = string.Empty;

    /// <summary>Gets or sets whether the transition created the instance.</summary>
    public bool Created { get; set; }

    /// <summary>Gets or sets the instance version after the transition.</summary>
    public int Version { get; set; }

    /// <summary>Gets or sets the published event type names as a JSON array.</summary>
    public string Published { get; set; } = "[]";

    /// <summary>Gets or sets the executed compensation keys as a JSON array.</summary>
    public string Compensated { get; set; } = "[]";

    /// <summary>Gets or sets the scheduled timeout keys as a JSON array.</summary>
    public string Scheduled { get; set; } = "[]";

    /// <summary>Gets or sets the cancelled timeout keys as a JSON array.</summary>
    public string Unscheduled { get; set; } = "[]";

    /// <summary>Gets or sets the handled fault as <c>"Type: message"</c>, or <c>null</c>.</summary>
    public string? Fault { get; set; }

    /// <summary>Gets or sets whether the instance was removed after reaching Final.</summary>
    public bool Removed { get; set; }

    /// <summary>Gets or sets when the transition happened, as UTC ticks (provider-independent ordering).</summary>
    public long OccurredAtUtcTicks { get; set; }
}
