namespace EventFlux.Saga.EfCore;

/// <summary>
/// A published event waiting in the outbox, as stored by <see cref="EfCoreSagaOutbox"/>. Map it with
/// <see cref="SagaModelBuilderExtensions.ConfigureSagaOutbox"/>.
/// </summary>
public sealed class SagaOutboxRecord
{
    /// <summary>Gets or sets the message id.</summary>
    public Guid Id { get; set; }

    /// <summary>Gets or sets the event type as <c>"Namespace.Type, Assembly"</c> (see <see cref="SagaEventTypeNames"/>).</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary>Gets or sets the event as JSON.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>Gets or sets when the message was staged, as UTC ticks.</summary>
    public long StagedAtUtcTicks { get; set; }
}
