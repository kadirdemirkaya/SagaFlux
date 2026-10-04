namespace EventFlux.Saga;

/// <summary>
/// What happened when one event was applied to a saga instance. Passed to every <see cref="ISagaTransitionObserver"/>.
/// </summary>
/// <param name="CorrelationId">The saga instance.</param>
/// <param name="SagaType">The saga definition type name.</param>
/// <param name="EventType">The applied event type name.</param>
/// <param name="FromState">The state before the event.</param>
/// <param name="ToState">The state after the event.</param>
/// <param name="Created">Whether the event created the instance.</param>
/// <param name="Version">The instance version after it was saved.</param>
/// <param name="Published">The type names of the events the transition published, in order.</param>
/// <param name="Compensated">The compensation keys executed, in execution order.</param>
/// <param name="Scheduled">The timeout keys scheduled.</param>
/// <param name="Unscheduled">The timeout keys cancelled.</param>
/// <param name="OccurredAt">When the instance was saved.</param>
public sealed record SagaTransitionRecord(
    Guid CorrelationId,
    string SagaType,
    string EventType,
    string FromState,
    string ToState,
    bool Created,
    int Version,
    IReadOnlyList<string> Published,
    IReadOnlyList<string> Compensated,
    IReadOnlyList<string> Scheduled,
    IReadOnlyList<string> Unscheduled,
    DateTimeOffset OccurredAt)
{
    /// <summary>
    /// Gets the exception an <c>OnFault&lt;TException&gt;(...)</c> handler of the transition handled, as
    /// <c>"Type: message"</c>; <c>null</c> when the transition ran without a fault.
    /// </summary>
    public string? Fault { get; init; }

    /// <summary>
    /// Gets whether the instance was removed from its repository after reaching <see cref="SagaStates.Final"/>
    /// (<c>RemoveWhenFinal()</c>).
    /// </summary>
    public bool Removed { get; init; }
}
