namespace EventFlux.Saga;

/// <summary>
/// The outcome of applying one event to a saga instance.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <param name="Handled">Whether a transition matched and ran.</param>
/// <param name="Created">Whether the event created a new instance.</param>
/// <param name="State">The instance after the event, or <c>null</c> when no instance exists.</param>
/// <param name="FromState">The state name before the event.</param>
/// <param name="ToState">The state name after the event.</param>
/// <param name="Published">The events published by the transition and its compensations, in order.</param>
/// <param name="Compensated">The compensation entries executed by this event, in execution (LIFO) order.</param>
public sealed record SagaExecutionResult<TState>(
    bool Handled,
    bool Created,
    TState? State,
    string FromState,
    string ToState,
    IReadOnlyList<SagaPublishedEvent> Published,
    IReadOnlyList<CompensationEntry> Compensated) where TState : class, ISagaState
{
    /// <summary>
    /// Gets the timeouts scheduled by this event, in order.
    /// </summary>
    public IReadOnlyList<SagaScheduledTimeout> Scheduled { get; init; } = Array.Empty<SagaScheduledTimeout>();

    /// <summary>
    /// Gets the keys of the timeouts cancelled by this event.
    /// </summary>
    public IReadOnlyList<string> Unscheduled { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Gets the requests the transition sent with <c>Ask</c>, in order.
    /// </summary>
    public IReadOnlyList<object> Asked { get; init; } = Array.Empty<object>();

    /// <summary>
    /// Gets the exception an <c>OnFault&lt;TException&gt;(...)</c> handler of the transition handled, if any.
    /// </summary>
    public Exception? Fault { get; init; }

    internal static SagaExecutionResult<TState> NotHandled(TState? state, string currentState)
        => new(false, false, state, currentState, currentState, Array.Empty<SagaPublishedEvent>(), Array.Empty<CompensationEntry>());
}
