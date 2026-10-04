using EventFlux.Abstractions;

namespace EventFlux.Saga.Testing;

/// <summary>
/// The outcome of a <see cref="SagaScenario{TState}"/>.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class SagaTestResult<TState> where TState : class, ISagaState
{
    internal SagaTestResult(string initialState, TState? instance, IReadOnlyList<SagaExecutionResult<TState>> steps, bool removed = false)
    {
        Removed = removed;
        InitialState = initialState;
        Instance = instance;
        Steps = steps;
        Published = steps.SelectMany(s => s.Published).Select(p => p.Event).ToList();
        Compensated = steps.SelectMany(s => s.Compensated).ToList();
        Scheduled = steps.SelectMany(s => s.Scheduled).ToList();
        Unscheduled = steps.SelectMany(s => s.Unscheduled).ToList();
        Asked = steps.SelectMany(s => s.Asked).ToList();
    }

    /// <summary>
    /// Gets the state name before the first event.
    /// </summary>
    public string InitialState { get; }

    /// <summary>
    /// Gets the instance after the last event, or <c>null</c> when no instance exists.
    /// </summary>
    public TState? Instance { get; }

    /// <summary>
    /// Gets the state name after the last event.
    /// </summary>
    public string CurrentState => Removed ? SagaStates.Final.Name : Instance?.CurrentState ?? InitialState;

    /// <summary>
    /// Gets whether the instance reached <see cref="SagaStates.Final"/> and was removed (the saga calls
    /// <c>RemoveWhenFinal()</c>); <see cref="Instance"/> is then <c>null</c>.
    /// </summary>
    public bool Removed { get; }

    /// <summary>
    /// Gets the requests the saga sent with <c>Ask</c>, in order.
    /// </summary>
    public IReadOnlyList<object> Asked { get; }

    /// <summary>
    /// Gets the exceptions handled by <c>OnFault&lt;TException&gt;(...)</c> handlers, in order.
    /// </summary>
    public IReadOnlyList<Exception> Faults => Steps.Where(s => s.Fault is not null).Select(s => s.Fault!).ToList();

    /// <summary>
    /// Gets the per-event execution results, in order.
    /// </summary>
    public IReadOnlyList<SagaExecutionResult<TState>> Steps { get; }

    /// <summary>
    /// Gets every event published during the scenario, in order.
    /// </summary>
    public IReadOnlyList<IEventRequest> Published { get; }

    /// <summary>
    /// Gets every compensation executed during the scenario, in execution (LIFO) order.
    /// </summary>
    public IReadOnlyList<CompensationEntry> Compensated { get; }

    /// <summary>
    /// Gets every timeout scheduled during the scenario, in order.
    /// </summary>
    public IReadOnlyList<SagaScheduledTimeout> Scheduled { get; }

    /// <summary>
    /// Gets the keys of every timeout cancelled during the scenario, in order.
    /// </summary>
    public IReadOnlyList<string> Unscheduled { get; }

    /// <summary>
    /// Starts assertions on this result.
    /// </summary>
    /// <returns>The assertions.</returns>
    public SagaAssertions<TState> Should() => new(this);
}
