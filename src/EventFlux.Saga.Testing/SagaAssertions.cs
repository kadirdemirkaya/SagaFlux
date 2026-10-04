namespace EventFlux.Saga.Testing;

/// <summary>
/// Test-framework agnostic assertions on a <see cref="SagaTestResult{TState}"/>. A failed assertion throws
/// <see cref="SagaAssertionException"/>.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class SagaAssertions<TState> where TState : class, ISagaState
{
    private readonly SagaTestResult<TState> _result;

    internal SagaAssertions(SagaTestResult<TState> result) => _result = result;

    /// <summary>
    /// Gets these assertions again, for chaining: <c>.TransitionTo(x).And.HavePublished&lt;T&gt;()</c>.
    /// </summary>
    public SagaAssertions<TState> And => this;

    /// <summary>
    /// Asserts that the instance ends in <paramref name="state"/>.
    /// </summary>
    /// <param name="state">The expected state.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> TransitionTo(SagaState state)
    {
        if (_result.Instance is null && !_result.Removed)
            Fail($"Expected the saga to transition to '{state}', but no instance exists (no transition matched).");
        if (_result.CurrentState != state.Name)
            Fail($"Expected the saga to transition to '{state}', but it is in '{_result.CurrentState}'.");
        return this;
    }

    /// <summary>
    /// Asserts that the instance has completed (<see cref="SagaStates.Final"/>).
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> BeFinal() => TransitionTo(SagaStates.Final);

    /// <summary>
    /// Asserts that the instance is still in <paramref name="state"/>.
    /// </summary>
    /// <param name="state">The expected state.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> StayIn(SagaState state)
    {
        if (_result.CurrentState != state.Name)
            Fail($"Expected the saga to stay in '{state}', but it is in '{_result.CurrentState}'.");
        return this;
    }

    /// <summary>
    /// Asserts that every event was handled by a transition.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> BeHandled()
    {
        var ignored = _result.Steps.FirstOrDefault(s => !s.Handled);
        if (ignored is not null)
            Fail($"Expected every event to be handled, but an event was ignored in state '{ignored.FromState}'.");
        return this;
    }

    /// <summary>
    /// Asserts that no event was handled.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotBeHandled()
    {
        var handled = _result.Steps.FirstOrDefault(s => s.Handled);
        if (handled is not null)
            Fail($"Expected no event to be handled, but one moved the saga from '{handled.FromState}' to '{handled.ToState}'.");
        return this;
    }

    /// <summary>
    /// Asserts that the scenario created a new instance.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveCreatedInstance()
    {
        if (!_result.Steps.Any(s => s.Created))
            Fail("Expected the saga to create a new instance, but none was created.");
        return this;
    }

    /// <summary>
    /// Asserts that at least one <typeparamref name="TEvent"/> matching <paramref name="predicate"/> was published.
    /// </summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="predicate">An optional filter.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HavePublished<TEvent>(Func<TEvent, bool>? predicate = null)
    {
        if (!_result.Published.OfType<TEvent>().Any(e => predicate is null || predicate(e)))
            Fail($"Expected a published {typeof(TEvent).Name}{(predicate is null ? "" : " matching the predicate")}, but found: {Describe()}.");
        return this;
    }

    /// <summary>
    /// Asserts that no <typeparamref name="TEvent"/> was published.
    /// </summary>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHavePublished<TEvent>()
    {
        if (_result.Published.OfType<TEvent>().Any())
            Fail($"Expected no published {typeof(TEvent).Name}, but found: {Describe()}.");
        return this;
    }

    /// <summary>
    /// Asserts that nothing was published.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHavePublishedAnything()
    {
        if (_result.Published.Count > 0)
            Fail($"Expected nothing to be published, but found: {Describe()}.");
        return this;
    }

    /// <summary>
    /// Asserts a condition on the instance data.
    /// </summary>
    /// <param name="predicate">The condition.</param>
    /// <param name="because">Describes the condition in the failure message.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveState(Func<TState, bool> predicate, string because = "the condition")
    {
        ArgumentNullException.ThrowIfNull(predicate);
        if (_result.Instance is null || !predicate(_result.Instance))
            Fail($"Expected the saga instance to satisfy {because}, but it did not.");
        return this;
    }

    /// <summary>
    /// Asserts that the compensation registered under <paramref name="key"/> ran during the scenario.
    /// </summary>
    /// <param name="key">The compensation key.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveCompensated(string key)
    {
        if (!_result.Compensated.Any(c => c.CompensationActionKey == key))
            Fail($"Expected compensation '{key}' to run, but ran: {DescribeCompensations()}.");
        return this;
    }

    /// <summary>
    /// Asserts that the compensation registered under <paramref name="key"/> did not run.
    /// </summary>
    /// <param name="key">The compensation key.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHaveCompensated(string key)
    {
        if (_result.Compensated.Any(c => c.CompensationActionKey == key))
            Fail($"Expected compensation '{key}' not to run, but it did.");
        return this;
    }

    /// <summary>
    /// Asserts that exactly these compensations ran, in this order.
    /// </summary>
    /// <param name="keys">The expected keys in execution order (most recent first).</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveCompensatedInOrder(params string[] keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (!_result.Compensated.Select(c => c.CompensationActionKey).SequenceEqual(keys))
            Fail($"Expected compensations [{string.Join(", ", keys)}], but ran: {DescribeCompensations()}.");
        return this;
    }

    /// <summary>
    /// Asserts that a <typeparamref name="TTimeout"/> timeout was scheduled.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type.</typeparam>
    /// <param name="delay">The expected delay, or <c>null</c> for any.</param>
    /// <param name="predicate">An optional filter on the timeout event.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveScheduled<TTimeout>(TimeSpan? delay = null, Func<TTimeout, bool>? predicate = null)
    {
        var match = _result.Scheduled.Any(s =>
            s.Event is TTimeout e && (delay is null || s.Delay == delay) && (predicate is null || predicate(e)));
        if (!match)
            Fail($"Expected a scheduled {typeof(TTimeout).Name}{(delay is null ? "" : $" after {delay}")}, but found: {DescribeScheduled()}.");
        return this;
    }

    /// <summary>
    /// Asserts that no <typeparamref name="TTimeout"/> timeout was scheduled.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type.</typeparam>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHaveScheduled<TTimeout>()
    {
        if (_result.Scheduled.Any(s => s.Event is TTimeout))
            Fail($"Expected no scheduled {typeof(TTimeout).Name}, but found: {DescribeScheduled()}.");
        return this;
    }

    /// <summary>
    /// Asserts that the pending <typeparamref name="TTimeout"/> timeout was cancelled.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type.</typeparam>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveUnscheduled<TTimeout>()
    {
        if (!_result.Unscheduled.Contains(SagaTimeoutKeys.For<TTimeout>()))
            Fail($"Expected the {typeof(TTimeout).Name} timeout to be unscheduled, but it was not.");
        return this;
    }

    /// <summary>
    /// Asserts that the saga sent a <typeparamref name="TRequest"/> with <c>Ask</c>.
    /// </summary>
    /// <typeparam name="TRequest">The request type.</typeparam>
    /// <param name="predicate">An optional condition on the request.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveAsked<TRequest>(Func<TRequest, bool>? predicate = null)
    {
        if (!_result.Asked.OfType<TRequest>().Any(r => predicate is null || predicate(r)))
        {
            var asked = _result.Asked.Count == 0 ? "nothing" : string.Join(", ", _result.Asked.Select(r => r.GetType().Name));
            Fail($"Expected the saga to ask {typeof(TRequest).Name}, but it asked: {asked}.");
        }
        return this;
    }

    /// <summary>
    /// Asserts that the saga sent no <typeparamref name="TRequest"/>.
    /// </summary>
    /// <typeparam name="TRequest">The request type.</typeparam>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHaveAsked<TRequest>()
    {
        if (_result.Asked.OfType<TRequest>().Any())
            Fail($"Expected the saga not to ask {typeof(TRequest).Name}, but it did.");
        return this;
    }

    /// <summary>
    /// Asserts that the instance reached <see cref="SagaStates.Final"/> and is removed from the repository, because
    /// the saga calls <c>RemoveWhenFinal()</c>.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> BeRemoved()
    {
        if (!_result.Removed)
            Fail($"Expected the instance to be removed when final, but it is in '{_result.CurrentState}' and kept.");
        return this;
    }

    /// <summary>
    /// Asserts that an <c>OnFault&lt;TException&gt;(...)</c> handler handled a <typeparamref name="TException"/>.
    /// </summary>
    /// <typeparam name="TException">The expected exception type (derived types match too).</typeparam>
    /// <param name="predicate">An optional condition on the exception.</param>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> HaveHandledFault<TException>(Func<TException, bool>? predicate = null) where TException : Exception
    {
        if (!_result.Faults.OfType<TException>().Any(e => predicate is null || predicate(e)))
        {
            var handled = _result.Faults.Count == 0 ? "none" : string.Join(", ", _result.Faults.Select(f => f.GetType().Name));
            Fail($"Expected a handled {typeof(TException).Name} fault, but handled faults are: {handled}.");
        }
        return this;
    }

    /// <summary>
    /// Asserts that no fault was handled.
    /// </summary>
    /// <returns>These assertions.</returns>
    public SagaAssertions<TState> NotHaveHandledFault()
    {
        if (_result.Faults.Count > 0)
            Fail($"Expected no handled fault, but {_result.Faults[0].GetType().Name} was handled: {_result.Faults[0].Message}");
        return this;
    }

    private string DescribeScheduled()
        => _result.Scheduled.Count == 0 ? "none" : string.Join(", ", _result.Scheduled.Select(s => $"{s.Event.GetType().Name} after {s.Delay}"));

    private string DescribeCompensations()
        => _result.Compensated.Count == 0 ? "none" : "[" + string.Join(", ", _result.Compensated.Select(c => c.CompensationActionKey)) + "]";

    private string Describe()
        => _result.Published.Count == 0 ? "nothing" : string.Join(", ", _result.Published.Select(e => e.GetType().Name));

    private static void Fail(string message) => throw new SagaAssertionException(message);
}
