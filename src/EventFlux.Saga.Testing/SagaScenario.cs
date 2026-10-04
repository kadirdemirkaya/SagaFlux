using EventFlux.Abstractions;

namespace EventFlux.Saga.Testing;

/// <summary>
/// A saga test scenario: an optional existing instance (Given), the events to apply (When), and their execution (Then).
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class SagaScenario<TState> where TState : class, ISagaState, new()
{
    private readonly Saga<TState> _saga;
    private readonly List<object> _events = new();
    private TState? _given;
    private readonly ScenarioRequester _requester = new();

    internal SagaScenario(Saga<TState> saga) => _saga = saga;

    /// <summary>
    /// Starts from an existing instance in <paramref name="state"/>. Its correlation id is taken from the first event.
    /// </summary>
    /// <param name="state">The state the instance is in.</param>
    /// <param name="configure">Optionally sets the instance's data.</param>
    /// <returns>This scenario.</returns>
    public SagaScenario<TState> Given(SagaState state, Action<TState>? configure = null)
    {
        var instance = new TState { CurrentState = state.Name };
        configure?.Invoke(instance);
        return Given(instance);
    }

    /// <summary>
    /// Starts from the given existing instance. When its correlation id is empty, it is taken from the first event.
    /// </summary>
    /// <param name="instance">The instance.</param>
    /// <returns>This scenario.</returns>
    public SagaScenario<TState> Given(TState instance)
    {
        ArgumentNullException.ThrowIfNull(instance);
        _given = instance;
        return this;
    }

    /// <summary>
    /// Adds pending compensation entries (oldest first) to the given instance, as if earlier transitions had recorded them.
    /// </summary>
    /// <param name="keys">The compensation keys, oldest first.</param>
    /// <returns>This scenario.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no instance was given or the state is not compensable.</exception>
    public SagaScenario<TState> WithCompensations(params string[] keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (_given is null)
            throw new InvalidOperationException("Call Given(...) before WithCompensations(...).");
        if (_given is not ICompensableSagaState compensable)
            throw new InvalidOperationException($"'{typeof(TState).Name}' does not implement {nameof(ICompensableSagaState)}.");

        compensable.Compensations ??= new List<CompensationEntry>();
        foreach (var key in keys)
            compensable.Compensations.Add(new CompensationEntry(_given.CurrentState, key, DateTimeOffset.UtcNow));
        return this;
    }

    /// <summary>
    /// Answers the <typeparamref name="TRequest"/> requests the saga sends with <c>Ask</c>, so the scenario needs no
    /// handler, database or service. A request without an answer fails the scenario.
    /// </summary>
    /// <typeparam name="TRequest">The request type.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="respond">Creates the response from the request.</param>
    /// <returns>This scenario.</returns>
    public SagaScenario<TState> Answer<TRequest, TResponse>(Func<TRequest, TResponse> respond)
        where TRequest : IEventRequest<TResponse>
        where TResponse : IEventResponse
    {
        ArgumentNullException.ThrowIfNull(respond);
        _requester.Answers[typeof(TRequest)] = request => respond((TRequest)request);
        return this;
    }

    /// <summary>
    /// Adds events to apply, in order.
    /// </summary>
    /// <param name="events">The events.</param>
    /// <returns>This scenario.</returns>
    public SagaScenario<TState> When(params object[] events)
    {
        ArgumentNullException.ThrowIfNull(events);
        foreach (var @event in events)
        {
            ArgumentNullException.ThrowIfNull(@event, nameof(events));
            _events.Add(@event);
        }
        return this;
    }

    /// <summary>
    /// Applies the events and returns the outcome.
    /// </summary>
    /// <returns>The outcome of the scenario.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no event was given.</exception>
    public SagaTestResult<TState> Then() => ThenAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Applies the events and returns the outcome; use when transitions run asynchronous actions.
    /// </summary>
    /// <param name="ct">Cancellation token passed to the transitions.</param>
    /// <returns>The outcome of the scenario.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no event was given.</exception>
    public async Task<SagaTestResult<TState>> ThenAsync(CancellationToken ct = default)
    {
        if (_events.Count == 0)
            throw new InvalidOperationException("Call When(...) with at least one event before Then().");

        var state = _given;
        if (state is { CorrelationId: var id } && id == Guid.Empty)
            state.CorrelationId = _saga.GetCorrelationId(_events[0]);

        var initialState = state?.CurrentState ?? SagaStates.Initial.Name;
        var steps = new List<SagaExecutionResult<TState>>();
        var removed = false;
        foreach (var @event in _events)
        {
            var step = await _saga.ExecuteAsync(state, @event, _requester, ct).ConfigureAwait(false);
            steps.Add(step);
            state = step.State;

            // Mirrors the runtime: a RemoveWhenFinal() instance is gone once it is final.
            if (step.Handled && step.ToState == SagaStates.Final.Name && _saga.RemovesWhenFinal)
            {
                removed = true;
                state = null;
            }
            else if (state is not null)
            {
                removed = false;
            }
        }

        return new SagaTestResult<TState>(initialState, state, steps, removed);
    }

    private sealed class ScenarioRequester : ISagaRequester
    {
        public Dictionary<Type, Func<object, object>> Answers { get; } = new();

        public Task<TResponse?> AskAsync<TResponse>(IEventRequest<TResponse> request, CancellationToken ct) where TResponse : IEventResponse
        {
            if (!Answers.TryGetValue(request.GetType(), out var respond))
                throw new SagaConfigurationException(
                    $"The saga asked '{request.GetType().Name}', but the scenario has no answer for it. Call Answer<{request.GetType().Name}, {typeof(TResponse).Name}>(...).");
            return Task.FromResult((TResponse?)respond(request));
        }
    }
}
