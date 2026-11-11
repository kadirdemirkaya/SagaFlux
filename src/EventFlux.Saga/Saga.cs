using System.ComponentModel;
using EventFlux.Abstractions;
using EventFlux.Saga.Internal;

namespace EventFlux.Saga;

/// <summary>
/// Base class of a saga (state machine / process manager) definition.
/// </summary>
/// <remarks>
/// <para>
/// A saga definition is stateless and shared: the per-instance data lives in <typeparamref name="TState"/>, loaded
/// and stored through <see cref="ISagaRepository{TState}"/>. Declare the state machine in the constructor:
/// </para>
/// <code>
/// public sealed class OrderSaga : Saga&lt;OrderState&gt;
/// {
///     public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));
///
///     public OrderSaga()
///     {
///         During(SagaStates.Initial,
///             When&lt;OrderPlaced&gt;().TransitionTo(AwaitingPayment));
///         During(AwaitingPayment,
///             When&lt;PaymentReceived&gt;().Publish(ctx =&gt; new ShipOrder(ctx.CorrelationId)).Finalize());
///     }
/// }
/// </code>
/// <para>
/// The definition never knows how published events are transported (see <see cref="ISagaTransport"/>).
/// </para>
/// </remarks>
/// <typeparam name="TState">The saga state type.</typeparam>
public abstract class Saga<TState> where TState : class, ISagaState, new()
{
    private readonly Dictionary<string, List<SagaTransitionDefinition>> _transitions = new(StringComparer.Ordinal);
    private readonly Dictionary<Type, Func<object, Guid>> _correlations = new();
    private readonly HashSet<Type> _eventTypes = new();
    private readonly Dictionary<string, Func<SagaCompensationContext<TState>, Task>> _compensations = new(StringComparer.Ordinal);
    private readonly HashSet<Type> _timeoutTypes = new();
    private readonly List<SagaTransitionDefinition> _activeTransitions = new();

    /// <summary>
    /// Gets the event types this saga reacts to, in any state.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public IReadOnlyCollection<Type> EventTypes => _eventTypes;

    /// <summary>
    /// Declares the transitions allowed while an instance is in <paramref name="state"/>.
    /// </summary>
    /// <remarks>
    /// Transitions declared for <see cref="SagaStates.Initial"/> are the only ones that create new instances.
    /// When several transitions match an event, the first declared one whose condition holds is executed.
    /// </remarks>
    /// <param name="state">The state the transitions apply to.</param>
    /// <param name="transitions">The transitions, created with <see cref="When{TEvent}"/>.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="state"/> is <see cref="SagaStates.Final"/>.</exception>
    protected void During(SagaState state, params SagaTransitionDefinition[] transitions)
    {
        ArgumentNullException.ThrowIfNull(transitions);
        if (state == SagaStates.Final)
            throw new ArgumentException("A final saga instance does not accept events.", nameof(state));

        if (!_transitions.TryGetValue(state.Name, out var list))
            _transitions[state.Name] = list = new List<SagaTransitionDefinition>();

        foreach (var transition in transitions)
        {
            ArgumentNullException.ThrowIfNull(transition, nameof(transitions));
            list.Add(transition);
            _eventTypes.UnionWith(transition.TriggerTypes);
        }
    }

    /// <summary>
    /// Declares transitions allowed in every <em>active</em> state: any state of a started instance except
    /// <see cref="SagaStates.Final"/>. Typical use: an event that may arrive at any point, such as a cancellation.
    /// </summary>
    /// <remarks>
    /// Transitions declared with <see cref="During"/> for the current state are tried first; active transitions only
    /// run when none of them matches. They never create an instance.
    /// </remarks>
    /// <param name="transitions">The transitions, created with <see cref="When{TEvent}"/>.</param>
    protected void DuringActive(params SagaTransitionDefinition[] transitions)
    {
        ArgumentNullException.ThrowIfNull(transitions);
        foreach (var transition in transitions)
        {
            ArgumentNullException.ThrowIfNull(transition, nameof(transitions));
            _activeTransitions.Add(transition);
            _eventTypes.UnionWith(transition.TriggerTypes);
        }
    }

    /// <summary>
    /// Removes an instance from its repository once it reaches <see cref="SagaStates.Final"/>, instead of keeping it
    /// as a finished record. Transition observers (and the inspector timeline) still keep its history.
    /// </summary>
    /// <remarks>
    /// The repository must support <see cref="ISagaRepository{TState}.RemoveAsync"/> (the in-memory and EF Core ones
    /// do). Once removed, an event matching a <see cref="SagaStates.Initial"/> transition starts a new instance with
    /// the same correlation id.
    /// </remarks>
    protected void RemoveWhenFinal() => RemovesWhenFinal = true;

    /// <summary>
    /// Gets whether instances are removed from the repository when they reach <see cref="SagaStates.Final"/>
    /// (see <see cref="RemoveWhenFinal"/>).
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool RemovesWhenFinal { get; private set; }

    /// <summary>
    /// Starts a transition triggered by <typeparamref name="TEvent"/>.
    /// </summary>
    /// <typeparam name="TEvent">The triggering event (an EventFlux notification or request).</typeparam>
    /// <param name="condition">An optional guard; the transition only runs when it returns <c>true</c>.</param>
    /// <returns>A builder describing what happens when the event arrives.</returns>
    protected SagaTransitionBuilder<TEvent> When<TEvent>(Func<TEvent, bool>? condition = null) where TEvent : class
        => new(this, condition);

    /// <summary>
    /// Starts a transition that runs once <typeparamref name="TFirst"/> and <typeparamref name="TSecond"/> have both
    /// arrived for the instance, in any order. Until then each of them is recorded and the instance stays where it is.
    /// </summary>
    /// <remarks>
    /// Requires <typeparamref name="TState"/> to implement <see cref="IJoinableSagaState"/>. In the steps,
    /// <c>ctx.Event</c> is the event that completed the join. A repeated event counts once. Once the steps have run,
    /// the join starts over.
    /// </remarks>
    /// <typeparam name="TFirst">The first awaited event.</typeparam>
    /// <typeparam name="TSecond">The second awaited event.</typeparam>
    /// <returns>A builder describing what happens when all events have arrived.</returns>
    protected SagaTransitionBuilder<object> WhenAll<TFirst, TSecond>()
        where TFirst : class where TSecond : class
        => Join(typeof(TFirst), typeof(TSecond));

    /// <summary>
    /// Starts a transition that runs once all three events have arrived for the instance, in any order. See
    /// <see cref="WhenAll{TFirst, TSecond}"/>.
    /// </summary>
    /// <typeparam name="TFirst">The first awaited event.</typeparam>
    /// <typeparam name="TSecond">The second awaited event.</typeparam>
    /// <typeparam name="TThird">The third awaited event.</typeparam>
    /// <returns>A builder describing what happens when all events have arrived.</returns>
    protected SagaTransitionBuilder<object> WhenAll<TFirst, TSecond, TThird>()
        where TFirst : class where TSecond : class where TThird : class
        => Join(typeof(TFirst), typeof(TSecond), typeof(TThird));

    /// <summary>
    /// Starts a transition that runs once all four events have arrived for the instance, in any order. See
    /// <see cref="WhenAll{TFirst, TSecond}"/>.
    /// </summary>
    /// <typeparam name="TFirst">The first awaited event.</typeparam>
    /// <typeparam name="TSecond">The second awaited event.</typeparam>
    /// <typeparam name="TThird">The third awaited event.</typeparam>
    /// <typeparam name="TFourth">The fourth awaited event.</typeparam>
    /// <returns>A builder describing what happens when all events have arrived.</returns>
    protected SagaTransitionBuilder<object> WhenAll<TFirst, TSecond, TThird, TFourth>()
        where TFirst : class where TSecond : class where TThird : class where TFourth : class
        => Join(typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth));

    /// <summary>
    /// Starts a transition that runs once all five events have arrived for the instance, in any order. See
    /// <see cref="WhenAll{TFirst, TSecond}"/>.
    /// </summary>
    /// <typeparam name="TFirst">The first awaited event.</typeparam>
    /// <typeparam name="TSecond">The second awaited event.</typeparam>
    /// <typeparam name="TThird">The third awaited event.</typeparam>
    /// <typeparam name="TFourth">The fourth awaited event.</typeparam>
    /// <typeparam name="TFifth">The fifth awaited event.</typeparam>
    /// <returns>A builder describing what happens when all events have arrived.</returns>
    protected SagaTransitionBuilder<object> WhenAll<TFirst, TSecond, TThird, TFourth, TFifth>()
        where TFirst : class where TSecond : class where TThird : class where TFourth : class where TFifth : class
        => Join(typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth), typeof(TFifth));

    private SagaTransitionBuilder<object> Join(params Type[] types)
    {
        if (!typeof(IJoinableSagaState).IsAssignableFrom(typeof(TState)))
            throw new InvalidOperationException(
                $"Saga '{GetType().Name}' uses WhenAll, so '{typeof(TState).Name}' must implement {nameof(IJoinableSagaState)}.");
        if (types.Distinct().Count() != types.Length)
            throw new ArgumentException("WhenAll needs distinct event types.");
        return new SagaTransitionBuilder<object>(this, null, types);
    }

    /// <summary>
    /// Registers a compensation action under a stable key. Transitions record the key with
    /// <see cref="SagaTransitionBuilder{TEvent}.OnCompensate(string)"/>; it runs when the saga is compensated.
    /// </summary>
    /// <remarks>
    /// Only the key is persisted. Keep it a constant string: renaming it strands instances created by older deployments.
    /// Requires <typeparamref name="TState"/> to implement <see cref="ICompensableSagaState"/>.
    /// </remarks>
    /// <param name="key">The stable compensation key.</param>
    /// <param name="action">The compensation action.</param>
    /// <exception cref="InvalidOperationException">Thrown when the key is already registered or the state is not compensable.</exception>
    protected void OnCompensate(string key, Action<SagaCompensationContext<TState>> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        OnCompensate(key, ctx => { action(ctx); return Task.CompletedTask; });
    }

    /// <summary>
    /// Registers an asynchronous compensation action under a stable key.
    /// </summary>
    /// <param name="key">The stable compensation key.</param>
    /// <param name="action">The compensation action.</param>
    /// <exception cref="InvalidOperationException">Thrown when the key is already registered or the state is not compensable.</exception>
    protected void OnCompensate(string key, Func<SagaCompensationContext<TState>, Task> action)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(action);
        EnsureCompensable();
        if (!_compensations.TryAdd(key, action))
            throw new InvalidOperationException($"Saga '{GetType().Name}' already registers a compensation action for key '{key}'.");
    }

    /// <summary>
    /// Gets the timeout event types declared with <c>Schedule&lt;TTimeout&gt;(...)</c> on a transition.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public IReadOnlyCollection<Type> TimeoutTypes => _timeoutTypes;

    /// <summary>
    /// Gets the registered compensation keys.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public IReadOnlyCollection<string> CompensationKeys => _compensations.Keys;

    /// <summary>
    /// Declares how the correlation id is read from <typeparamref name="TEvent"/>.
    /// </summary>
    /// <remarks>
    /// Not needed for events implementing <see cref="ICorrelatedEvent"/>; a selector declared here takes precedence.
    /// </remarks>
    /// <typeparam name="TEvent">The event type.</typeparam>
    /// <param name="selector">Returns the correlation id of the saga instance the event belongs to.</param>
    protected void CorrelateBy<TEvent>(Func<TEvent, Guid> selector) where TEvent : class
    {
        ArgumentNullException.ThrowIfNull(selector);
        _correlations[typeof(TEvent)] = e => selector((TEvent)e);
    }

    /// <summary>
    /// Checks that every event this saga reacts to can be correlated.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when an event has neither a <see cref="CorrelateBy{TEvent}"/>
    /// selector nor implements <see cref="ICorrelatedEvent"/>.</exception>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public void Validate()
    {
        foreach (var key in _transitions.Values.SelectMany(l => l).Concat(_activeTransitions).SelectMany(t => t.RecordedCompensations))
        {
            if (!_compensations.ContainsKey(key))
                throw new InvalidOperationException(
                    $"Saga '{GetType().Name}' records compensation '{key}' but registers no action for it. Call OnCompensate(\"{key}\", ...).");
        }

        foreach (var type in _eventTypes.Concat(_timeoutTypes))
        {
            if (!CanCorrelate(type))
                throw new InvalidOperationException(
                    $"Saga '{GetType().Name}' cannot correlate event '{type.Name}'. Implement {nameof(ICorrelatedEvent)} on the event or call {nameof(CorrelateBy)}<{type.Name}>(...) in the saga constructor.");
        }
    }

    /// <summary>
    /// Determines whether this saga reacts to events of the given type.
    /// </summary>
    /// <param name="eventType">The runtime event type.</param>
    /// <returns><c>true</c> when at least one transition is triggered by the type.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public bool Handles(Type eventType)
        => _eventTypes.Any(t => t.IsAssignableFrom(eventType))
           || (_compensations.Count > 0 && IsCompensatingEvent(eventType) && CanCorrelate(eventType));

    private bool CanCorrelate(Type eventType)
        => FindCorrelation(eventType) is not null || typeof(ICorrelatedEvent).IsAssignableFrom(eventType);

    /// <summary>
    /// Reads the correlation id from an event.
    /// </summary>
    /// <param name="event">The event.</param>
    /// <returns>The correlation id of the saga instance the event belongs to.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the event cannot be correlated.</exception>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Guid GetCorrelationId(object @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        var selector = FindCorrelation(@event.GetType());
        if (selector is not null) return selector(@event);
        if (@event is ICorrelatedEvent correlated) return correlated.CorrelationId;
        throw new InvalidOperationException($"Saga '{GetType().Name}' cannot correlate event '{@event.GetType().Name}'.");
    }

    /// <summary>
    /// Applies an event to a saga instance without any I/O: no repository, no transport, no DI container.
    /// </summary>
    /// <remarks>
    /// This is the pure core of the saga engine; the runtime and the test harness both call it. When
    /// <paramref name="state"/> is <c>null</c>, a new instance in <see cref="SagaStates.Initial"/> is created if (and
    /// only if) an <c>Initial</c> transition matches. <paramref name="state"/> is modified in place.
    /// </remarks>
    /// <param name="state">The current instance, or <c>null</c> when none exists yet.</param>
    /// <param name="event">The event to apply.</param>
    /// <param name="ct">Cancellation token passed to the transition's actions.</param>
    /// <returns>The outcome of applying the event.</returns>
    public Task<SagaExecutionResult<TState>> ExecuteAsync(TState? state, object @event, CancellationToken ct = default)
        => ExecuteAsync(state, @event, requester: null, ct);

    /// <summary>
    /// Applies an event to a saga instance, answering the requests of <c>Ask</c> steps with <paramref name="requester"/>.
    /// </summary>
    /// <param name="state">The current instance, or <c>null</c> when none exists yet.</param>
    /// <param name="event">The event to apply.</param>
    /// <param name="requester">Sends the requests of <c>Ask</c> steps; <c>null</c> makes such a step fail.</param>
    /// <param name="ct">Cancellation token passed to the transition's actions.</param>
    /// <returns>The outcome of applying the event.</returns>
    public async Task<SagaExecutionResult<TState>> ExecuteAsync(TState? state, object @event, ISagaRequester? requester, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(@event);

        var isNew = state is null;
        var instance = state ?? new TState
        {
            CorrelationId = GetCorrelationId(@event),
            CurrentState = SagaStates.Initial.Name
        };
        var from = instance.CurrentState;

        var transition = FindTransition(from, @event);
        var unwind = !isNew && from != SagaStates.Final.Name && _compensations.Count > 0 && IsCompensatingEvent(@event.GetType());
        if (transition is null && !unwind)
            return SagaExecutionResult<TState>.NotHandled(isNew ? null : instance, from);

        var effects = new SagaEffects(requester);
        var compensated = new List<CompensationEntry>();
        Exception? fault = null;
        if (transition is not null)
        {
            var outcome = await transition.ExecuteAsync(instance, @event, effects, ct).ConfigureAwait(false);
            if (outcome.Target is { } next)
                instance.CurrentState = next.Name;

            foreach (var key in outcome.Recorded)
                Stack(instance).Add(new CompensationEntry(instance.CurrentState, key, DateTimeOffset.UtcNow));

            unwind |= outcome.Compensate;
            fault = outcome.Fault;
        }

        if (unwind)
            await CompensateAsync(instance, effects, compensated, ct).ConfigureAwait(false);

        return new SagaExecutionResult<TState>(true, isNew, instance, from, instance.CurrentState, effects.Published, compensated)
        {
            Scheduled = effects.Scheduled,
            Unscheduled = effects.Unscheduled,
            Asked = effects.Asked,
            Fault = fault
        };
    }

    private async Task CompensateAsync(TState instance, SagaEffects effects, List<CompensationEntry> compensated, CancellationToken ct)
    {
        if (_compensations.Count == 0) return;
        var stack = Stack(instance);
        for (var i = stack.Count - 1; i >= 0; i--)
        {
            var entry = stack[i];
            if (entry.IsCompensated) continue;
            if (!_compensations.TryGetValue(entry.CompensationActionKey, out var action))
                throw new SagaCompensationException(instance.CorrelationId, entry.CompensationActionKey);

            await action(new SagaCompensationContext<TState>(instance, entry, effects, ct)).ConfigureAwait(false);
            stack[i] = entry with { CompensatedAt = DateTimeOffset.UtcNow };
            compensated.Add(stack[i]);
        }
    }

    private static List<CompensationEntry> Stack(TState instance)
    {
        var compensable = (ICompensableSagaState)instance;
        return compensable.Compensations ??= new List<CompensationEntry>();
    }

    private static bool IsCompensatingEvent(Type eventType)
        => Attribute.IsDefined(eventType, typeof(CompensatingEventAttribute), inherit: true);

    private void EnsureCompensable()
    {
        if (!typeof(ICompensableSagaState).IsAssignableFrom(typeof(TState)))
            throw new InvalidOperationException(
                $"Saga '{GetType().Name}' uses compensation, so '{typeof(TState).Name}' must implement {nameof(ICompensableSagaState)}.");
    }

    private SagaTransitionDefinition? FindTransition(string currentState, object @event)
    {
        if (currentState == SagaStates.Final.Name) return null;
        var type = @event.GetType();
        if (_transitions.TryGetValue(currentState, out var list)
            && list.FirstOrDefault(t => t.Accepts(type) && t.Matches(@event)) is { } declared)
            return declared;
        if (currentState == SagaStates.Initial.Name) return null;
        return _activeTransitions.FirstOrDefault(t => t.Accepts(type) && t.Matches(@event));
    }

    private Func<object, Guid>? FindCorrelation(Type eventType)
    {
        if (_correlations.TryGetValue(eventType, out var exact)) return exact;
        foreach (var (type, selector) in _correlations)
            if (type.IsAssignableFrom(eventType)) return selector;
        return null;
    }

    /// <summary>
    /// A transition declared with <see cref="When{TEvent}"/>. Pass it to <see cref="During"/> or <see cref="DuringActive"/>.
    /// </summary>
    public abstract class SagaTransitionDefinition
    {
        private protected SagaTransitionDefinition() { }

        /// <summary>
        /// Gets the event type that triggers the transition.
        /// </summary>
        public abstract Type EventType { get; }

        internal virtual IEnumerable<Type> TriggerTypes => [EventType];

        internal virtual bool Accepts(Type eventType) => EventType.IsAssignableFrom(eventType);

        internal abstract bool Matches(object @event);

        internal abstract IEnumerable<string> RecordedCompensations { get; }

        internal abstract Task<TransitionOutcome> ExecuteAsync(TState state, object @event, SagaEffects effects, CancellationToken ct);
    }

    internal readonly record struct TransitionOutcome(SagaState? Target, IReadOnlyList<string> Recorded, bool Compensate, Exception? Fault);

    /// <summary>The mutable outcome of one transition run, shared by its branches and fault handlers.</summary>
    internal sealed class TransitionRun(SagaEffects effects)
    {
        public SagaEffects Effects { get; } = effects;
        public SagaState? Target { get; set; }
        public List<string> Recorded { get; } = new();
        public bool Compensate { get; set; }
        public Exception? Fault { get; set; }

        public (SagaState? Target, int Recorded, bool Compensate) Snapshot() => (Target, Recorded.Count, Compensate);

        public void Restore((SagaState? Target, int Recorded, bool Compensate) snapshot)
        {
            Target = snapshot.Target;
            Recorded.RemoveRange(snapshot.Recorded, Recorded.Count - snapshot.Recorded);
            Compensate = snapshot.Compensate;
        }
    }

    /// <summary>
    /// Fluent builder of a transition triggered by <typeparamref name="TEvent"/>. Steps run in declaration order;
    /// the state change declared with <see cref="TransitionTo"/> is applied after all steps have completed (when
    /// several run, for example in a <see cref="Branch"/>, the last one wins).
    /// </summary>
    /// <typeparam name="TEvent">The triggering event type.</typeparam>
    public sealed class SagaTransitionBuilder<TEvent> : SagaTransitionDefinition where TEvent : class
    {
        private readonly Saga<TState> _saga;
        private readonly Func<TEvent, bool>? _condition;
        private readonly List<Func<SagaContext<TState, TEvent>, TransitionRun, Task>> _steps = new();
        private readonly List<string> _recorded = new();
        private readonly List<SagaTransitionBuilder<TEvent>> _nested = new();
        private readonly List<(Type Exception, SagaTransitionBuilder<TEvent> Handler)> _faultHandlers = new();

        private readonly Type[]? _joined;

        internal SagaTransitionBuilder(Saga<TState> saga, Func<TEvent, bool>? condition, Type[]? joined = null)
        {
            _saga = saga;
            _condition = condition;
            _joined = joined;
        }

        /// <inheritdoc />
        public override Type EventType => typeof(TEvent);

        /// <summary>
        /// Runs a synchronous action, typically updating the state data.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Then(Action<SagaContext<TState, TEvent>> action)
        {
            ArgumentNullException.ThrowIfNull(action);
            return Step(ctx => action(ctx));
        }

        /// <summary>
        /// Runs an asynchronous action.
        /// </summary>
        /// <param name="action">The action.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> ThenAsync(Func<SagaContext<TState, TEvent>, Task> action)
        {
            ArgumentNullException.ThrowIfNull(action);
            _steps.Add((ctx, _) => action(ctx));
            return this;
        }

        /// <summary>
        /// Publishes an event once the instance has been saved.
        /// </summary>
        /// <typeparam name="TOut">The published EventFlux notification type.</typeparam>
        /// <param name="factory">Creates the event from the context.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Publish<TOut>(Func<SagaContext<TState, TEvent>, TOut> factory) where TOut : IEventRequest
        {
            ArgumentNullException.ThrowIfNull(factory);
            return Step(ctx => ctx.Publish(factory(ctx)));
        }

        /// <summary>
        /// Sends a request and hands its response to <paramref name="onResponse"/>, typically to store a decision in
        /// the state data before a <see cref="Branch"/> or <see cref="TransitionTo"/>. See
        /// <see cref="SagaContext{TState, TEvent}.AskAsync{TResponse}"/>.
        /// </summary>
        /// <typeparam name="TResponse">The response type.</typeparam>
        /// <param name="request">Creates the request from the context.</param>
        /// <param name="onResponse">Receives the response.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Ask<TResponse>(
            Func<SagaContext<TState, TEvent>, IEventRequest<TResponse>> request,
            Action<SagaContext<TState, TEvent>, TResponse> onResponse)
            where TResponse : IEventResponse
        {
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(onResponse);
            _steps.Add(async (ctx, _) => onResponse(ctx, await ctx.AskAsync(request(ctx)).ConfigureAwait(false)));
            return this;
        }

        /// <summary>
        /// Sends a request and awaits <paramref name="onResponse"/> with its response, for a callback that does
        /// asynchronous work. Otherwise the same as <see cref="Ask{TResponse}"/>.
        /// </summary>
        /// <typeparam name="TResponse">The response type.</typeparam>
        /// <param name="request">Creates the request from the context.</param>
        /// <param name="onResponse">Receives the response; the step completes when the returned task does.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> AskAsync<TResponse>(
            Func<SagaContext<TState, TEvent>, IEventRequest<TResponse>> request,
            Func<SagaContext<TState, TEvent>, TResponse, Task> onResponse)
            where TResponse : IEventResponse
        {
            ArgumentNullException.ThrowIfNull(request);
            ArgumentNullException.ThrowIfNull(onResponse);
            _steps.Add(async (ctx, _) => await onResponse(ctx, await ctx.AskAsync(request(ctx)).ConfigureAwait(false)).ConfigureAwait(false));
            return this;
        }

        /// <summary>
        /// Delivers a <typeparamref name="TTimeout"/> event back to the saga after <paramref name="delay"/>, unless it
        /// is unscheduled or the instance reaches <see cref="SagaStates.Final"/> first. Handle it with an ordinary
        /// <c>When&lt;TTimeout&gt;()</c> transition. Scheduling the same type again replaces the pending timeout.
        /// </summary>
        /// <typeparam name="TTimeout">The timeout event type; the saga must be able to correlate it.</typeparam>
        /// <param name="delay">The delay, counted from when the instance is saved.</param>
        /// <param name="factory">Creates the timeout event.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Schedule<TTimeout>(TimeSpan delay, Func<SagaContext<TState, TEvent>, TTimeout> factory)
            where TTimeout : IEventRequest
        {
            ArgumentNullException.ThrowIfNull(factory);
            ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
            _saga._timeoutTypes.Add(typeof(TTimeout));
            return Step(ctx => ctx.Schedule(delay, factory(ctx)));
        }

        /// <summary>
        /// Cancels the pending <typeparamref name="TTimeout"/> timeout of the instance, if any.
        /// </summary>
        /// <typeparam name="TTimeout">The timeout event type.</typeparam>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Unschedule<TTimeout>() where TTimeout : IEventRequest
            => Step(ctx => ctx.Unschedule<TTimeout>());

        /// <summary>
        /// Moves the instance to <paramref name="state"/>.
        /// </summary>
        /// <param name="state">The target state.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> TransitionTo(SagaState state)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(state.Name, nameof(state));
            _steps.Add((_, run) =>
            {
                run.Target = state;
                return Task.CompletedTask;
            });
            return this;
        }

        /// <summary>
        /// Moves the instance to <see cref="SagaStates.Final"/>; it will ignore further events.
        /// </summary>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Finalize() => TransitionTo(SagaStates.Final);

        /// <summary>
        /// Records the compensation <paramref name="key"/> on the instance's compensation stack when this transition
        /// runs. The action must be registered with the saga's <c>OnCompensate(key, ...)</c>.
        /// </summary>
        /// <param name="key">The stable compensation key.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> OnCompensate(string key)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);
            _saga.EnsureCompensable();
            _recorded.Add(key);
            _steps.Add((_, run) =>
            {
                run.Recorded.Add(key);
                return Task.CompletedTask;
            });
            return this;
        }

        /// <summary>
        /// Registers a compensation action under <paramref name="key"/> and records it when this transition runs.
        /// </summary>
        /// <param name="key">The stable compensation key.</param>
        /// <param name="action">The compensation action.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> OnCompensate(string key, Action<SagaCompensationContext<TState>> action)
        {
            _saga.OnCompensate(key, action);
            return OnCompensate(key);
        }

        /// <summary>
        /// Registers an asynchronous compensation action under <paramref name="key"/> and records it when this transition runs.
        /// </summary>
        /// <param name="key">The stable compensation key.</param>
        /// <param name="action">The compensation action.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> OnCompensate(string key, Func<SagaCompensationContext<TState>, Task> action)
        {
            _saga.OnCompensate(key, action);
            return OnCompensate(key);
        }

        /// <summary>
        /// After this transition's steps, runs every pending compensation of the instance in reverse order (LIFO).
        /// </summary>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Compensate()
        {
            _saga.EnsureCompensable();
            _steps.Add((_, run) =>
            {
                run.Compensate = true;
                return Task.CompletedTask;
            });
            return this;
        }

        /// <summary>
        /// Continues with one of two step chains depending on a condition evaluated at this point of the transition.
        /// </summary>
        /// <remarks>
        /// A branch may use every step of a transition, including <see cref="TransitionTo"/>, <see cref="OnCompensate(string)"/>,
        /// <see cref="Compensate"/> and nested branches. Steps declared after the branch run whichever way it went.
        /// </remarks>
        /// <param name="condition">Evaluated when the branch is reached.</param>
        /// <param name="then">The steps to run when <paramref name="condition"/> is <c>true</c>.</param>
        /// <param name="otherwise">The steps to run otherwise; <c>null</c> runs nothing.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> Branch(
            Func<SagaContext<TState, TEvent>, bool> condition,
            Action<SagaTransitionBuilder<TEvent>> then,
            Action<SagaTransitionBuilder<TEvent>>? otherwise = null)
        {
            ArgumentNullException.ThrowIfNull(condition);
            ArgumentNullException.ThrowIfNull(then);
            var whenTrue = Nested(then);
            var whenFalse = otherwise is null ? null : Nested(otherwise);
            _steps.Add((ctx, run) => condition(ctx)
                ? whenTrue.RunAsync(ctx, run)
                : whenFalse?.RunAsync(ctx, run) ?? Task.CompletedTask);
            return this;
        }

        /// <summary>
        /// Handles a <typeparamref name="TException"/> thrown by a step of this transition inside the saga, instead
        /// of failing the message (and leaving it to the transport's retry).
        /// </summary>
        /// <remarks>
        /// <para>
        /// When a step throws, the events it had published or scheduled and the state change, compensation records
        /// and <see cref="Compensate"/> request it had declared are discarded, then the <paramref name="handle"/>
        /// steps run with <see cref="SagaContext{TState, TEvent}.Fault"/> set. Changes the failed steps made to the
        /// state data are kept. The instance is saved as usual.
        /// </para>
        /// <para>
        /// Handlers are tried in declaration order; the first whose exception type matches runs. Cancellation of the
        /// event's own token and <see cref="SagaConfigurationException"/> are never handled. An exception thrown by
        /// the handler fails the message.
        /// </para>
        /// </remarks>
        /// <typeparam name="TException">The handled exception type (derived types match too).</typeparam>
        /// <param name="handle">The steps to run instead, for example <c>f =&gt; f.Publish(...).TransitionTo(Faulted)</c>.</param>
        /// <returns>This builder.</returns>
        public SagaTransitionBuilder<TEvent> OnFault<TException>(Action<SagaTransitionBuilder<TEvent>> handle) where TException : Exception
        {
            ArgumentNullException.ThrowIfNull(handle);
            _faultHandlers.Add((typeof(TException), Nested(handle)));
            return this;
        }

        internal override IEnumerable<Type> TriggerTypes => _joined ?? [EventType];

        internal override bool Accepts(Type eventType)
            => _joined is null ? EventType.IsAssignableFrom(eventType) : _joined.Any(t => t.IsAssignableFrom(eventType));

        internal override bool Matches(object @event) => _condition is null || _condition((TEvent)@event);

        internal override IEnumerable<string> RecordedCompensations => _recorded.Concat(_nested.SelectMany(n => n.RecordedCompensations));

        internal override async Task<TransitionOutcome> ExecuteAsync(TState state, object @event, SagaEffects effects, CancellationToken ct)
        {
            if (_joined is not null && !Join(state, @event))
                return new TransitionOutcome(null, [], false, null);

            var run = new TransitionRun(effects);
            await RunAsync(new SagaContext<TState, TEvent>(state, (TEvent)@event, effects, ct), run).ConfigureAwait(false);
            return new TransitionOutcome(run.Target, run.Recorded, run.Compensate, run.Fault);
        }

        private async Task RunAsync(SagaContext<TState, TEvent> context, TransitionRun run)
        {
            if (_faultHandlers.Count == 0)
            {
                await RunStepsAsync(context, run).ConfigureAwait(false);
                return;
            }

            var effects = run.Effects.Snapshot();
            var outcome = run.Snapshot();
            try
            {
                await RunStepsAsync(context, run).ConfigureAwait(false);
            }
            catch (Exception ex) when (FindFaultHandler(ex, context.CancellationToken) is { } handler)
            {
                run.Effects.Restore(effects);
                run.Restore(outcome);
                run.Fault = ex;
                await handler.RunAsync(context.WithFault(ex), run).ConfigureAwait(false);
            }
        }

        // Records the event of a WhenAll join; true when every awaited event has arrived (the join then starts over).
        private bool Join(TState state, object @event)
        {
            var key = string.Join("+", _joined!.Select(t => t.Name));
            var received = ((IJoinableSagaState)state).JoinedEvents ??= new List<string>();
            var arrived = _joined!.First(t => t.IsInstanceOfType(@event));
            var entry = $"{key}/{arrived.Name}";
            if (!received.Contains(entry))
                received.Add(entry);

            if (!_joined!.All(t => received.Contains($"{key}/{t.Name}")))
                return false;

            received.RemoveAll(e => e.StartsWith(key + "/", StringComparison.Ordinal));
            return true;
        }

        private async Task RunStepsAsync(SagaContext<TState, TEvent> context, TransitionRun run)
        {
            foreach (var step in _steps)
                await step(context, run).ConfigureAwait(false);
        }

        private SagaTransitionBuilder<TEvent>? FindFaultHandler(Exception ex, CancellationToken ct)
        {
            if (ex is OperationCanceledException && ct.IsCancellationRequested) return null;
            if (ex is SagaConfigurationException) return null;
            foreach (var (type, handler) in _faultHandlers)
                if (type.IsInstanceOfType(ex)) return handler;
            return null;
        }

        private SagaTransitionBuilder<TEvent> Nested(Action<SagaTransitionBuilder<TEvent>> configure)
        {
            var nested = new SagaTransitionBuilder<TEvent>(_saga, null);
            configure(nested);
            _nested.Add(nested);
            return nested;
        }

        private SagaTransitionBuilder<TEvent> Step(Action<SagaContext<TState, TEvent>> action)
        {
            _steps.Add((ctx, _) =>
            {
                action(ctx);
                return Task.CompletedTask;
            });
            return this;
        }
    }
}
