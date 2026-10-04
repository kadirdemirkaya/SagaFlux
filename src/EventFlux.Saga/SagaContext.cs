using EventFlux.Abstractions;
using EventFlux.Saga.Internal;

namespace EventFlux.Saga;

/// <summary>
/// The context passed to the actions of a transition.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
/// <typeparam name="TEvent">The triggering event type.</typeparam>
public sealed class SagaContext<TState, TEvent> where TState : class, ISagaState
{
    private readonly SagaEffects _effects;

    internal SagaContext(TState state, TEvent @event, SagaEffects effects, CancellationToken cancellationToken, Exception? fault = null)
    {
        State = state;
        Event = @event;
        _effects = effects;
        CancellationToken = cancellationToken;
        Fault = fault;
    }

    /// <summary>
    /// Gets the exception being handled inside an <c>OnFault&lt;TException&gt;(...)</c> handler; <c>null</c> elsewhere.
    /// </summary>
    public Exception? Fault { get; }

    /// <summary>
    /// Sends a request and waits for its response, for example to ask another component for a decision the
    /// transition depends on. In-process, the request goes to its EventFlux <c>IEventHandler&lt;TRequest, TResponse&gt;</c>.
    /// </summary>
    /// <remarks>
    /// The request runs while the instance is being processed (and locked, when a lock provider is used), so its
    /// handler must not wait for this same saga instance. To talk to another service over a broker, publish a
    /// command and handle the reply with an ordinary <c>When&lt;TReply&gt;()</c> transition instead.
    /// </remarks>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="request">The request.</param>
    /// <returns>The response.</returns>
    /// <exception cref="SagaConfigurationException">Thrown when no requester is available.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the handler returned no response.</exception>
    public async Task<TResponse> AskAsync<TResponse>(IEventRequest<TResponse> request) where TResponse : IEventResponse
    {
        ArgumentNullException.ThrowIfNull(request);
        var requester = _effects.Requester
            ?? throw new SagaConfigurationException(
                $"No {nameof(ISagaRequester)} is available to send '{request.GetType().Name}'. The runtime provides one; in the test harness, answer the request with Answer<{request.GetType().Name}, {typeof(TResponse).Name}>(...).");
        _effects.Asked.Add(request);
        var response = await requester.AskAsync(request, CancellationToken).ConfigureAwait(false);
        return response ?? throw new InvalidOperationException($"The handler of '{request.GetType().Name}' returned no {typeof(TResponse).Name}.");
    }

    internal SagaContext<TState, TEvent> WithFault(Exception fault) => new(State, Event, _effects, CancellationToken, fault);

    /// <summary>
    /// Gets the saga instance data. Changes are persisted after the transition completes.
    /// </summary>
    public TState State { get; }

    /// <summary>
    /// Gets the triggering event.
    /// </summary>
    public TEvent Event { get; }

    /// <summary>
    /// Gets the correlation id of the instance.
    /// </summary>
    public Guid CorrelationId => State.CorrelationId;

    /// <summary>
    /// Gets the cancellation token of the dispatch.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    /// Queues an event to be published through the configured <see cref="ISagaTransport"/> once the instance has been saved.
    /// </summary>
    /// <typeparam name="TOut">The published EventFlux notification type.</typeparam>
    /// <param name="event">The event to publish.</param>
    public void Publish<TOut>(TOut @event) where TOut : IEventRequest
    {
        ArgumentNullException.ThrowIfNull(@event);
        _effects.Published.Add(SagaPublishedEvent.Create(@event));
    }

    /// <summary>
    /// Delivers <paramref name="event"/> back to the saga after <paramref name="delay"/>, counted from when the
    /// instance is saved. Scheduling the same timeout type again replaces the pending one.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type; the saga must be able to correlate it.</typeparam>
    /// <param name="delay">The delay.</param>
    /// <param name="event">The event delivered when the timeout fires.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="delay"/> is negative.</exception>
    public void Schedule<TTimeout>(TimeSpan delay, TTimeout @event) where TTimeout : IEventRequest
    {
        ArgumentNullException.ThrowIfNull(@event);
        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        _effects.Schedule(new SagaScheduledTimeout(SagaTimeoutKeys.For<TTimeout>(), delay, @event));
    }

    /// <summary>
    /// Cancels the pending <typeparamref name="TTimeout"/> timeout of this instance, if any.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type.</typeparam>
    public void Unschedule<TTimeout>() where TTimeout : IEventRequest
        => _effects.Unschedule(SagaTimeoutKeys.For<TTimeout>());
}
