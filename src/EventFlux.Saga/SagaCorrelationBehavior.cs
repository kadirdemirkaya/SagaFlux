using EventFlux.Abstractions;
using EventFlux.Delegates;
using EventFlux.Saga.Internal;

namespace EventFlux.Saga;

/// <summary>
/// EventFlux pipeline behavior that feeds notifications published through <see cref="IEventDispatcher"/> to the
/// registered sagas, after the regular handlers have run. Registered by <c>AddSagaFlux()</c>. Notifications
/// published through <see cref="IEventBus"/> reach the sagas through <see cref="SagaEventHandler{TEvent}"/> instead.
/// </summary>
/// <typeparam name="TEvent">The notification type.</typeparam>
public sealed class SagaCorrelationBehavior<TEvent> : IEventCustomPipeline<TEvent> where TEvent : IEventRequest
{
    private readonly ISagaEventRouter _router;

    /// <summary>
    /// Initializes the behavior.
    /// </summary>
    /// <param name="router">The saga router.</param>
    public SagaCorrelationBehavior(ISagaEventRouter router) => _router = router;

    /// <inheritdoc />
    public async Task Handle(TEvent request, EventHandlerDelegate next, CancellationToken cancellationToken)
    {
        await SagaPipelineScope.RunAsync(request, () => next(cancellationToken)).ConfigureAwait(false);
        await _router.RouteAsync(request, cancellationToken).ConfigureAwait(false);
    }
}

/// <summary>
/// EventFlux pipeline behavior that feeds sent requests (commands) to the registered sagas, after their handler has
/// produced a response. Registered by <c>AddSagaFlux()</c>.
/// </summary>
/// <typeparam name="TRequest">The request type.</typeparam>
/// <typeparam name="TResponse">The response type.</typeparam>
public sealed class SagaCorrelationBehavior<TRequest, TResponse> : IEventCustomPipeline<TRequest, TResponse>
    where TRequest : IEventRequest<TResponse>
    where TResponse : IEventResponse
{
    private readonly ISagaEventRouter _router;

    /// <summary>
    /// Initializes the behavior.
    /// </summary>
    /// <param name="router">The saga router.</param>
    public SagaCorrelationBehavior(ISagaEventRouter router) => _router = router;

    /// <inheritdoc />
    public async Task<TResponse> Handle(TRequest request, EventHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
    {
        var response = await next(cancellationToken).ConfigureAwait(false);
        await _router.RouteAsync(request, cancellationToken).ConfigureAwait(false);
        return response;
    }
}
