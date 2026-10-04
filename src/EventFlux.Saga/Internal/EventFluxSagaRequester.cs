using EventFlux.Abstractions;

namespace EventFlux.Saga.Internal;

internal sealed class EventFluxSagaRequester(IEventDispatcher dispatcher) : ISagaRequester
{
    public Task<TResponse?> AskAsync<TResponse>(IEventRequest<TResponse> request, CancellationToken ct) where TResponse : IEventResponse
        => dispatcher.SendAsync(request, ct);
}
