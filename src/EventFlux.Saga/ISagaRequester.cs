using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// Sends a request from inside a saga transition and returns its response (see <c>.Ask(...)</c>).
/// </summary>
/// <remarks>
/// The runtime implementation sends through EventFlux's <see cref="IEventDispatcher"/> to the request's
/// <c>IEventHandler&lt;TRequest, TResponse&gt;</c>; the test harness answers with the responses given to
/// <c>Answer&lt;TRequest, TResponse&gt;(...)</c>.
/// </remarks>
public interface ISagaRequester
{
    /// <summary>
    /// Sends <paramref name="request"/> and waits for its response.
    /// </summary>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="request">The request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The response, or <c>null</c> when the handler returned none.</returns>
    Task<TResponse?> AskAsync<TResponse>(IEventRequest<TResponse> request, CancellationToken ct) where TResponse : IEventResponse;
}
