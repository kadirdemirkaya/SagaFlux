using EventFlux.Saga.Inspector;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace Microsoft.AspNetCore.Builder;

/// <summary>
/// Maps the saga inspector endpoints.
/// </summary>
public static class SagaInspectorEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the inspector under <paramref name="prefix"/>:
    /// <list type="bullet">
    /// <item><description><c>GET {prefix}</c>: a minimal HTML page to look up an instance.</description></item>
    /// <item><description><c>GET {prefix}/instances/{correlationId}</c>: the instance timeline as JSON, oldest first (404 when unknown).</description></item>
    /// <item><description><c>GET {prefix}/recent?take=50</c>: the latest records as JSON, newest first (take 1–500).</description></item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// The timeline exposes internal business data. Protect the returned group, for example with
    /// <c>.RequireAuthorization(...)</c>, before enabling it outside development.
    /// </remarks>
    /// <param name="endpoints">The endpoint route builder.</param>
    /// <param name="prefix">The route prefix.</param>
    /// <returns>The route group, for conventions such as authorization.</returns>
    public static RouteGroupBuilder MapSagaInspector(this IEndpointRouteBuilder endpoints, string prefix = "/saga-inspector")
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);

        var group = endpoints.MapGroup(prefix);

        group.MapGet("/", () => Results.Content(SagaInspectorPage.Html, "text/html; charset=utf-8"));

        group.MapGet("/instances/{correlationId:guid}", async (Guid correlationId, ISagaTimelineStore store, CancellationToken ct) =>
        {
            var timeline = await store.GetTimelineAsync(correlationId, ct).ConfigureAwait(false);
            return timeline.Count == 0 ? Results.NotFound() : Results.Ok(timeline);
        });

        group.MapGet("/recent", async (int? take, ISagaTimelineStore store, CancellationToken ct) =>
        {
            var count = Math.Clamp(take ?? 50, 1, 500);
            return Results.Ok(await store.GetRecentAsync(count, ct).ConfigureAwait(false));
        });

        return group;
    }
}
