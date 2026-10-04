namespace EventFlux.Saga.Internal;

internal sealed class SagaEventRouter(IEnumerable<ISagaRunner> runners) : ISagaEventRouter
{
    public async Task RouteAsync(object @event, CancellationToken ct)
    {
        var type = @event.GetType();
        foreach (var runner in runners)
        {
            if (runner.Handles(type))
                await runner.RunAsync(@event, ct).ConfigureAwait(false);
        }
    }
}
