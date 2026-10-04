using EventFlux.RedisFlow.Redis;
using Microsoft.Extensions.Hosting;

namespace EventFlux.Saga.RedisFlow;

/// <summary>
/// Registers the events the sagas react to with the RedisFlow <see cref="EventTypeResolver"/> before the RedisFlow
/// worker starts reading, so stream entries resolve to the saga event types instead of a <c>GenericEvent</c>.
/// </summary>
internal sealed class RedisFlowSagaEventRegistration(ISagaEventCatalog catalog) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var eventType in catalog.EventTypes)
            EventTypeResolver.RegisterEvent(eventType);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
