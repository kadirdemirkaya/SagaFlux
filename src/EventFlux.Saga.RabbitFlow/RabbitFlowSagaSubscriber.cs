using System.Reflection;
using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.RabbitFlow;

internal sealed class RabbitFlowSagaSubscriber(
    IServiceProvider provider,
    ISagaEventCatalog catalog,
    RabbitFlowSagaOptions options,
    ILogger<RabbitFlowSagaSubscriber> logger) : IHostedService
{
    private static readonly MethodInfo SubscribeMethod =
        typeof(RabbitFlowSagaSubscriber).GetMethod(nameof(SubscribeAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!options.SubscribeToSagaEvents || catalog.EventTypes.Count == 0)
            return;

        // EventFlux.RabbitFlow registers the broker as a singleton, so its consumers live until the host stops.
        var broker = provider.GetRequiredService<IEventBroker>();

        foreach (var eventType in catalog.EventTypes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var subscription = (Task)SubscribeMethod.MakeGenericMethod(eventType).Invoke(null, [broker, options.ExchangeName])!;
            await subscription.ConfigureAwait(false);
            logger.LogInformation("SagaFlux subscribed to {Event} through RabbitFlow.", eventType.Name);
        }
    }

    // The broker owns its consumers and drains them when it is disposed (RabbitFlowOptions.ShutdownTimeout).
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static Task SubscribeAsync<TEvent>(IEventBroker broker, string? exchangeName) where TEvent : IEventRequest
        => broker.SubscribeAsync<TEvent, SagaEventHandler<TEvent>>(exchangeName!);
}
