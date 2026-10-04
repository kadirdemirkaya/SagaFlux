using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventFlux.Saga.RabbitFlow.Test;

public class RabbitFlowSagaSubscriberTests
{
    private static (ServiceProvider Provider, FakeBroker Broker) Build(Action<RabbitFlowSagaOptions>? configure = null)
    {
        var broker = new FakeBroker();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IEventBroker>(broker);
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseRabbitFlow(configure);
        return (services.BuildServiceProvider(), broker);
    }

    [Fact]
    public async Task Subscribes_every_saga_event_with_the_saga_handler_and_leaves_the_broker_to_the_container()
    {
        var (provider, broker) = Build(o => o.ExchangeName = "sagas");
        await using var _ = provider;
        var hosted = provider.GetRequiredService<IHostedService>();

        await hosted.StartAsync(default);

        Assert.Equal(
            new[] { typeof(OrderPlaced), typeof(PaymentReceived) }.OrderBy(t => t.Name),
            broker.Subscriptions.Select(s => s.Event).OrderBy(t => t.Name));
        Assert.All(broker.Subscriptions, s =>
        {
            Assert.Equal(typeof(SagaEventHandler<>).MakeGenericType(s.Event), s.Handler);
            Assert.Equal("sagas", s.Exchange);
        });
        Assert.False(broker.Disposed);

        await hosted.StopAsync(default);
        Assert.False(broker.Disposed);
    }

    [Fact]
    public async Task Subscription_can_be_disabled()
    {
        var (provider, broker) = Build(o => o.SubscribeToSagaEvents = false);
        await using var _ = provider;

        await provider.GetRequiredService<IHostedService>().StartAsync(default);

        Assert.Empty(broker.Subscriptions);
    }
}
