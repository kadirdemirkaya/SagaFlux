using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventFlux.Saga.RabbitFlow.Test;

public class RabbitFlowSagaTransportTests
{
    [Fact]
    public async Task Publishes_to_default_exchange()
    {
        var broker = new FakeBroker();
        var transport = new RabbitFlowSagaTransport(broker, new RabbitFlowSagaOptions());

        await transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), default);

        var (published, exchange) = Assert.Single(broker.Published);
        Assert.IsType<ChargeCustomer>(published);
        Assert.Null(exchange);
    }

    [Fact]
    public async Task Publishes_to_configured_exchange()
    {
        var broker = new FakeBroker();
        var transport = new RabbitFlowSagaTransport(broker, new RabbitFlowSagaOptions { ExchangeName = "sagas" });

        await transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), default);

        Assert.Equal("sagas", Assert.Single(broker.Published).Exchange);
    }

    [Fact]
    public async Task Passes_the_cancellation_token_to_the_broker()
    {
        var broker = new FakeBroker();
        using var cts = new CancellationTokenSource();
        var transport = new RabbitFlowSagaTransport(broker, new RabbitFlowSagaOptions());

        await transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), cts.Token);

        Assert.Equal(cts.Token, Assert.Single(broker.PublishTokens));
    }

    [Fact]
    public async Task Cancelled_publish_does_not_reach_the_broker()
    {
        var broker = new FakeBroker();
        var transport = new RabbitFlowSagaTransport(broker, new RabbitFlowSagaOptions());

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), new CancellationToken(true)));
        Assert.Empty(broker.Published);
    }

    [Fact]
    public async Task Saga_events_go_through_the_broker_without_changing_the_saga()
    {
        var broker = new FakeBroker();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IEventBroker>(broker);
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseRabbitFlow(o => o.SubscribeToSagaEvents = false);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var orderId = Guid.NewGuid();

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new OrderPlaced(orderId, 9m));

        var charge = Assert.IsType<ChargeCustomer>(Assert.Single(broker.Published).Event);
        Assert.Equal(orderId, charge.OrderId);
    }

    [Fact]
    public void UseRabbitFlow_replaces_transport_and_registers_one_subscriber()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux().UseRabbitFlow().UseRabbitFlow(o => o.ExchangeName = "x");

        var transport = Assert.Single(services, d => d.ServiceType == typeof(ISagaTransport));
        Assert.Equal(typeof(RabbitFlowSagaTransport), transport.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, transport.Lifetime);
        Assert.Single(services, d => d.ServiceType == typeof(IHostedService));
        var options = Assert.Single(services, d => d.ServiceType == typeof(RabbitFlowSagaOptions)).ImplementationInstance;
        Assert.Equal("x", ((RabbitFlowSagaOptions)options!).ExchangeName);
    }
}
