using EventFlux.RedisFlow.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventFlux.Saga.RedisFlow.Test;

public class RedisFlowSagaTransportTests
{
    [Fact]
    public async Task Publishes_with_the_typed_RedisFlow_publisher_and_passes_the_token()
    {
        var publisher = new FakePublisher();
        using var cts = new CancellationTokenSource();
        var transport = new RedisFlowSagaTransport(publisher);

        await transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), cts.Token);

        var (published, token) = Assert.Single(publisher.Published);
        Assert.IsType<ChargeCustomer>(published);
        Assert.Equal(cts.Token, token);
    }

    [Fact]
    public async Task Cancelled_publish_does_not_reach_the_stream()
    {
        var publisher = new FakePublisher();
        var transport = new RedisFlowSagaTransport(publisher);

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => transport.PublishAsync(new ChargeCustomer(Guid.NewGuid(), 1m), new CancellationToken(true)));
        Assert.Empty(publisher.Published);
    }

    [Fact]
    public async Task Saga_events_go_to_the_stream_without_changing_the_saga()
    {
        var publisher = new FakePublisher();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IRedisStreamPublisher>(publisher);
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseRedisFlow();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var orderId = Guid.NewGuid();

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<EventFlux.Abstractions.IEventDispatcher>().PublishAsync(new OrderPlaced(orderId, 9m));

        var charge = Assert.IsType<ChargeCustomer>(Assert.Single(publisher.Published).Event);
        Assert.Equal(orderId, charge.OrderId);
    }

    [Fact]
    public void UseRedisFlow_replaces_the_transport_and_registers_the_event_types_first()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHostedService, SomeWorker>();
        services.AddSagaFlux().UseRedisFlow().UseRedisFlow();

        var transport = Assert.Single(services, d => d.ServiceType == typeof(ISagaTransport));
        Assert.Equal(typeof(RedisFlowSagaTransport), transport.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, transport.Lifetime);
        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).ToList();
        Assert.Equal(2, hosted.Count);
        Assert.Equal(typeof(SomeWorker), hosted[1].ImplementationType);
    }

    private sealed class SomeWorker : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}

public class RedisSagaLockRegistrationTests
{
    [Fact]
    public void UseRedisDistributedLock_replaces_the_lock_provider_and_keeps_one_options_instance()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux()
            .UseRedisDistributedLock(o => o.KeyPrefix = "a:")
            .UseRedisDistributedLock(o => o.AcquireTimeout = TimeSpan.FromSeconds(5));

        var provider = Assert.Single(services, d => d.ServiceType == typeof(Concurrency.ISagaLockProvider));
        Assert.Equal(typeof(RedisSagaLockProvider), provider.ImplementationType);
        var options = (RedisSagaLockOptions)Assert.Single(services, d => d.ServiceType == typeof(RedisSagaLockOptions)).ImplementationInstance!;
        Assert.Equal("a:", options.KeyPrefix);
        Assert.Equal(TimeSpan.FromSeconds(5), options.AcquireTimeout);
    }
}
