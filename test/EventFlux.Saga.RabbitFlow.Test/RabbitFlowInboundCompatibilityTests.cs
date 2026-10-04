using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using EventFlux.RabbitMQ.Context;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace EventFlux.Saga.RabbitFlow.Test;

public class RabbitFlowInboundCompatibilityTests
{
    public sealed record OrderExploded(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class ExplodingState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public sealed class ExplodingSaga : Saga<ExplodingState>
    {
        public ExplodingSaga() =>
            During(SagaStates.Initial,
                When<OrderExploded>().Then(_ => throw new InvalidOperationException("boom")));
    }

    // Drives the real EventFlux.RabbitFlow consumer path (deserialize -> IEventBus) without a broker.
    private sealed class Consumer(IServiceProvider provider, IEventBusSubscriptionsManager subscriptions)
        : SubscribeProcessEvent(
            provider.GetRequiredService<ILogger<IEventBroker>>(),
            subscriptions,
            provider.GetRequiredService<IServiceScopeFactory>(),
            "orders",
            new EventFluxContextAccessor())
    {
        public Task DeliverAsync(object @event) =>
            ProcessEvent(@event.GetType().Name, JsonConvert.SerializeObject(@event), null, CancellationToken.None);
    }

    private static (ServiceProvider Provider, FakeBroker Broker, Consumer Consumer) Build()
    {
        var broker = new FakeBroker();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IEventBroker>(broker);
        services.AddEventBus(typeof(RabbitFlowInboundCompatibilityTests).Assembly);
        services.AddSagaFlux()
            .AddSaga<OrderSaga, OrderState>()
            .AddSaga<ExplodingSaga, ExplodingState>()
            .UseRabbitFlow();
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var subscriptions = new EventBusSubscriptionsManager();
        subscriptions.AddSubscription<OrderPlaced, SagaEventHandler<OrderPlaced>>();
        subscriptions.AddSubscription<PaymentReceived, SagaEventHandler<PaymentReceived>>();
        subscriptions.AddSubscription<OrderExploded, SagaEventHandler<OrderExploded>>();
        return (provider, broker, new Consumer(provider, subscriptions));
    }

    [Fact]
    public async Task Messages_from_the_RabbitFlow_consumer_drive_the_saga_to_its_final_state()
    {
        var (provider, broker, consumer) = Build();
        await using var _ = provider;
        var orderId = Guid.NewGuid();
        var repository = provider.GetRequiredService<ISagaRepository<OrderState>>();

        await consumer.DeliverAsync(new OrderPlaced(orderId, 15m));

        var state = (await repository.FindAsync(orderId, default))!;
        Assert.Equal(OrderSaga.AwaitingPayment.Name, state.CurrentState);
        Assert.Equal(15m, state.Amount);
        var charge = Assert.IsType<ChargeCustomer>(Assert.Single(broker.Published).Event);
        Assert.Equal(orderId, charge.OrderId);

        await consumer.DeliverAsync(new PaymentReceived(orderId));

        Assert.Equal(SagaStates.Final.Name, (await repository.FindAsync(orderId, default))!.CurrentState);
    }

    [Fact]
    public async Task A_failing_saga_step_surfaces_to_the_consumer_so_the_message_is_not_acknowledged()
    {
        // EventFlux.RabbitFlow >= 1.1.0 nacks (requeue or dead-letter) a delivery whose processing throws;
        // SagaFlux must therefore let the failure reach the consumer instead of swallowing it.
        var (provider, _, consumer) = Build();
        await using var __ = provider;
        var id = Guid.NewGuid();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => consumer.DeliverAsync(new OrderExploded(id)));

        Assert.Contains("boom", ex.ToString());
        Assert.Null(await provider.GetRequiredService<ISagaRepository<ExplodingState>>().FindAsync(id, default));
    }
}
