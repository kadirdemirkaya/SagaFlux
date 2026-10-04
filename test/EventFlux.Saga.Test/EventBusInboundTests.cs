using EventFlux.Abstractions;
using EventFlux.Saga.Test.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class EventBusInboundTests
{
    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(EventBusInboundTests).Assembly);
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Events_published_through_event_bus_drive_the_saga()
    {
        await using var provider = BuildProvider();
        var bus = provider.GetRequiredService<IEventBus>();
        var repository = provider.GetRequiredService<ISagaRepository<OrderState>>();
        var orderId = Guid.NewGuid();

        await bus.PublishAsync(new OrderPlaced(orderId, 4m));
        await bus.PublishAsync(new PaymentReceived(orderId));

        var state = (await repository.FindAsync(orderId, default))!;
        Assert.Equal(OrderSaga.Shipping.Name, state.CurrentState);
        Assert.Equal(2, state.Version);
    }

    [Fact]
    public async Task Events_published_through_dispatcher_are_routed_once()
    {
        await using var provider = BuildProvider();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var repository = provider.GetRequiredService<ISagaRepository<OrderState>>();
        var orderId = Guid.NewGuid();

        await dispatcher.PublishAsync(new OrderPlaced(orderId, 4m));

        var state = (await repository.FindAsync(orderId, default))!;
        Assert.Equal(1, state.Version);
        Assert.Single(provider.GetRequiredService<Recorder>().Events, e => e is ChargeCustomer c && c.OrderId == orderId);
    }

    [Fact]
    public void Catalog_lists_the_notification_events_sagas_react_to()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().AddSaga<TravelSaga, TravelState>();
        using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredService<ISagaEventCatalog>();

        Assert.Contains(typeof(OrderPlaced), catalog.EventTypes);
        Assert.Contains(typeof(CarFailed), catalog.EventTypes);
        Assert.DoesNotContain(typeof(ShipOrder), catalog.EventTypes);
    }
}
