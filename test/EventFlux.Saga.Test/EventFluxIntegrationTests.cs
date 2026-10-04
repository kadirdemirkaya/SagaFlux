using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Test.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public sealed class Recorder
{
    public ConcurrentQueue<object> Events { get; } = new();
}

public sealed class ChargeCustomerHandler(Recorder recorder) : IEventHandler<ChargeCustomer>
{
    public Task Handle(ChargeCustomer @event, CancellationToken cancellationToken)
    {
        recorder.Events.Enqueue(@event);
        return Task.CompletedTask;
    }
}

public sealed class ShipOrderHandler(Recorder recorder) : IEventHandler<ShipOrder>
{
    public Task Handle(ShipOrder @event, CancellationToken cancellationToken)
    {
        recorder.Events.Enqueue(@event);
        return Task.CompletedTask;
    }
}

public class EventFluxIntegrationTests
{
    private static ServiceProvider BuildProvider(Action<SagaFluxBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(EventFluxIntegrationTests).Assembly);
        var builder = services.AddSagaFlux().AddSaga<OrderSaga, OrderState>();
        configure?.Invoke(builder);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }

    [Fact]
    public async Task Saga_runs_in_process_through_event_dispatcher()
    {
        await using var provider = BuildProvider();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var repository = provider.GetRequiredService<ISagaRepository<OrderState>>();
        var recorder = provider.GetRequiredService<Recorder>();
        var orderId = Guid.NewGuid();

        await dispatcher.PublishAsync(new OrderPlaced(orderId, 10m));
        Assert.Equal(OrderSaga.AwaitingPayment.Name, (await repository.FindAsync(orderId, default))!.CurrentState);
        Assert.Contains(recorder.Events, e => e is ChargeCustomer c && c.OrderId == orderId);

        await dispatcher.PublishAsync(new PaymentReceived(orderId));
        var state = (await repository.FindAsync(orderId, default))!;
        Assert.Equal(OrderSaga.Shipping.Name, state.CurrentState);
        Assert.Equal(2, state.Version);
        Assert.Contains(recorder.Events, e => e is ShipOrder s && s.OrderId == orderId);

        await dispatcher.PublishAsync(new OrderShipped(orderId));
        Assert.Equal(SagaStates.Final.Name, (await repository.FindAsync(orderId, default))!.CurrentState);
    }

    [Fact]
    public async Task Transport_can_be_replaced_without_touching_the_saga()
    {
        var transport = new RecordingTransport();
        await using var provider = BuildProvider(b =>
        {
            b.Services.AddSingleton(transport);
            b.UseTransport<RecordingTransportAdapter>();
        });
        var orderId = Guid.NewGuid();

        await provider.GetRequiredService<IEventDispatcher>().PublishAsync(new OrderPlaced(orderId, 3m));

        Assert.IsType<ChargeCustomer>(Assert.Single(transport.Published));
        Assert.Empty(provider.GetRequiredService<Recorder>().Events);
    }

    [Fact]
    public async Task Assembly_scanning_registers_sagas_once()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSagaFlux(typeof(OrderSaga).Assembly).AddSaga<OrderSaga, OrderState>();

        Assert.Single(services, d => d.ServiceType == typeof(OrderSaga));
        await using var provider = services.BuildServiceProvider();
        Assert.NotNull(provider.GetRequiredService<ISagaEventRouter>());
    }

    public sealed class RecordingTransport
    {
        public ConcurrentQueue<IEventRequest> Published { get; } = new();
    }

    public sealed class RecordingTransportAdapter(RecordingTransport inner) : ISagaTransport
    {
        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            inner.Published.Enqueue(@event);
            return Task.CompletedTask;
        }
    }
}
