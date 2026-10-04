using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Test.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class TransitionObserverTests
{
    private sealed class RecordingObserver : ISagaTransitionObserver
    {
        public ConcurrentQueue<SagaTransitionRecord> Records { get; } = new();

        public Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct)
        {
            Records.Enqueue(record);
            return Task.CompletedTask;
        }
    }

    private sealed class FailingObserver : ISagaTransitionObserver
    {
        public Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct) => throw new InvalidOperationException("observer down");
    }

    private static ServiceProvider Build(params ISagaTransitionObserver[] observers)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(TransitionObserverTests).Assembly);
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().AddSaga<TravelSaga, TravelState>();
        foreach (var observer in observers)
            services.AddSingleton(observer);
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task Every_handled_event_produces_one_record_in_order()
    {
        var observer = new RecordingObserver();
        await using var provider = Build(observer);
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();

        await dispatcher.PublishAsync(new OrderPlaced(id, 3m));
        await dispatcher.PublishAsync(new PaymentFailed(id, "declined"));
        await dispatcher.PublishAsync(new PaymentFailed(id, "ignored in Cancelled"));

        var records = observer.Records.Where(r => r.CorrelationId == id).ToArray();
        Assert.Equal(2, records.Length);

        Assert.Equal(nameof(OrderSaga), records[0].SagaType);
        Assert.Equal(nameof(OrderPlaced), records[0].EventType);
        Assert.Equal("Initial", records[0].FromState);
        Assert.Equal(OrderSaga.AwaitingPayment.Name, records[0].ToState);
        Assert.True(records[0].Created);
        Assert.Equal(1, records[0].Version);
        Assert.Equal(new[] { nameof(ChargeCustomer) }, records[0].Published);

        Assert.Equal(OrderSaga.Cancelled.Name, records[1].ToState);
        Assert.False(records[1].Created);
        Assert.Equal(2, records[1].Version);
    }

    [Fact]
    public async Task Compensations_appear_in_the_record()
    {
        var observer = new RecordingObserver();
        await using var provider = Build(observer);
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();

        foreach (var e in new IEventRequest[] { new TripRequested(id), new FlightBooked(id), new HotelBooked(id), new CarFailed(id) })
            await dispatcher.PublishAsync(e);

        var last = observer.Records.Last(r => r.CorrelationId == id);
        Assert.Equal(new[] { TravelSaga.CancelHotelKey, TravelSaga.CancelFlightKey }, last.Compensated);
    }

    [Fact]
    public async Task Failing_observer_does_not_break_the_saga()
    {
        var observer = new RecordingObserver();
        await using var provider = Build(new FailingObserver(), observer);
        var id = Guid.NewGuid();

        await provider.GetRequiredService<IEventDispatcher>().PublishAsync(new OrderPlaced(id, 3m));

        var state = await provider.GetRequiredService<ISagaRepository<OrderState>>().FindAsync(id, default);
        Assert.Equal(OrderSaga.AwaitingPayment.Name, state!.CurrentState);
        Assert.Single(observer.Records, r => r.CorrelationId == id);
    }
}
