using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Test.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public sealed class UndoLog
{
    public ConcurrentQueue<IEventRequest> Commands { get; } = new();
}

public sealed class CancelFlightHandler(UndoLog log) : IEventHandler<CancelFlight>
{
    public Task Handle(CancelFlight @event, CancellationToken cancellationToken)
    {
        log.Commands.Enqueue(@event);
        return Task.CompletedTask;
    }
}

public sealed class CancelHotelHandler(UndoLog log) : IEventHandler<CancelHotel>
{
    public Task Handle(CancelHotel @event, CancellationToken cancellationToken)
    {
        log.Commands.Enqueue(@event);
        return Task.CompletedTask;
    }
}

public class CompensationIntegrationTests
{
    [Fact]
    public async Task Undo_commands_are_dispatched_in_reverse_order_and_stack_is_persisted()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(CompensationIntegrationTests).Assembly);
        services.AddSagaFlux().AddSaga<TravelSaga, TravelState>();
        await using var provider = services.BuildServiceProvider();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var tripId = Guid.NewGuid();

        foreach (var e in new IEventRequest[] { new TripRequested(tripId), new FlightBooked(tripId), new HotelBooked(tripId), new CarFailed(tripId) })
            await dispatcher.PublishAsync(e);

        var undo = provider.GetRequiredService<UndoLog>().Commands.ToArray();
        Assert.IsType<CancelHotel>(undo[0]);
        Assert.IsType<CancelFlight>(undo[1]);

        var state = (await provider.GetRequiredService<ISagaRepository<TravelState>>().FindAsync(tripId, default))!;
        Assert.Equal(TravelSaga.Cancelled.Name, state.CurrentState);
        Assert.Equal(2, state.Compensations.Count);
        Assert.All(state.Compensations, c => Assert.True(c.IsCompensated));
    }
}
