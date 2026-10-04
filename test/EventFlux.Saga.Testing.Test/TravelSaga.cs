using EventFlux.Abstractions;
using EventFlux.Saga;

namespace EventFlux.Saga.Testing.Test.Fixtures;

public sealed record TripRequested(Guid CorrelationId) : ICorrelatedEvent;
public sealed record FlightBooked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record HotelBooked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record CarBooked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record CarFailed(Guid CorrelationId) : ICorrelatedEvent;
[CompensatingEvent]
public sealed record TripCancelled(Guid CorrelationId) : ICorrelatedEvent;

public sealed record BookFlight(Guid TripId) : IEventRequest;
public sealed record BookHotel(Guid TripId) : IEventRequest;
public sealed record BookCar(Guid TripId) : IEventRequest;
public sealed record CancelFlight(Guid TripId) : IEventRequest;
public sealed record CancelHotel(Guid TripId) : IEventRequest;

public sealed class TravelState : ICompensableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public List<CompensationEntry> Compensations { get; set; } = new();
}

public sealed class TravelSaga : Saga<TravelState>
{
    public const string CancelFlightKey = "CancelFlight";
    public const string CancelHotelKey = "CancelHotel";

    public static readonly SagaState AwaitingFlight = SagaStates.Define(nameof(AwaitingFlight));
    public static readonly SagaState AwaitingHotel = SagaStates.Define(nameof(AwaitingHotel));
    public static readonly SagaState AwaitingCar = SagaStates.Define(nameof(AwaitingCar));
    public static readonly SagaState Cancelled = SagaStates.Define(nameof(Cancelled));

    public TravelSaga()
    {
        OnCompensate(CancelFlightKey, ctx => ctx.Publish(new CancelFlight(ctx.CorrelationId)));

        During(SagaStates.Initial,
            When<TripRequested>().Publish(ctx => new BookFlight(ctx.CorrelationId)).TransitionTo(AwaitingFlight));

        During(AwaitingFlight,
            When<FlightBooked>()
                .OnCompensate(CancelFlightKey)
                .Publish(ctx => new BookHotel(ctx.CorrelationId))
                .TransitionTo(AwaitingHotel));

        During(AwaitingHotel,
            When<HotelBooked>()
                .OnCompensate(CancelHotelKey, ctx => ctx.Publish(new CancelHotel(ctx.CorrelationId)))
                .Publish(ctx => new BookCar(ctx.CorrelationId))
                .TransitionTo(AwaitingCar));

        During(AwaitingCar,
            When<CarBooked>().Finalize(),
            When<CarFailed>().Compensate().TransitionTo(Cancelled));
    }
}
