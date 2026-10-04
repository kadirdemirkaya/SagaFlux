using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class CompensationHarnessTests
{
    private readonly Guid _tripId = Guid.NewGuid();

    [Fact]
    public void Car_failure_compensates_hotel_then_flight()
    {
        var result = SagaTestHarness.For<TravelSaga, TravelState>()
            .Given(TravelSaga.AwaitingCar)
            .WithCompensations(TravelSaga.CancelFlightKey, TravelSaga.CancelHotelKey)
            .When(new CarFailed(_tripId))
            .Then();

        result.Should().TransitionTo(TravelSaga.Cancelled)
              .And.HaveCompensated(TravelSaga.CancelHotelKey)
              .And.HaveCompensatedInOrder(TravelSaga.CancelHotelKey, TravelSaga.CancelFlightKey)
              .And.HavePublished<CancelHotel>()
              .And.HavePublished<CancelFlight>(c => c.TripId == _tripId);
    }

    [Fact]
    public void Full_happy_path_records_but_never_runs_compensations()
    {
        var result = SagaTestHarness.For<TravelSaga, TravelState>()
            .When(new TripRequested(_tripId), new FlightBooked(_tripId), new HotelBooked(_tripId), new CarBooked(_tripId))
            .Then();

        result.Should().BeFinal()
              .And.NotHaveCompensated(TravelSaga.CancelFlightKey)
              .And.HaveState(s => s.Compensations.Count == 2, "two recorded compensations");
    }

    [Fact]
    public void Cancellation_event_unwinds_only_completed_steps()
    {
        var result = SagaTestHarness.For<TravelSaga, TravelState>()
            .When(new TripRequested(_tripId), new FlightBooked(_tripId), new TripCancelled(_tripId))
            .Then();

        result.Should().HaveCompensatedInOrder(TravelSaga.CancelFlightKey)
              .And.NotHaveCompensated(TravelSaga.CancelHotelKey)
              .And.StayIn(TravelSaga.AwaitingHotel);
    }

    [Fact]
    public void Compensation_assertion_failure_lists_what_ran()
    {
        var result = SagaTestHarness.For<TravelSaga, TravelState>()
            .Given(TravelSaga.AwaitingCar)
            .WithCompensations(TravelSaga.CancelFlightKey)
            .When(new CarFailed(_tripId))
            .Then();

        var ex = Assert.Throws<SagaAssertionException>(() => result.Should().HaveCompensated(TravelSaga.CancelHotelKey));
        Assert.Contains("[CancelFlight]", ex.Message);
    }

    [Fact]
    public void WithCompensations_requires_given_instance()
    {
        Assert.Throws<InvalidOperationException>(() =>
            SagaTestHarness.For<TravelSaga, TravelState>().WithCompensations("x"));
    }
}
