using EventFlux.Saga.Test.Fixtures;

namespace EventFlux.Saga.Test;

public class CompensationTests
{
    private readonly TravelSaga _saga = new();
    private readonly Guid _tripId = Guid.NewGuid();

    private async Task<TravelState> Run(params object[] events)
    {
        TravelState? state = null;
        foreach (var e in events)
            state = (await _saga.ExecuteAsync(state, e)).State;
        return state!;
    }

    [Fact]
    public async Task Transitions_append_entries_to_the_stack()
    {
        var state = await Run(new TripRequested(_tripId), new FlightBooked(_tripId), new HotelBooked(_tripId));

        Assert.Equal(new[] { "CancelFlight", "CancelHotel" }, state.Compensations.Select(c => c.CompensationActionKey));
        Assert.Equal(new[] { "AwaitingHotel", "AwaitingCar" }, state.Compensations.Select(c => c.StateName));
        Assert.All(state.Compensations, c => Assert.False(c.IsCompensated));
    }

    [Fact]
    public async Task Compensate_runs_pending_entries_in_reverse_and_marks_them()
    {
        var state = await Run(new TripRequested(_tripId), new FlightBooked(_tripId), new HotelBooked(_tripId));

        var result = await _saga.ExecuteAsync(state, new CarFailed(_tripId));

        Assert.Equal(TravelSaga.Cancelled.Name, result.ToState);
        Assert.Equal(new[] { "CancelHotel", "CancelFlight" }, result.Compensated.Select(c => c.CompensationActionKey));
        Assert.Equal(new[] { typeof(CancelHotel), typeof(CancelFlight) }, result.Published.Select(p => p.Event.GetType()));
        Assert.Equal(2, state.Compensations.Count);
        Assert.All(state.Compensations, c => Assert.True(c.IsCompensated));
    }

    [Fact]
    public async Task Compensating_event_unwinds_without_a_transition()
    {
        var state = await Run(new TripRequested(_tripId), new FlightBooked(_tripId));

        var result = await _saga.ExecuteAsync(state, new TripCancelled(_tripId));
        var again = await _saga.ExecuteAsync(state, new TripCancelled(_tripId));

        Assert.True(result.Handled);
        Assert.Equal(TravelSaga.AwaitingHotel.Name, result.ToState);
        Assert.Equal("CancelFlight", Assert.Single(result.Compensated).CompensationActionKey);
        Assert.True(again.Handled);
        Assert.Empty(again.Compensated);
        Assert.True(_saga.Handles(typeof(TripCancelled)));
    }

    [Fact]
    public async Task Compensating_event_does_not_create_an_instance()
    {
        var result = await _saga.ExecuteAsync(null, new TripCancelled(_tripId));

        Assert.False(result.Handled);
        Assert.Null(result.State);
    }

    [Fact]
    public async Task Unknown_persisted_key_throws()
    {
        var state = new TravelState
        {
            CorrelationId = _tripId,
            CurrentState = TravelSaga.AwaitingCar,
            Compensations = { new CompensationEntry("AwaitingCar", "RenamedKey", DateTimeOffset.UtcNow) }
        };

        var ex = await Assert.ThrowsAsync<SagaCompensationException>(() => _saga.ExecuteAsync(state, new CarFailed(_tripId)));
        Assert.Equal("RenamedKey", ex.Key);
    }

    [Fact]
    public void Recording_an_unregistered_key_fails_validation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new UnregisteredKeySaga().Validate());
        Assert.Contains("Missing", ex.Message);
    }

    [Fact]
    public void Duplicate_key_registration_throws()
    {
        Assert.Throws<InvalidOperationException>(() => new DuplicateKeySaga());
    }

    [Fact]
    public void Compensation_requires_compensable_state()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new NonCompensableSaga());
        Assert.Contains(nameof(ICompensableSagaState), ex.Message);
    }

    private sealed class UnregisteredKeySaga : Saga<TravelState>
    {
        public UnregisteredKeySaga() => During(SagaStates.Initial, When<TripRequested>().OnCompensate("Missing"));
    }

    private sealed class DuplicateKeySaga : Saga<TravelState>
    {
        public DuplicateKeySaga()
        {
            OnCompensate("A", _ => { });
            OnCompensate("A", _ => { });
        }
    }

    private sealed class NonCompensableSaga : Saga<OrderState>
    {
        public NonCompensableSaga() => OnCompensate("A", _ => { });
    }
}
