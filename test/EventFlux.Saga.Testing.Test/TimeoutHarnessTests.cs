using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class TimeoutHarnessTests
{
    private readonly Guid _id = Guid.NewGuid();

    [Fact]
    public void Request_schedules_expiry()
    {
        SagaTestHarness.For<ReservationSaga, ReservationState>()
            .When(new ReservationRequested(_id))
            .Then()
            .Should().TransitionTo(ReservationSaga.AwaitingPayment)
            .And.HaveScheduled<ReservationExpired>(ReservationSaga.HoldFor, e => e.CorrelationId == _id);
    }

    [Fact]
    public void Payment_unschedules_expiry()
    {
        SagaTestHarness.For<ReservationSaga, ReservationState>()
            .Given(ReservationSaga.AwaitingPayment)
            .When(new ReservationPaid(_id))
            .Then()
            .Should().TransitionTo(ReservationSaga.Confirmed)
            .And.HaveUnscheduled<ReservationExpired>()
            .And.NotHaveScheduled<ReservationExpired>();
    }

    [Fact]
    public void Timeout_event_is_tested_like_any_other_event()
    {
        SagaTestHarness.For<ReservationSaga, ReservationState>()
            .Given(ReservationSaga.AwaitingPayment)
            .When(new ReservationExpired(_id))
            .Then()
            .Should().TransitionTo(ReservationSaga.Expired)
            .And.HavePublished<ReleaseSeat>(r => r.ReservationId == _id);
    }

    [Fact]
    public void Wrong_delay_fails_with_what_was_scheduled()
    {
        var result = SagaTestHarness.For<ReservationSaga, ReservationState>()
            .When(new ReservationRequested(_id))
            .Then();

        var ex = Assert.Throws<SagaAssertionException>(() => result.Should().HaveScheduled<ReservationExpired>(TimeSpan.FromMinutes(1)));
        Assert.Contains("ReservationExpired after 00:15:00", ex.Message);
        Assert.Throws<SagaAssertionException>(() => result.Should().HaveUnscheduled<ReservationExpired>());
    }
}
