using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class PaymentHarnessTests
{
    private readonly Guid _id = Guid.NewGuid();

    private static SagaScenario<PaymentState> Payment() => SagaTestHarness.For<PaymentSaga, PaymentState>();

    [Fact]
    public void Branch_takes_the_approval_path_above_the_limit()
    {
        Payment().When(new PaymentRequested(_id, 5000m)).Then()
            .Should().TransitionTo(PaymentSaga.AwaitingApproval)
            .And.HavePublished<RequestApproval>()
            .And.NotHavePublished<ChargeCard>()
            .And.NotHaveScheduled<PaymentCaptureExpired>();
    }

    [Fact]
    public void Branch_takes_the_otherwise_path_below_the_limit()
    {
        var result = Payment().When(new PaymentRequested(_id, 50m)).Then();

        result.Should().TransitionTo(PaymentSaga.Charging)
            .And.HavePublished<ChargeCard>(c => c.Amount == 50m)
            .And.NotHavePublished<RequestApproval>()
            .And.HaveScheduled<PaymentCaptureExpired>(TimeSpan.FromMinutes(5))
            .And.NotHaveHandledFault();
        Assert.Equal(PaymentSaga.ReleaseFundsKey, Assert.Single(result.Instance!.Compensations).CompensationActionKey);
    }

    [Fact]
    public void OnFault_discards_the_failed_steps_and_runs_the_handler()
    {
        var result = Payment().When(new PaymentRequested(_id, 20.13m)).Then();

        result.Should().TransitionTo(PaymentSaga.Failed)
            .And.HaveHandledFault<CardDeclinedException>(e => e.Message == "card declined")
            .And.HavePublished<PaymentDeclined>(f => f.Reason == "card declined")
            // ChargeCard was published before the failure: it is discarded with the failed branch.
            .And.NotHavePublished<ChargeCard>()
            .And.NotHaveScheduled<PaymentCaptureExpired>()
            .And.HaveState(s => s.Declined, "the handler marked the payment declined");
        Assert.Empty(result.Instance!.Compensations);
    }

    [Fact]
    public void DuringActive_applies_in_every_started_state()
    {
        Payment().Given(PaymentSaga.AwaitingApproval).When(new PaymentCancelled(_id)).Then()
            .Should().BeFinal().And.BeRemoved();

        Payment().Given(PaymentSaga.Charging).WithCompensations(PaymentSaga.ReleaseFundsKey)
            .When(new PaymentCancelled(_id)).Then()
            .Should().BeFinal()
            .And.HaveCompensated(PaymentSaga.ReleaseFundsKey)
            .And.HavePublished<ReleaseFunds>();
    }

    [Fact]
    public void DuringActive_never_starts_an_instance()
    {
        var result = Payment().When(new PaymentCancelled(_id)).Then();

        result.Should().NotBeHandled();
        Assert.Null(result.Instance);
        Assert.False(result.Removed);
    }

    [Fact]
    public void RemoveWhenFinal_removes_the_instance_and_a_later_request_starts_a_new_one()
    {
        var removed = Payment().When(new PaymentRequested(_id, 10m), new PaymentCaptured(_id)).Then();
        removed.Should().BeFinal().And.BeRemoved().And.HaveUnscheduled<PaymentCaptureExpired>();
        Assert.Null(removed.Instance);

        var restarted = Payment().When(new PaymentRequested(_id, 10m), new PaymentCaptured(_id), new PaymentRequested(_id, 7m)).Then();
        restarted.Should().TransitionTo(PaymentSaga.Charging).And.HaveState(s => s.Amount == 7m);
        Assert.False(restarted.Removed);
    }

    [Fact]
    public void Assertions_report_what_happened_when_they_fail()
    {
        var result = Payment().When(new PaymentRequested(_id, 50m)).Then();

        Assert.Contains("none", Assert.Throws<SagaAssertionException>(() => result.Should().HaveHandledFault<CardDeclinedException>()).Message);
        Assert.Contains("kept", Assert.Throws<SagaAssertionException>(() => result.Should().BeRemoved()).Message);
    }
}
