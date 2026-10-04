using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class AskHarnessTests
{
    private readonly Guid _id = Guid.NewGuid();

    [Fact]
    public void The_answer_drives_the_branch()
    {
        SagaTestHarness.For<LoanSaga, LoanState>()
            .Answer<CheckCredit, CreditScore>(_ => new CreditScore(720))
            .When(new LoanApplied(_id, 5000m))
            .Then()
            .Should().TransitionTo(LoanSaga.Approved)
            .And.HaveAsked<CheckCredit>(c => c.ApplicantId == _id && c.Amount == 5000m)
            .And.HavePublished<LoanApproved>()
            .And.HaveState(s => s.Score == 720);

        SagaTestHarness.For<LoanSaga, LoanState>()
            .Answer<CheckCredit, CreditScore>(c => new CreditScore(c.Amount > 1000m ? 500 : 650))
            .When(new LoanApplied(_id, 5000m))
            .Then()
            .Should().TransitionTo(LoanSaga.Rejected)
            .And.HavePublished<LoanRejected>(r => r.Score == 500);
    }

    [Fact]
    public void An_unanswered_request_fails_the_scenario_with_a_hint()
    {
        var scenario = SagaTestHarness.For<LoanSaga, LoanState>().When(new LoanApplied(_id, 10m));

        // A setup error, so an OnFault<InvalidOperationException> of the saga cannot swallow it.
        var ex = Assert.Throws<SagaConfigurationException>(() => scenario.Then());

        Assert.Contains("Answer<CheckCredit, CreditScore>", ex.Message);
    }

    [Fact]
    public void NotHaveAsked_and_HaveAsked_report_what_was_asked()
    {
        var result = SagaTestHarness.For<LoanSaga, LoanState>()
            .Answer<CheckCredit, CreditScore>(_ => new CreditScore(700))
            .When(new LoanApplied(_id, 10m))
            .Then();

        Assert.Contains("CheckCredit", Assert.Throws<SagaAssertionException>(() => result.Should().NotHaveAsked<CheckCredit>()).Message);
        Assert.Contains("asked: CheckCredit", Assert.Throws<SagaAssertionException>(() => result.Should().HaveAsked<CheckCredit>(c => c.Amount == 1m)).Message);
    }
}
