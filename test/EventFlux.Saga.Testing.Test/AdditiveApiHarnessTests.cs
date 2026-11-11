using EventFlux.Abstractions;
using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class AdditiveApiHarnessTests
{
    private readonly Guid _id = Guid.NewGuid();

    public sealed record PickingDone(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record GiftWrapped(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record FreightReleased(Guid OrderId) : IEventRequest;

    public sealed class ScoredLoanState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int Score { get; set; }
        public bool CallbackCompleted { get; set; }
    }

    private sealed class AsyncLoanSaga : Saga<ScoredLoanState>
    {
        public static readonly SagaState Scored = SagaStates.Define(nameof(Scored));
        public static readonly SagaState Declined = SagaStates.Define(nameof(Declined));

        public AsyncLoanSaga() =>
            During(SagaStates.Initial,
                When<LoanApplied>()
                    .AskAsync(ctx => new CheckCredit(ctx.CorrelationId, ctx.Event.Amount), async (ctx, score) =>
                    {
                        await Task.Delay(10);
                        ctx.State.Score = score.Value;
                        ctx.State.CallbackCompleted = true;
                    })
                    .Branch(ctx => ctx.State.CallbackCompleted && ctx.State.Score >= LoanSaga.MinimumScore,
                        then => then.Publish(ctx => new LoanApproved(ctx.CorrelationId)).TransitionTo(Scored),
                        otherwise => otherwise.TransitionTo(Declined)));
    }

    private sealed class FourWayShipmentSaga : Saga<ShipmentState>
    {
        public static readonly SagaState Shipped = SagaStates.Define(nameof(Shipped));

        public FourWayShipmentSaga() =>
            During(SagaStates.Initial,
                WhenAll<OrderPaid, OrderPacked, CarrierBooked, LabelPrinted>()
                    .Publish(ctx => new ShipParcel(ctx.CorrelationId, ctx.Event.GetType().Name))
                    .TransitionTo(Shipped));
    }

    private sealed class FiveWayShipmentSaga : Saga<ShipmentState>
    {
        public static readonly SagaState Released = SagaStates.Define(nameof(Released));

        public FiveWayShipmentSaga() =>
            During(SagaStates.Initial,
                WhenAll<OrderPaid, OrderPacked, CarrierBooked, LabelPrinted, CustomsCleared>()
                    .Publish(ctx => new FreightReleased(ctx.CorrelationId))
                    .TransitionTo(Released));
    }

    private sealed class RepeatedFourWaySaga : Saga<ShipmentState>
    {
        public RepeatedFourWaySaga() =>
            During(SagaStates.Initial, WhenAll<OrderPaid, OrderPacked, CarrierBooked, OrderPaid>().Finalize());
    }

    private sealed class RepeatedFiveWaySaga : Saga<ShipmentState>
    {
        public RepeatedFiveWaySaga() =>
            During(SagaStates.Initial, WhenAll<OrderPaid, OrderPacked, CarrierBooked, LabelPrinted, LabelPrinted>().Finalize());
    }

    private sealed class PlainState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    private sealed class NotJoinableFiveWaySaga : Saga<PlainState>
    {
        public NotJoinableFiveWaySaga() =>
            During(SagaStates.Initial, WhenAll<OrderPaid, OrderPacked, CarrierBooked, LabelPrinted, CustomsCleared>().Finalize());
    }

    [Theory]
    [InlineData(720, true)]
    [InlineData(500, false)]
    public void The_async_ask_step_awaits_the_callback_before_the_next_step(int score, bool approved)
    {
        var result = SagaTestHarness.For<AsyncLoanSaga, ScoredLoanState>()
            .Answer<CheckCredit, CreditScore>(_ => new CreditScore(score))
            .When(new LoanApplied(_id, 5000m))
            .Then();

        result.Should().TransitionTo(approved ? AsyncLoanSaga.Scored : AsyncLoanSaga.Declined)
            .And.HaveAsked<CheckCredit>(c => c.ApplicantId == _id && c.Amount == 5000m)
            .And.HaveState(s => s.Score == score && s.CallbackCompleted);
    }

    [Fact]
    public void The_async_ask_step_needs_an_answer_like_the_sync_one()
    {
        var scenario = SagaTestHarness.For<AsyncLoanSaga, ScoredLoanState>().When(new LoanApplied(_id, 10m));

        var ex = Assert.Throws<SagaConfigurationException>(() => scenario.Then());

        Assert.Contains("Answer<CheckCredit, CreditScore>", ex.Message);
    }

    [Fact]
    public void A_four_event_join_waits_for_all_four_in_any_order()
    {
        var waiting = SagaTestHarness.For<FourWayShipmentSaga, ShipmentState>()
            .When(new LabelPrinted(_id), new OrderPaid(_id), new CarrierBooked(_id), new OrderPaid(_id)).Then();
        waiting.Should().StayIn(SagaStates.Initial).And.NotHavePublishedAnything();
        Assert.Equal(3, waiting.Instance!.JoinedEvents.Count);

        var done = SagaTestHarness.For<FourWayShipmentSaga, ShipmentState>()
            .When(new LabelPrinted(_id), new OrderPaid(_id), new CarrierBooked(_id), new OrderPacked(_id)).Then();
        done.Should().TransitionTo(FourWayShipmentSaga.Shipped)
            .And.HavePublished<ShipParcel>(s => s.CompletedBy == nameof(OrderPacked));
        Assert.Single(done.Published);
        Assert.Empty(done.Instance!.JoinedEvents);
    }

    [Fact]
    public void A_five_event_join_waits_for_all_five_in_any_order()
    {
        var waiting = SagaTestHarness.For<FiveWayShipmentSaga, ShipmentState>()
            .When(new CustomsCleared(_id), new LabelPrinted(_id), new OrderPacked(_id), new CarrierBooked(_id)).Then();
        waiting.Should().StayIn(SagaStates.Initial).And.NotHavePublished<FreightReleased>();

        var done = SagaTestHarness.For<FiveWayShipmentSaga, ShipmentState>()
            .When(new CustomsCleared(_id), new LabelPrinted(_id), new OrderPacked(_id), new CarrierBooked(_id), new OrderPaid(_id)).Then();
        done.Should().TransitionTo(FiveWayShipmentSaga.Released).And.HavePublished<FreightReleased>(f => f.OrderId == _id);
        Assert.Single(done.Published);
        Assert.Empty(done.Instance!.JoinedEvents);
    }

    [Fact]
    public void Larger_joins_still_need_distinct_event_types_and_a_joinable_state()
    {
        Assert.Throws<ArgumentException>(() => new RepeatedFourWaySaga());
        Assert.Throws<ArgumentException>(() => new RepeatedFiveWaySaga());
        Assert.Contains(nameof(IJoinableSagaState), Assert.Throws<InvalidOperationException>(() => new NotJoinableFiveWaySaga()).Message);
    }
}
