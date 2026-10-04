using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class JoinHarnessTests
{
    private readonly Guid _id = Guid.NewGuid();

    private static SagaScenario<ShipmentState> Shipment() => SagaTestHarness.For<ShipmentSaga, ShipmentState>();

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void The_join_completes_when_both_events_arrived_in_any_order(bool paidFirst)
    {
        object paid = new OrderPaid(_id), packed = new OrderPacked(_id);

        var result = Shipment().When(paidFirst ? [paid, packed] : [packed, paid]).Then();

        result.Should().TransitionTo(ShipmentSaga.ReadyToDispatch)
            .And.HavePublished<ShipParcel>(s => s.CompletedBy == (paidFirst ? nameof(OrderPacked) : nameof(OrderPaid)));
        Assert.Single(result.Published);
        Assert.Empty(result.Instance!.JoinedEvents);
    }

    [Fact]
    public void One_event_is_recorded_and_the_instance_waits()
    {
        var result = Shipment().When(new OrderPaid(_id), new OrderPaid(_id)).Then();

        result.Should().StayIn(SagaStates.Initial)
            .And.BeHandled()
            .And.HaveCreatedInstance()
            .And.NotHavePublishedAnything();
        Assert.Equal(new[] { "OrderPaid+OrderPacked/OrderPaid" }, result.Instance!.JoinedEvents);
    }

    [Fact]
    public void A_three_event_join_waits_for_all_three()
    {
        var waiting = Shipment().Given(ShipmentSaga.ReadyToDispatch)
            .When(new LabelPrinted(_id), new CarrierBooked(_id)).Then();
        waiting.Should().StayIn(ShipmentSaga.ReadyToDispatch).And.NotHavePublished<HandOver>();

        Shipment().Given(ShipmentSaga.ReadyToDispatch)
            .When(new LabelPrinted(_id), new CustomsCleared(_id), new CarrierBooked(_id)).Then()
            .Should().BeFinal().And.HavePublished<HandOver>();
    }

    private sealed class PlainState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    private sealed class NotJoinableSaga : Saga<PlainState>
    {
        public NotJoinableSaga() => During(SagaStates.Initial, WhenAll<OrderPaid, OrderPacked>().Finalize());
    }

    [Fact]
    public void WhenAll_requires_a_joinable_state()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new NotJoinableSaga());
        Assert.Contains(nameof(IJoinableSagaState), ex.Message);
    }
}
