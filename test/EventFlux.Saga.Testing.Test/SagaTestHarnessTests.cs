using EventFlux.Saga.Testing.Test.Fixtures;

namespace EventFlux.Saga.Testing.Test;

public class SagaTestHarnessTests
{
    private readonly Guid _orderId = Guid.NewGuid();

    [Fact]
    public void Initial_event_creates_instance()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .When(new OrderPlaced(_orderId, 42m))
            .Then();

        result.Should()
              .HaveCreatedInstance()
              .And.TransitionTo(OrderSaga.AwaitingPayment)
              .And.HavePublished<ChargeCustomer>(c => c.OrderId == _orderId && c.Amount == 42m)
              .And.HaveState(s => s.Amount == 42m, "amount copied from the event");
    }

    [Fact]
    public void Payment_failure_cancels_and_notifies_customer()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .Given(OrderSaga.AwaitingPayment, s => s.Amount = 10m)
            .When(new PaymentFailed(_orderId, "card declined"))
            .Then();

        result.Should().TransitionTo(OrderSaga.Cancelled)
              .And.HavePublished<NotifyCustomer>(n => n.Message == "card declined")
              .And.NotHavePublished<ShipOrder>();
        Assert.Equal(_orderId, result.Instance!.CorrelationId);
    }

    [Fact]
    public void Multiple_events_run_in_order()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .When(new OrderPlaced(_orderId, 1m), new PaymentReceived(_orderId), new OrderShipped(_orderId))
            .Then();

        result.Should().BeHandled().And.BeFinal()
              .And.HavePublished<ChargeCustomer>()
              .And.HavePublished<ShipOrder>();
        Assert.Equal(3, result.Steps.Count);
    }

    [Fact]
    public void Ignored_event_keeps_state()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .Given(OrderSaga.Shipping)
            .When(new PaymentReceived(_orderId))
            .Then();

        result.Should().NotBeHandled().And.StayIn(OrderSaga.Shipping).And.NotHavePublishedAnything();
    }

    [Fact]
    public void Guard_prevents_instance_creation()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .When(new OrderPlaced(_orderId, 0m))
            .Then();

        result.Should().NotBeHandled();
        Assert.Null(result.Instance);
        Assert.Throws<SagaAssertionException>(() => result.Should().TransitionTo(OrderSaga.AwaitingPayment));
    }

    [Fact]
    public void Failed_assertions_describe_what_happened()
    {
        var result = SagaTestHarness.For<OrderSaga, OrderState>()
            .When(new OrderPlaced(_orderId, 5m))
            .Then();

        var wrongState = Assert.Throws<SagaAssertionException>(() => result.Should().TransitionTo(OrderSaga.Cancelled));
        Assert.Contains("AwaitingPayment", wrongState.Message);

        var missing = Assert.Throws<SagaAssertionException>(() => result.Should().HavePublished<ShipOrder>());
        Assert.Contains(nameof(ChargeCustomer), missing.Message);
    }

    [Fact]
    public async Task Async_actions_are_awaited()
    {
        var result = await SagaTestHarness.For<AsyncSaga, OrderState>()
            .When(new PaymentReceived(_orderId))
            .ThenAsync();

        result.Should().BeFinal().And.HaveState(s => s.FailureReason == "async ran");
    }

    [Fact]
    public void Then_without_events_throws()
    {
        Assert.Throws<InvalidOperationException>(() => SagaTestHarness.For<OrderSaga, OrderState>().Then());
    }

    public sealed class AsyncSaga : Saga<OrderState>
    {
        public AsyncSaga()
        {
            During(SagaStates.Initial,
                When<PaymentReceived>()
                    .ThenAsync(async ctx =>
                    {
                        await Task.Yield();
                        ctx.State.FailureReason = "async ran";
                    })
                    .Finalize());
        }
    }
}
