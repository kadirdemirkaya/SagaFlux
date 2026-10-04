using EventFlux.Saga.Test.Fixtures;

namespace EventFlux.Saga.Test;

public class SagaExecutionTests
{
    private readonly OrderSaga _saga = new();
    private readonly Guid _orderId = Guid.NewGuid();

    [Fact]
    public async Task Initial_event_creates_instance_and_transitions()
    {
        var result = await _saga.ExecuteAsync(null, new OrderPlaced(_orderId, 42m));

        Assert.True(result.Handled);
        Assert.True(result.Created);
        Assert.Equal(_orderId, result.State!.CorrelationId);
        Assert.Equal("Initial", result.FromState);
        Assert.Equal(OrderSaga.AwaitingPayment.Name, result.ToState);
        Assert.Equal(42m, result.State.Amount);
        var charge = Assert.IsType<ChargeCustomer>(Assert.Single(result.Published).Event);
        Assert.Equal(42m, charge.Amount);
    }

    [Fact]
    public async Task Guard_that_fails_does_not_create_instance()
    {
        var result = await _saga.ExecuteAsync(null, new OrderPlaced(_orderId, 0m));

        Assert.False(result.Handled);
        Assert.Null(result.State);
        Assert.Empty(result.Published);
    }

    [Fact]
    public async Task Non_initial_event_without_instance_is_ignored()
    {
        var result = await _saga.ExecuteAsync(null, new PaymentReceived(_orderId));

        Assert.False(result.Handled);
        Assert.Null(result.State);
    }

    [Fact]
    public async Task Event_not_allowed_in_current_state_is_ignored()
    {
        var state = new OrderState { CorrelationId = _orderId, CurrentState = OrderSaga.Shipping };

        var result = await _saga.ExecuteAsync(state, new PaymentFailed(_orderId, "late"));

        Assert.False(result.Handled);
        Assert.Equal(OrderSaga.Shipping.Name, state.CurrentState);
    }

    [Fact]
    public async Task Finalize_moves_to_final_and_further_events_are_ignored()
    {
        var state = new OrderState { CorrelationId = _orderId, CurrentState = OrderSaga.Shipping };

        var shipped = await _saga.ExecuteAsync(state, new OrderShipped(_orderId));
        var again = await _saga.ExecuteAsync(state, new OrderShipped(_orderId));

        Assert.Equal(SagaStates.Final.Name, shipped.ToState);
        Assert.False(again.Handled);
    }

    [Fact]
    public void Correlation_uses_selector_or_correlated_event()
    {
        Assert.Equal(_orderId, _saga.GetCorrelationId(new OrderPlaced(_orderId, 1m)));
        Assert.Equal(_orderId, _saga.GetCorrelationId(new PaymentReceived(_orderId)));
        Assert.True(_saga.Handles(typeof(PaymentFailed)));
        Assert.False(_saga.Handles(typeof(ShipOrder)));
    }

    [Fact]
    public void Validate_rejects_uncorrelated_events()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new UncorrelatedSaga().Validate());
        Assert.Contains(nameof(ShipOrder), ex.Message);
    }

    [Fact]
    public void During_final_is_rejected()
    {
        Assert.Throws<ArgumentException>(() => new FinalStateSaga());
    }

    private sealed class UncorrelatedSaga : Saga<OrderState>
    {
        public UncorrelatedSaga() => During(SagaStates.Initial, When<ShipOrder>().Finalize());
    }

    private sealed class FinalStateSaga : Saga<OrderState>
    {
        public FinalStateSaga() => During(SagaStates.Final, When<PaymentReceived>().Finalize());
    }
}
