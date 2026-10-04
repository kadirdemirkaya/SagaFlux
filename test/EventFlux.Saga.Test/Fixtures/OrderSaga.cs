using EventFlux.Abstractions;

namespace EventFlux.Saga.Test.Fixtures;

public sealed record OrderPlaced(Guid OrderId, decimal Amount) : IEventRequest;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record PaymentFailed(Guid CorrelationId, string Reason) : ICorrelatedEvent;
public sealed record OrderShipped(Guid CorrelationId) : ICorrelatedEvent;

public sealed record ChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;
public sealed record ShipOrder(Guid OrderId) : IEventRequest;
public sealed record NotifyCustomer(Guid OrderId, string Message) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
    public string? FailureReason { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));
    public static readonly SagaState Shipping = SagaStates.Define(nameof(Shipping));
    public static readonly SagaState Cancelled = SagaStates.Define(nameof(Cancelled));

    public OrderSaga()
    {
        CorrelateBy<OrderPlaced>(e => e.OrderId);

        During(SagaStates.Initial,
            When<OrderPlaced>(e => e.Amount > 0)
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Publish(ctx => new ChargeCustomer(ctx.CorrelationId, ctx.State.Amount))
                .TransitionTo(AwaitingPayment));

        During(AwaitingPayment,
            When<PaymentReceived>()
                .Publish(ctx => new ShipOrder(ctx.CorrelationId))
                .TransitionTo(Shipping),
            When<PaymentFailed>()
                .Then(ctx => ctx.State.FailureReason = ctx.Event.Reason)
                .Publish(ctx => new NotifyCustomer(ctx.CorrelationId, ctx.Event.Reason))
                .TransitionTo(Cancelled));

        During(Shipping,
            When<OrderShipped>().Finalize());
    }
}
