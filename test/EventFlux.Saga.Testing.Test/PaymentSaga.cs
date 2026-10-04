using EventFlux.Abstractions;

namespace EventFlux.Saga.Testing.Test.Fixtures;

public sealed record PaymentRequested(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record PaymentApproved(Guid CorrelationId) : ICorrelatedEvent;
public sealed record PaymentCaptured(Guid CorrelationId) : ICorrelatedEvent;
public sealed record PaymentCancelled(Guid CorrelationId) : ICorrelatedEvent;

public sealed record ChargeCard(Guid PaymentId, decimal Amount) : IEventRequest;
public sealed record RequestApproval(Guid PaymentId) : IEventRequest;
public sealed record PaymentDeclined(Guid PaymentId, string Reason) : IEventRequest;
public sealed record ReleaseFunds(Guid PaymentId) : IEventRequest;
public sealed record PaymentCaptureExpired(Guid CorrelationId) : ICorrelatedEvent;

public sealed class CardDeclinedException(string reason) : Exception(reason);

public sealed class PaymentState : ICompensableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
    public bool Declined { get; set; }
    public List<CompensationEntry> Compensations { get; set; } = new();
}

/// <summary>Uses DuringActive, Branch, OnFault and RemoveWhenFinal.</summary>
public sealed class PaymentSaga : Saga<PaymentState>
{
    public const decimal ApprovalLimit = 1000m;
    public const string ReleaseFundsKey = "ReleaseFunds";

    public static readonly SagaState AwaitingApproval = SagaStates.Define(nameof(AwaitingApproval));
    public static readonly SagaState Charging = SagaStates.Define(nameof(Charging));
    public static readonly SagaState Failed = SagaStates.Define(nameof(Failed));

    public PaymentSaga()
    {
        RemoveWhenFinal();
        OnCompensate(ReleaseFundsKey, ctx => ctx.Publish(new ReleaseFunds(ctx.State.CorrelationId)));

        During(SagaStates.Initial,
            When<PaymentRequested>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Branch(ctx => ctx.State.Amount > ApprovalLimit,
                    then => then
                        .Publish(ctx => new RequestApproval(ctx.CorrelationId))
                        .TransitionTo(AwaitingApproval),
                    otherwise => otherwise
                        .Publish(ctx => new ChargeCard(ctx.CorrelationId, ctx.State.Amount))
                        .Then(ctx => CardProcessor.Authorize(ctx.State.Amount))
                        .OnCompensate(ReleaseFundsKey)
                        .Schedule(TimeSpan.FromMinutes(5), ctx => new PaymentCaptureExpired(ctx.CorrelationId))
                        .TransitionTo(Charging))
                .OnFault<CardDeclinedException>(fault => fault
                    .Then(ctx => ctx.State.Declined = true)
                    .Publish(ctx => new PaymentDeclined(ctx.CorrelationId, ctx.Fault!.Message))
                    .TransitionTo(Failed)));

        During(AwaitingApproval,
            When<PaymentApproved>()
                .Publish(ctx => new ChargeCard(ctx.CorrelationId, ctx.State.Amount))
                .OnCompensate(ReleaseFundsKey)
                .TransitionTo(Charging));

        During(Charging,
            When<PaymentCaptured>().Unschedule<PaymentCaptureExpired>().Finalize());

        DuringActive(
            When<PaymentCancelled>().Compensate().Finalize());
    }
}

/// <summary>The card processor stand-in: amounts ending in .13 are declined.</summary>
public static class CardProcessor
{
    public static void Authorize(decimal amount)
    {
        if (amount % 1 == 0.13m)
            throw new CardDeclinedException("card declined");
    }
}
