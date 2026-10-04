using EventFlux.Abstractions;

namespace EventFlux.Saga.Test.Fixtures;

public sealed record ReservationRequested(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ReservationExtended(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ReservationPaid(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ReservationClosed(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ReservationExpired(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ReleaseSeat(Guid ReservationId) : IEventRequest;

public sealed class ReservationState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public int ExpiryAttempts { get; set; }
}

public sealed class ReservationSaga : Saga<ReservationState>
{
    public static readonly TimeSpan HoldFor = TimeSpan.FromMinutes(15);
    public static readonly TimeSpan ExtendBy = TimeSpan.FromMinutes(30);
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));
    public static readonly SagaState Confirmed = SagaStates.Define(nameof(Confirmed));
    public static readonly SagaState Expired = SagaStates.Define(nameof(Expired));

    public static int FailExpiryTimes { get; set; }

    public ReservationSaga()
    {
        During(SagaStates.Initial,
            When<ReservationRequested>()
                .Schedule(HoldFor, ctx => new ReservationExpired(ctx.CorrelationId))
                .TransitionTo(AwaitingPayment));

        During(AwaitingPayment,
            When<ReservationExtended>()
                .Schedule(ExtendBy, ctx => new ReservationExpired(ctx.CorrelationId)),
            When<ReservationPaid>()
                .Unschedule<ReservationExpired>()
                .TransitionTo(Confirmed),
            When<ReservationClosed>()
                .Finalize(),
            When<ReservationExpired>()
                .Then(ctx =>
                {
                    if (FailExpiryTimes > 0)
                    {
                        FailExpiryTimes--;
                        throw new InvalidOperationException("expiry failed");
                    }
                })
                .Publish(ctx => new ReleaseSeat(ctx.CorrelationId))
                .TransitionTo(Expired));
    }
}
