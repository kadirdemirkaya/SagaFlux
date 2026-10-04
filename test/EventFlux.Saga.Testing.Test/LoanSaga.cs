using EventFlux.Abstractions;

namespace EventFlux.Saga.Testing.Test.Fixtures;

public sealed record LoanApplied(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record CheckCredit(Guid ApplicantId, decimal Amount) : IEventRequest<CreditScore>;
public sealed record CreditScore(int Value) : IEventResponse;
public sealed record LoanApproved(Guid LoanId) : IEventRequest;
public sealed record LoanRejected(Guid LoanId, int Score) : IEventRequest;

public sealed class LoanState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public int Score { get; set; }
}

public sealed class LoanSaga : Saga<LoanState>
{
    public const int MinimumScore = 600;
    public static readonly SagaState Approved = SagaStates.Define(nameof(Approved));
    public static readonly SagaState Rejected = SagaStates.Define(nameof(Rejected));

    public LoanSaga() =>
        During(SagaStates.Initial,
            When<LoanApplied>()
                .Ask(ctx => new CheckCredit(ctx.CorrelationId, ctx.Event.Amount), (ctx, score) => ctx.State.Score = score.Value)
                .Branch(ctx => ctx.State.Score >= MinimumScore,
                    then => then.Publish(ctx => new LoanApproved(ctx.CorrelationId)).TransitionTo(Approved),
                    otherwise => otherwise.Publish(ctx => new LoanRejected(ctx.CorrelationId, ctx.State.Score)).TransitionTo(Rejected)));
}
