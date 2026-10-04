using EventFlux.Abstractions;

namespace EventFlux.Saga.Testing.Test.Fixtures;

public sealed record OrderPaid(Guid CorrelationId) : ICorrelatedEvent;
public sealed record OrderPacked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record CarrierBooked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record LabelPrinted(Guid CorrelationId) : ICorrelatedEvent;
public sealed record CustomsCleared(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipParcel(Guid OrderId, string CompletedBy) : IEventRequest;
public sealed record HandOver(Guid OrderId) : IEventRequest;

public sealed class ShipmentState : IJoinableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public List<string> JoinedEvents { get; set; } = new();
}

/// <summary>Waits for payment and packing (any order), then for three dispatch steps.</summary>
public sealed class ShipmentSaga : Saga<ShipmentState>
{
    public static readonly SagaState ReadyToDispatch = SagaStates.Define(nameof(ReadyToDispatch));

    public ShipmentSaga()
    {
        During(SagaStates.Initial,
            WhenAll<OrderPaid, OrderPacked>()
                .Publish(ctx => new ShipParcel(ctx.CorrelationId, ctx.Event.GetType().Name))
                .TransitionTo(ReadyToDispatch));

        During(ReadyToDispatch,
            WhenAll<CarrierBooked, LabelPrinted, CustomsCleared>()
                .Publish(ctx => new HandOver(ctx.CorrelationId))
                .Finalize());
    }
}
