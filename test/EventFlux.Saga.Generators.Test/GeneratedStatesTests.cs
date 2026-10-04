using EventFlux.Abstractions;

namespace EventFlux.Saga.Generators.Test;

[SagaStateSet]
public enum ShipmentStates
{
    Initial,
    Packed,
    InTransit,
    Delivered,
    Final
}

public sealed record ShipmentPacked(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipmentDispatched(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipmentDelivered(Guid CorrelationId) : ICorrelatedEvent;
public sealed record NotifyRecipient(Guid ShipmentId) : IEventRequest;

public sealed class ShipmentState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
}

public sealed class ShipmentSaga : Saga<ShipmentState>
{
    public ShipmentSaga()
    {
        During(ShipmentStatesSet.Initial, When<ShipmentPacked>().TransitionTo(ShipmentStatesSet.Packed));
        During(ShipmentStatesSet.Packed, When<ShipmentDispatched>().TransitionTo(ShipmentStatesSet.InTransit));
        During(ShipmentStatesSet.InTransit,
            When<ShipmentDelivered>().Publish(ctx => new NotifyRecipient(ctx.CorrelationId)).TransitionTo(ShipmentStatesSet.Delivered));
    }
}

public class GeneratedStatesTests
{
    [Fact]
    public void Generated_states_mirror_the_enum()
    {
        Assert.Equal("Packed", ShipmentStatesSet.Packed.Name);
        Assert.Equal((int)ShipmentStates.InTransit, ShipmentStatesSet.InTransit.Ordinal);
        Assert.Equal(SagaStates.Initial, ShipmentStatesSet.Initial);
        Assert.Equal(SagaStates.Final, ShipmentStatesSet.Final);
        Assert.Equal(5, ShipmentStatesSet.All.Count);
        Assert.Equal(ShipmentStatesSet.Delivered, ShipmentStatesSet.From(ShipmentStates.Delivered));
    }

    [Fact]
    public void TryParse_resolves_persisted_names_only()
    {
        Assert.True(ShipmentStatesSet.TryParse("InTransit", out var state));
        Assert.Equal(ShipmentStatesSet.InTransit, state);
        Assert.False(ShipmentStatesSet.TryParse("intransit", out _));
        Assert.False(ShipmentStatesSet.TryParse(null, out _));
    }

    [Fact]
    public void From_rejects_undeclared_values()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => ShipmentStatesSet.From((ShipmentStates)42));
    }

    [Fact]
    public async Task Saga_built_on_generated_states_runs()
    {
        var saga = new ShipmentSaga();
        var id = Guid.NewGuid();
        ShipmentState? state = null;

        foreach (var e in new object[] { new ShipmentPacked(id), new ShipmentDispatched(id), new ShipmentDelivered(id) })
            state = (await saga.ExecuteAsync(state, e)).State;

        Assert.Equal(ShipmentStatesSet.Delivered.Name, state!.CurrentState);
    }
}
