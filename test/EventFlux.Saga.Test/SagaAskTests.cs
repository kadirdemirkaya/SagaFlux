using EventFlux.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class SagaAskTests
{
    public sealed record StockRequested(Guid CorrelationId, string Sku) : ICorrelatedEvent;
    public sealed record ReserveStock(string Sku) : IEventRequest<StockReservation>;
    public sealed record StockReservation(bool Reserved, string? Reason) : IEventResponse;

    public sealed class StockState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public string? Failure { get; set; }
    }

    public sealed class StockSaga : Saga<StockState>
    {
        public static readonly SagaState Reserved = SagaStates.Define(nameof(Reserved));
        public static readonly SagaState Unavailable = SagaStates.Define(nameof(Unavailable));
        public static readonly SagaState Broken = SagaStates.Define(nameof(Broken));

        public StockSaga() =>
            During(SagaStates.Initial,
                When<StockRequested>()
                    .Ask(ctx => new ReserveStock(ctx.Event.Sku), (ctx, r) => ctx.State.Failure = r.Reason)
                    .Branch(ctx => ctx.State.Failure is null, then => then.TransitionTo(Reserved), otherwise => otherwise.TransitionTo(Unavailable))
                    .OnFault<InvalidOperationException>(f => f.Then(ctx => ctx.State.Failure = ctx.Fault!.Message).TransitionTo(Broken)));
    }

    /// <summary>The in-process "inventory" component the saga asks.</summary>
    public sealed class ReserveStockHandler : IEventHandler<ReserveStock, StockReservation>
    {
        public Task<StockReservation> Handle(ReserveStock @event, CancellationToken cancellationToken) => @event.Sku switch
        {
            "broken" => throw new InvalidOperationException("inventory offline"),
            "none" => Task.FromResult<StockReservation>(null!),
            "sold-out" => Task.FromResult(new StockReservation(false, "sold out")),
            _ => Task.FromResult(new StockReservation(true, null)),
        };
    }

    private static ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(SagaAskTests).Assembly);
        services.AddSagaFlux().AddSaga<StockSaga, StockState>();
        return services.BuildServiceProvider();
    }

    [Theory]
    [InlineData("SKU-1", "Reserved", null)]
    [InlineData("sold-out", "Unavailable", "sold out")]
    [InlineData("broken", "Broken", "inventory offline")]
    [InlineData("none", "Broken", "The handler of 'ReserveStock' returned no StockReservation.")]
    public async Task The_saga_asks_the_EventFlux_request_handler(string sku, string expectedState, string? failure)
    {
        await using var provider = Build();
        var id = Guid.NewGuid();

        await provider.GetRequiredService<IEventDispatcher>().PublishAsync(new StockRequested(id, sku));

        var state = (await provider.GetRequiredService<ISagaRepository<StockState>>().FindAsync(id, default))!;
        Assert.Equal((expectedState, failure), (state.CurrentState, state.Failure));
    }

    [Fact]
    public async Task ExecuteAsync_without_a_requester_explains_what_is_missing()
    {
        // The saga handles InvalidOperationException with OnFault; a missing requester must not be taken for that.
        var ex = await Assert.ThrowsAsync<SagaConfigurationException>(
            () => new StockSaga().ExecuteAsync(null, new StockRequested(Guid.NewGuid(), "SKU-1"), requester: null));

        Assert.Contains(nameof(ISagaRequester), ex.Message);
    }
}
