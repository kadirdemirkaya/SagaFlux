using EventFlux.Abstractions;
using EventFlux.RedisFlow;
using EventFlux.RedisFlow.Redis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace EventFlux.Saga.Integration.Test;

public sealed record RedisOrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record RedisPaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record RedisChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;
public sealed record RedisOrderExploded(Guid CorrelationId) : ICorrelatedEvent;

public sealed class RedisOrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class RedisOrderSaga : Saga<RedisOrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public RedisOrderSaga()
    {
        During(SagaStates.Initial,
            When<RedisOrderPlaced>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Publish(ctx => new RedisChargeCustomer(ctx.CorrelationId, ctx.State.Amount))
                .TransitionTo(AwaitingPayment),
            When<RedisOrderExploded>().Then(_ => throw new InvalidOperationException("boom")));
        During(AwaitingPayment,
            When<RedisPaymentReceived>().Finalize());
    }
}

/// <summary>The "payments service": charges the customer and answers through the stream.</summary>
public sealed class RedisPaymentService(IRedisStreamPublisher publisher) : IEventHandler<RedisChargeCustomer>
{
    public Task Handle(RedisChargeCustomer @event, CancellationToken cancellationToken)
        => publisher.PublishAsync(new RedisPaymentReceived(@event.OrderId), cancellationToken);
}

public class RedisFlowSagaIntegrationTests
{
    private static IHost BuildHost(string stream)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["RedisStream:ConnectionString"] = RedisFactAttribute.ConnectionString,
            ["RedisStream:StreamName"] = stream,
            ["RedisStream:ConsumerGroup"] = $"{stream}-group",
            ["RedisStream:MaxDeliveryAttempts"] = "2",
            ["RedisStream:RetryIdleTime"] = "00:00:01",
        });
        builder.Services.AddRedisEventQueue(builder.Configuration, typeof(RedisFlowSagaIntegrationTests).Assembly);
        builder.Services.AddSagaFlux().AddSaga<RedisOrderSaga, RedisOrderState>().UseRedisFlow();
        return builder.Build();
    }

    private static async Task DeleteStreamsAsync(IHost host, string stream)
    {
        var db = host.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
        await db.KeyDeleteAsync([stream, $"{stream}-dead-letter"]);
    }

    [RedisFact]
    public async Task Saga_runs_to_completion_through_Redis_streams()
    {
        var stream = $"sagaflux-it-{Guid.NewGuid():N}";
        using var host = BuildHost(stream);
        await host.StartAsync();
        try
        {
            var orderId = Guid.NewGuid();
            await host.Services.GetRequiredService<IRedisStreamPublisher>().PublishAsync(new RedisOrderPlaced(orderId, 42m));

            var repository = host.Services.GetRequiredService<ISagaRepository<RedisOrderState>>();
            var state = await Eventually.WaitAsync(
                () => repository.FindAsync(orderId, default),
                s => s.CurrentState == SagaStates.Final.Name);

            Assert.Equal(42m, state.Amount);
            Assert.Equal(2, state.Version);
        }
        finally
        {
            await DeleteStreamsAsync(host, stream);
            await host.StopAsync();
        }
    }

    [RedisFact]
    public async Task A_failing_saga_step_is_retried_then_dead_lettered_not_lost()
    {
        var stream = $"sagaflux-it-{Guid.NewGuid():N}";
        using var host = BuildHost(stream);
        await host.StartAsync();
        try
        {
            var id = Guid.NewGuid();
            await host.Services.GetRequiredService<IRedisStreamPublisher>().PublishAsync(new RedisOrderExploded(id));

            var db = host.Services.GetRequiredService<IConnectionMultiplexer>().GetDatabase();
            var deadLettered = await Eventually.WaitAsync(
                async () => (await db.StreamRangeAsync($"{stream}-dead-letter")) is { Length: > 0 } entries ? entries : null,
                _ => true,
                TimeSpan.FromSeconds(60));

            Assert.Contains(deadLettered.SelectMany(e => e.Values), v => v.Value.ToString().Contains(id.ToString()));
            Assert.Null(await host.Services.GetRequiredService<ISagaRepository<RedisOrderState>>().FindAsync(id, default));
        }
        finally
        {
            await DeleteStreamsAsync(host, stream);
            await host.StopAsync();
        }
    }
}
