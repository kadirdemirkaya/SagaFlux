using EventFlux.Abstractions;
using EventFlux.Saga.Concurrency;
using EventFlux.Saga.RedisFlow;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace EventFlux.Saga.Integration.Test;

public sealed record CounterStarted(Guid CorrelationId) : ICorrelatedEvent;
public sealed record CounterIncremented(Guid CorrelationId) : ICorrelatedEvent;

public sealed class CounterState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public int Count { get; set; }
}

public sealed class CounterSaga : Saga<CounterState>
{
    public static readonly SagaState Counting = SagaStates.Define(nameof(Counting));

    public CounterSaga()
    {
        During(SagaStates.Initial, When<CounterStarted>().TransitionTo(Counting));
        During(Counting, When<CounterIncremented>().ThenAsync(async ctx =>
        {
            await Task.Delay(5); // widen the read-modify-write window
            ctx.State.Count++;
        }));
    }
}

public class RedisSagaLockIntegrationTests
{
    [RedisFact]
    public async Task The_Redis_lock_serializes_one_saga_instance()
    {
        using var redis = await ConnectionMultiplexer.ConnectAsync(RedisFactAttribute.ConnectionString!);
        var options = new RedisSagaLockOptions { KeyPrefix = $"sagaflux-it:{Guid.NewGuid():N}:", AcquireTimeout = TimeSpan.FromMilliseconds(300) };
        var nodeA = new RedisSagaLockProvider(redis, options);
        var nodeB = new RedisSagaLockProvider(redis, options);
        var id = Guid.NewGuid();

        var held = await nodeA.AcquireAsync("OrderSaga", id, default);

        await Assert.ThrowsAsync<TimeoutException>(async () => await nodeB.AcquireAsync("OrderSaga", id, default));
        await using ((await nodeB.AcquireAsync("OrderSaga", Guid.NewGuid(), default)).ConfigureAwait(false)) { } // other instances are free

        await held.DisposeAsync();
        await using ((await nodeB.AcquireAsync("OrderSaga", id, default)).ConfigureAwait(false)) { }
    }

    [RedisFact]
    public async Task Concurrent_events_for_one_instance_do_not_exhaust_concurrency_retries_under_the_lock()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IConnectionMultiplexer>(await ConnectionMultiplexer.ConnectAsync(RedisFactAttribute.ConnectionString!));
        services.AddEventBus(typeof(RedisSagaLockIntegrationTests).Assembly);
        services.AddSagaFlux(o => o.MaxConcurrencyRetries = 1)
            .AddSaga<CounterSaga, CounterState>()
            .UseRedisDistributedLock(o => o.KeyPrefix = $"sagaflux-it:{Guid.NewGuid():N}:");
        await using var provider = services.BuildServiceProvider();
        Assert.IsType<RedisSagaLockProvider>(provider.GetRequiredService<ISagaLockProvider>());
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();
        await dispatcher.PublishAsync(new CounterStarted(id));

        await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => Task.Run(() => dispatcher.PublishAsync(new CounterIncremented(id)))));

        var state = (await provider.GetRequiredService<ISagaRepository<CounterState>>().FindAsync(id, default))!;
        Assert.Equal(20, state.Count);
        Assert.Equal(21, state.Version);
    }
}
