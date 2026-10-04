using System.Reflection;
using EventFlux.RedisFlow;
using EventFlux.RedisFlow.Redis;
using EventFlux.RedisFlow.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace EventFlux.Saga.RedisFlow.Test;

/// <summary>
/// Runs the real EventFlux.RedisFlow 1.1.0 publisher and worker against EventFlux 2.x and SagaFlux, with an in-memory
/// stand-in for Redis. The worker's per-entry methods are internal, so they are reached through reflection.
/// </summary>
public class RedisFlowCompatibilityTests
{
    public sealed record OrderExploded(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class ExplodingState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public sealed class ExplodingSaga : Saga<ExplodingState>
    {
        public ExplodingSaga() =>
            During(SagaStates.Initial,
                When<OrderExploded>().Then(_ => throw new InvalidOperationException("boom")));
    }

    private static IConfiguration Configuration() => new ConfigurationBuilder()
        .AddInMemoryCollection(new Dictionary<string, string?> { ["RedisStream:ConnectionString"] = "localhost:6379" })
        .Build();

    private static ServiceCollection Services(IConnectionMultiplexer? multiplexer = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddRedisEventQueue(Configuration(), typeof(RedisFlowCompatibilityTests).Assembly);
        if (multiplexer is not null)
            services.AddSingleton(multiplexer);
        services.AddSagaFlux()
            .AddSaga<OrderSaga, OrderState>()
            .AddSaga<ExplodingSaga, ExplodingState>()
            .UseRedisFlow();
        return services;
    }

    private static RedisStreamWorker Worker(IServiceProvider provider) => new(
        provider.GetRequiredService<IConnectionMultiplexer>(),
        provider.GetRequiredService<IOptions<RedisStreamOptions>>(),
        provider,
        provider.GetRequiredService<ILogger<RedisStreamWorker>>());

    private static Task DispatchAsync(RedisStreamWorker worker, StreamEntry entry) =>
        (Task)typeof(RedisStreamWorker).GetMethod("DispatchAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(worker, [entry, CancellationToken.None])!;

    private static Task<bool> ProcessEntryAsync(RedisStreamWorker worker, StreamEntry entry) =>
        (Task<bool>)typeof(RedisStreamWorker).GetMethod("ProcessEntryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(worker, [entry, CancellationToken.None])!;

    private static async Task StartEventRegistrationAsync(IServiceProvider provider)
    {
        foreach (var hosted in provider.GetServices<IHostedService>().OfType<RedisFlowSagaEventRegistration>())
            await hosted.StartAsync(default);
    }

    [Fact]
    public void RedisFlow_registration_runs_next_to_EventFlux_2_and_SagaFlux()
    {
        var services = Services();

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });

        Assert.IsType<RedisFlowSagaTransport>(provider.GetRequiredService<ISagaTransport>());
        Assert.IsType<RedisFlowSagaEventRegistration>(provider.GetServices<IHostedService>().First());
    }

    [Fact]
    public async Task Saga_events_written_by_the_RedisFlow_publisher_are_read_back_by_the_worker_and_drive_the_saga()
    {
        var (multiplexer, redis) = FakeRedis.Create();
        await using var provider = Services(multiplexer).BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await StartEventRegistrationAsync(provider);
        var publisher = provider.GetRequiredService<IRedisStreamPublisher>();
        var worker = Worker(provider);
        var repository = provider.GetRequiredService<ISagaRepository<OrderState>>();
        var orderId = Guid.NewGuid();

        // Another service starts the saga through the stream.
        await publisher.PublishAsync(new OrderPlaced(orderId, 15m));
        Assert.True(redis.Entries.TryDequeue(out var placed));
        await DispatchAsync(worker, placed);

        var state = (await repository.FindAsync(orderId, default))!;
        Assert.Equal(OrderSaga.AwaitingPayment.Name, state.CurrentState);
        Assert.Equal(15m, state.Amount);

        // The command the saga published went to the same stream in RedisFlow's own format.
        Assert.True(redis.Entries.TryDequeue(out var charge));
        Assert.Equal(nameof(ChargeCustomer), charge.Values.Single(v => v.Name == "type").Value.ToString());
        Assert.Contains(orderId.ToString(), charge.Values.Single(v => v.Name == "data").Value.ToString());

        await publisher.PublishAsync(new PaymentReceived(orderId));
        Assert.True(redis.Entries.TryDequeue(out var paid));
        await DispatchAsync(worker, paid);

        Assert.Equal(SagaStates.Final.Name, (await repository.FindAsync(orderId, default))!.CurrentState);
    }

    [Fact]
    public async Task A_failing_saga_step_leaves_the_entry_pending_for_retry()
    {
        var (multiplexer, redis) = FakeRedis.Create();
        await using var provider = Services(multiplexer).BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        await StartEventRegistrationAsync(provider);
        var id = Guid.NewGuid();

        await provider.GetRequiredService<IRedisStreamPublisher>().PublishAsync(new OrderExploded(id));
        Assert.True(redis.Entries.TryDequeue(out var entry));

        // false = not acknowledged; the FakeRedis would throw if the worker tried to acknowledge the entry.
        Assert.False(await ProcessEntryAsync(Worker(provider), entry));
        Assert.Null(await provider.GetRequiredService<ISagaRepository<ExplodingState>>().FindAsync(id, default));
    }
}
