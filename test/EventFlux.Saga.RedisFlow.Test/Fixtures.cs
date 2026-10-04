using System.Collections.Concurrent;
using System.Reflection;
using EventFlux.Abstractions;
using EventFlux.RedisFlow.Abstractions;
using EventFlux.RedisFlow.Redis;
using StackExchange.Redis;

namespace EventFlux.Saga.RedisFlow.Test;

public sealed record OrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public OrderSaga()
    {
        During(SagaStates.Initial,
            When<OrderPlaced>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Publish(ctx => new ChargeCustomer(ctx.CorrelationId, ctx.State.Amount))
                .TransitionTo(AwaitingPayment));
        During(AwaitingPayment,
            When<PaymentReceived>().Finalize());
    }
}

public sealed class FakePublisher : IRedisStreamPublisher
{
    public ConcurrentQueue<(IEventRequest Event, CancellationToken Token)> Published { get; } = new();

    public Task PublishAsync(string eventType, string payload) => throw new NotSupportedException();

    public Task PublishAsync(string eventType, string payload, EventContext context) => throw new NotSupportedException();

    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct = default) where TEvent : IEventRequest
    {
        Published.Enqueue((@event, ct));
        return Task.CompletedTask;
    }
}

/// <summary>
/// In-memory stand-in for the parts of StackExchange.Redis the RedisFlow publisher and worker touch in these tests:
/// <c>GetDatabase</c> and <c>StreamAddAsync</c>. Every appended entry is kept so it can be fed to the real worker.
/// </summary>
public class FakeRedis : DispatchProxy
{
    public ConcurrentQueue<StreamEntry> Entries { get; private set; } = new();
    private IDatabase? _database;
    private long _nextId;

    public static (IConnectionMultiplexer Multiplexer, FakeRedis Redis) Create()
    {
        var multiplexer = Create<IConnectionMultiplexer, FakeRedis>();
        var redis = (FakeRedis)(object)multiplexer;
        var database = Create<IDatabase, FakeRedis>();
        ((FakeRedis)(object)database).Entries = redis.Entries;
        ((FakeRedis)(object)database)._nextId = 0;
        redis._database = database;
        ((FakeRedis)(object)database)._database = database;
        return (multiplexer, redis);
    }

    protected override object? Invoke(MethodInfo? method, object?[]? args)
    {
        switch (method!.Name)
        {
            case nameof(IConnectionMultiplexer.GetDatabase):
                return _database;
            case nameof(IDatabaseAsync.StreamAddAsync) when args![1] is NameValueEntry[] values:
                var id = (RedisValue)$"{Interlocked.Increment(ref _nextId)}-0";
                Entries.Enqueue(new StreamEntry(id, values));
                return Task.FromResult(id);
            default:
                throw new NotSupportedException($"FakeRedis does not implement {method.Name}.");
        }
    }
}
