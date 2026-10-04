using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Concurrency;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class SagaLockTests
{
    public sealed record Ping(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record Pong(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class PingState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    /// <summary>Reacts to the event it publishes itself, on the same instance.</summary>
    public sealed class PingSaga : Saga<PingState>
    {
        public static readonly SagaState WaitingForPong = SagaStates.Define(nameof(WaitingForPong));

        public PingSaga()
        {
            During(SagaStates.Initial,
                When<Ping>().Publish(ctx => new Pong(ctx.CorrelationId)).TransitionTo(WaitingForPong));
            During(WaitingForPong,
                When<Pong>().Finalize());
        }
    }

    /// <summary>A non-reentrant per-instance lock that fails instead of hanging when it would deadlock.</summary>
    public sealed class RecordingLockProvider : ISagaLockProvider
    {
        private readonly ConcurrentDictionary<Guid, SemaphoreSlim> _locks = new();

        public ConcurrentQueue<string> Log { get; } = new();

        public bool IsHeld(Guid id) => _locks.TryGetValue(id, out var gate) && gate.CurrentCount == 0;

        public async ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct)
        {
            var gate = _locks.GetOrAdd(correlationId, _ => new SemaphoreSlim(1, 1));
            if (!await gate.WaitAsync(TimeSpan.FromSeconds(5), ct))
                throw new TimeoutException("The saga lock was not released: the runner would deadlock.");
            Log.Enqueue($"acquire {sagaName} {correlationId}");
            return new Release(() =>
            {
                Log.Enqueue($"release {sagaName} {correlationId}");
                gate.Release();
            });
        }

        private sealed class Release(Action release) : IAsyncDisposable
        {
            public ValueTask DisposeAsync()
            {
                release();
                return ValueTask.CompletedTask;
            }
        }
    }

    public sealed class HeldWhilePublishing : ConcurrentQueue<bool>;

    private sealed class LockCheckingTransport(RecordingLockProvider locks, IEventDispatcher dispatcher, HeldWhilePublishing held)
        : ISagaTransport
    {
        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            held.Enqueue(locks.IsHeld(((ICorrelatedEvent)@event).CorrelationId));
            return new Transports.InProcessSagaTransport(dispatcher).PublishAsync(@event, ct);
        }
    }

    private static ServiceProvider Build(RecordingLockProvider locks)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(locks);
        services.AddSingleton<HeldWhilePublishing>();
        services.AddEventBus(typeof(SagaLockTests).Assembly);
        services.AddSagaFlux().AddSaga<PingSaga, PingState>().UseTransport<LockCheckingTransport>();
        services.AddSingleton<ISagaLockProvider>(locks);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    [Fact]
    public async Task Each_event_runs_under_the_instance_lock_and_a_self_reacting_saga_does_not_deadlock()
    {
        var locks = new RecordingLockProvider();
        await using var provider = Build(locks);
        var id = Guid.NewGuid();

        await provider.GetRequiredService<IEventDispatcher>().PublishAsync(new Ping(id));

        Assert.Equal(SagaStates.Final.Name, (await provider.GetRequiredService<ISagaRepository<PingState>>().FindAsync(id, default))!.CurrentState);
        Assert.Equal(
            new[]
            {
                $"acquire {nameof(PingSaga)} {id}",
                $"release {nameof(PingSaga)} {id}",
                $"acquire {nameof(PingSaga)} {id}",
                $"release {nameof(PingSaga)} {id}",
            },
            locks.Log);
        // Pong was sent once, after the Ping transition released the lock.
        Assert.Equal(new[] { false }, provider.GetRequiredService<HeldWhilePublishing>());
    }

    [Fact]
    public void The_default_takes_no_lock_and_UseLockProvider_replaces_it()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux();
        Assert.Equal(typeof(NoSagaLockProvider), Assert.Single(services, d => d.ServiceType == typeof(ISagaLockProvider)).ImplementationType);

        services.AddSagaFlux().UseLockProvider<RecordingLockProvider>();

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(ISagaLockProvider));
        Assert.Equal(typeof(RecordingLockProvider), descriptor.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
    }
}
