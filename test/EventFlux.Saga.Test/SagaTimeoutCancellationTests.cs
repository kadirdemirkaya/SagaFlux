using EventFlux.Saga.Concurrency;
using EventFlux.Saga.Repositories;
using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.Test;

public class SagaTimeoutCancellationTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public Task Schedule_after_save_cancellation_preserves_timeouts_and_releases_lock(bool replacement)
        => VerifyAsync("schedule", replacement);

    [Fact]
    public Task Unschedule_after_save_cancellation_preserves_timeouts_and_releases_lock()
        => VerifyAsync("cancel", true);

    [Fact]
    public Task Final_after_save_cancellation_preserves_timeouts_and_releases_lock()
        => VerifyAsync("cancel-all", true);

    private static async Task VerifyAsync(string operation, bool replacement)
    {
        var id = Guid.NewGuid();
        var otherId = Guid.NewGuid();
        var history = new List<string>();
        using var cancellation = new CancellationTokenSource();
        var inner = new InMemorySagaRepository<TimeoutState>();
        var initial = new TimeoutState
        {
            CorrelationId = id,
            CurrentState = TimeoutSaga.Active.Name,
            Updates = 7,
            LastOperation = "seed"
        };
        await inner.SaveAsync(initial, CancellationToken.None);
        var repository = new CancelAfterSaveRepository(inner, cancellation, history);
        var locks = new RecordingLockProvider(history);
        var store = new InMemorySagaTimeoutStore();
        var key = SagaTimeoutKeys.For<Expired>();
        SagaTimeout[] pending =
        [
            new(Guid.NewGuid(), id, replacement ? key : "existing", Now.AddMinutes(-2), new Expired(id)),
            new(Guid.NewGuid(), id, "second", Now.AddMinutes(-1), new Expired(id)),
            new(Guid.NewGuid(), otherId, key, Now.AddMinutes(1), new Expired(otherId))
        ];
        foreach (var timeout in pending)
            await store.ScheduleAsync(timeout, CancellationToken.None);
        var timeouts = new RecordingTimeoutStore(store, locks, history);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ISagaRepository<TimeoutState>>(repository);
        services.AddSingleton<ISagaTimeoutStore>(timeouts);
        services.AddSingleton<ISagaLockProvider>(locks);
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(Now));
        services.AddSagaFlux().AddSaga<TimeoutSaga, TimeoutState>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        ICorrelatedEvent request = operation switch
        {
            "schedule" => new ScheduleRequested(id),
            "cancel" => new UnscheduleRequested(id),
            "cancel-all" => new FinalRequested(id),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        };

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            provider.GetRequiredService<ISagaEventRouter>().RouteAsync(request, cancellation.Token));

        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.Equal(new[] { "acquire", "save", "saved", "cancelled", operation, "dispose" }, history);
        Assert.Equal(cancellation.Token, repository.SaveToken);
        Assert.False(repository.CancelledAtSaveEntry);
        var saved = Assert.IsType<SavedState>(repository.Saved);
        var call = Assert.Single(timeouts.Calls);
        Assert.Equal(operation, call.Operation);
        Assert.Equal(id, call.CorrelationId);
        Assert.Equal(cancellation.Token, call.Token);
        Assert.True(call.CancelledAtEntry);
        Assert.True(call.LockHeldAtEntry);
        Assert.Equal(operation == "cancel-all" ? null : key, call.Key);
        if (operation == "schedule")
        {
            var candidate = Assert.IsType<SagaTimeout>(call.Timeout);
            Assert.DoesNotContain(pending, timeout => timeout.Id == candidate.Id);
            Assert.Equal(Now.AddMinutes(5), candidate.DueAt);
            Assert.Equal(id, Assert.IsType<Expired>(candidate.Event).CorrelationId);
            Assert.Equal(replacement, pending.Any(timeout => timeout.CorrelationId == id && timeout.Key == candidate.Key));
        }
        Assert.Equal(nameof(TimeoutSaga), locks.SagaName);
        Assert.Equal(id, locks.CorrelationId);
        Assert.Equal(cancellation.Token, locks.AcquireToken);
        Assert.Equal(1, locks.AcquireCount);
        Assert.Equal(1, locks.DisposeCount);
        Assert.False(locks.IsHeld);
        Assert.Equal(pending.Length, store.Count);
        var actual = await store.GetDueAsync(DateTimeOffset.MaxValue, int.MaxValue, CancellationToken.None);
        Assert.Equal(pending.Length, actual.Count);
        for (var i = 0; i < pending.Length; i++)
        {
            Assert.Equal(pending[i].Id, actual[i].Id);
            Assert.Equal(pending[i].CorrelationId, actual[i].CorrelationId);
            Assert.Equal(pending[i].Key, actual[i].Key);
            Assert.Equal(pending[i].DueAt, actual[i].DueAt);
            Assert.Same(pending[i].Event, actual[i].Event);
        }
        Assert.Equal(pending.Length, store.Count);
        var persisted = Assert.IsType<TimeoutState>(await repository.FindAsync(id, CancellationToken.None));
        Assert.Equal(id, persisted.CorrelationId);
        Assert.Equal(operation == "cancel-all" ? SagaStates.Final.Name : TimeoutSaga.Updated.Name, persisted.CurrentState);
        Assert.Equal(initial.Version + 1, persisted.Version);
        Assert.Equal(8, persisted.Updates);
        Assert.Equal(operation, persisted.LastOperation);
        Assert.Equal(saved, new SavedState(persisted.CorrelationId, persisted.CurrentState, persisted.Version,
            persisted.Updates, persisted.LastOperation));
        Assert.Equal(0, repository.RemoveCount);
        Assert.Equal(1, inner.Count);
    }

    public sealed record ScheduleRequested(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record UnscheduleRequested(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record FinalRequested(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record Expired(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class TimeoutState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int Updates { get; set; }
        public string LastOperation { get; set; } = string.Empty;
    }

    private sealed class TimeoutSaga : Saga<TimeoutState>
    {
        public static readonly SagaState Active = SagaStates.Define(nameof(Active));
        public static readonly SagaState Updated = SagaStates.Define(nameof(Updated));

        public TimeoutSaga()
        {
            During(Active,
                When<ScheduleRequested>()
                    .Then(ctx =>
                    {
                        ctx.State.Updates++;
                        ctx.State.LastOperation = "schedule";
                    })
                    .Schedule(TimeSpan.FromMinutes(5), ctx => new Expired(ctx.CorrelationId))
                    .TransitionTo(Updated),
                When<UnscheduleRequested>()
                    .Then(ctx =>
                    {
                        ctx.State.Updates++;
                        ctx.State.LastOperation = "cancel";
                    })
                    .Unschedule<Expired>()
                    .TransitionTo(Updated),
                When<FinalRequested>()
                    .Then(ctx =>
                    {
                        ctx.State.Updates++;
                        ctx.State.LastOperation = "cancel-all";
                    })
                    .Finalize());
        }
    }

    private sealed record SavedState(Guid CorrelationId, string CurrentState, int Version, int Updates, string LastOperation);

    private sealed class CancelAfterSaveRepository(
        InMemorySagaRepository<TimeoutState> inner, CancellationTokenSource cancellation, List<string> history)
        : ISagaRepository<TimeoutState>
    {
        public CancellationToken SaveToken { get; private set; }
        public bool CancelledAtSaveEntry { get; private set; }
        public SavedState? Saved { get; private set; }
        public int RemoveCount { get; private set; }

        public Task<TimeoutState?> FindAsync(Guid correlationId, CancellationToken ct) => inner.FindAsync(correlationId, ct);

        public async Task SaveAsync(TimeoutState state, CancellationToken ct)
        {
            history.Add("save");
            SaveToken = ct;
            CancelledAtSaveEntry = ct.IsCancellationRequested;
            await inner.SaveAsync(state, ct);
            Saved = new SavedState(state.CorrelationId, state.CurrentState, state.Version, state.Updates, state.LastOperation);
            history.Add("saved");
            cancellation.Cancel();
            history.Add("cancelled");
        }

        public Task RemoveAsync(Guid correlationId, CancellationToken ct)
        {
            RemoveCount++;
            return inner.RemoveAsync(correlationId, ct);
        }
    }

    private sealed class RecordingLockProvider(List<string> history) : ISagaLockProvider
    {
        public bool IsHeld { get; private set; }
        public int AcquireCount { get; private set; }
        public int DisposeCount { get; private set; }
        public string? SagaName { get; private set; }
        public Guid CorrelationId { get; private set; }
        public CancellationToken AcquireToken { get; private set; }

        public ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            Assert.False(IsHeld);
            SagaName = sagaName;
            CorrelationId = correlationId;
            AcquireToken = ct;
            AcquireCount++;
            IsHeld = true;
            history.Add("acquire");
            return ValueTask.FromResult<IAsyncDisposable>(new Handle(this, history));
        }

        private sealed class Handle(RecordingLockProvider owner, List<string> history) : IAsyncDisposable
        {
            public ValueTask DisposeAsync()
            {
                owner.DisposeCount++;
                owner.IsHeld = false;
                history.Add("dispose");
                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed record TimeoutCall(string Operation, Guid CorrelationId, string? Key, SagaTimeout? Timeout,
        CancellationToken Token, bool CancelledAtEntry, bool LockHeldAtEntry);

    private sealed class RecordingTimeoutStore(
        InMemorySagaTimeoutStore inner, RecordingLockProvider locks, List<string> history) : ISagaTimeoutStore
    {
        public List<TimeoutCall> Calls { get; } = [];

        public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
        {
            Record("schedule", timeout.CorrelationId, timeout.Key, timeout, ct);
            return inner.ScheduleAsync(timeout, ct);
        }

        public Task CancelAsync(Guid correlationId, string key, CancellationToken ct)
        {
            Record("cancel", correlationId, key, null, ct);
            return inner.CancelAsync(correlationId, key, ct);
        }

        public Task CancelAllAsync(Guid correlationId, CancellationToken ct)
        {
            Record("cancel-all", correlationId, null, null, ct);
            return inner.CancelAllAsync(correlationId, ct);
        }

        public Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
            => inner.GetDueAsync(now, maxCount, ct);

        public Task CompleteAsync(Guid timeoutId, CancellationToken ct) => inner.CompleteAsync(timeoutId, ct);

        private void Record(string operation, Guid correlationId, string? key, SagaTimeout? timeout, CancellationToken ct)
        {
            Calls.Add(new TimeoutCall(operation, correlationId, key, timeout, ct, ct.IsCancellationRequested, locks.IsHeld));
            history.Add(operation);
        }
    }
}
