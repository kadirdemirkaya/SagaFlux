using EventFlux.Abstractions;
using EventFlux.Saga.Concurrency;
using EventFlux.Saga.Repositories;
using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public sealed class SagaFinalRemovalFailureTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Removal_failure_leaves_final_saved_but_does_not_send_or_cancel_timeouts_on_redelivery(bool concurrency)
    {
        var id = Guid.NewGuid();
        var history = new List<string>();
        var inner = new InMemorySagaRepository<FinalState>();
        await inner.SaveAsync(new FinalState { CorrelationId = id, CurrentState = FinalSaga.Active.Name }, default);
        Exception failure = concurrency ? new SagaConcurrencyException(id, 2) : new InvalidOperationException("remove failed");
        var locks = new RecordingLocks(history);
        var repository = new FailingRemovalRepository(inner, failure, locks, history);
        var transport = new RecordingTransport();
        var timeouts = new RecordingTimeouts();
        var timeout = new SagaTimeout(Guid.NewGuid(), id, SagaTimeoutKeys.For<Expired>(),
            new DateTimeOffset(2026, 10, 29, 12, 0, 0, TimeSpan.Zero), new Expired(id));
        await timeouts.Inner.ScheduleAsync(timeout, default);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ISagaRepository<FinalState>>(repository);
        services.AddSingleton<ISagaTransport>(transport);
        services.AddSingleton<ISagaTimeoutStore>(timeouts);
        services.AddSingleton<ISagaLockProvider>(locks);
        services.AddSagaFlux(options => options.MaxConcurrencyRetries = 3).AddSaga<FinalSaga, FinalState>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Assert.Null(provider.GetService<ISagaOutbox>());
        var router = provider.GetRequiredService<ISagaEventRouter>();
        var request = new Finish(id);

        var error = await Assert.ThrowsAsync(failure.GetType(), () => router.RouteAsync(request, default));

        Assert.Same(failure, error);
        Assert.Equal(new[] { "acquire", "find", "save", "saved", "remove", "dispose" }, history);
        Assert.Equal(1, repository.FindCount);
        Assert.Equal(1, repository.SaveCount);
        Assert.Equal(1, repository.RemoveCount);
        Assert.Equal(1, locks.AcquireCount);
        Assert.Equal(1, locks.DisposeCount);
        Assert.False(locks.IsHeld);
        Assert.Empty(transport.Sent);
        Assert.Equal(0, timeouts.CancelAllCount);
        Assert.Empty(timeouts.OtherMutations);
        var saved = Assert.Single(repository.Saved);
        Assert.Equal((id, SagaStates.Final.Name, 2, 1), saved);
        await AssertPersistedAsync();

        repository.RemoveFails = false;
        await router.RouteAsync(request, default);

        Assert.Equal(new[] { "acquire", "find", "save", "saved", "remove", "dispose", "acquire", "find", "dispose" }, history);
        Assert.Equal(2, repository.FindCount);
        Assert.Equal(1, repository.SaveCount);
        Assert.Equal(1, repository.RemoveCount);
        Assert.Single(repository.Saved);
        Assert.Equal(2, locks.AcquireCount);
        Assert.Equal(2, locks.DisposeCount);
        Assert.False(locks.IsHeld);
        Assert.Empty(transport.Sent);
        Assert.Equal(0, timeouts.CancelAllCount);
        Assert.Empty(timeouts.OtherMutations);
        await AssertPersistedAsync();

        async Task AssertPersistedAsync()
        {
            var persisted = Assert.IsType<FinalState>(await inner.FindAsync(id, default));
            Assert.Equal(id, persisted.CorrelationId);
            Assert.Equal(SagaStates.Final.Name, persisted.CurrentState);
            Assert.Equal(2, persisted.Version);
            Assert.Equal(1, persisted.FinishCount);
            Assert.Equal(1, inner.Count);
            var retained = Assert.Single(await timeouts.Inner.GetDueAsync(DateTimeOffset.MaxValue, 10, default));
            Assert.Equal(timeout, retained);
            Assert.Same(timeout.Event, retained.Event);
        }
    }

    [Fact]
    public async Task Successful_removal_sends_the_final_event_and_cancels_timeouts()
    {
        var id = Guid.NewGuid();
        var history = new List<string>();
        var inner = new InMemorySagaRepository<FinalState>();
        await inner.SaveAsync(new FinalState { CorrelationId = id, CurrentState = FinalSaga.Active.Name }, default);
        var locks = new RecordingLocks(history);
        var repository = new FailingRemovalRepository(inner, new InvalidOperationException(), locks, history)
        {
            RemoveFails = false
        };
        var transport = new RecordingTransport();
        var timeouts = new RecordingTimeouts();
        await timeouts.Inner.ScheduleAsync(new SagaTimeout(Guid.NewGuid(), id, SagaTimeoutKeys.For<Expired>(),
            DateTimeOffset.MinValue, new Expired(id)), default);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<ISagaRepository<FinalState>>(repository);
        services.AddSingleton<ISagaTransport>(transport);
        services.AddSingleton<ISagaTimeoutStore>(timeouts);
        services.AddSingleton<ISagaLockProvider>(locks);
        services.AddSagaFlux().AddSaga<FinalSaga, FinalState>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Assert.Null(provider.GetService<ISagaOutbox>());

        await provider.GetRequiredService<ISagaEventRouter>().RouteAsync(new Finish(id), default);

        Assert.Equal(id, Assert.IsType<Finished>(Assert.Single(transport.Sent)).CorrelationId);
        Assert.Equal(1, timeouts.CancelAllCount);
        Assert.Equal(0, timeouts.Inner.Count);
        Assert.Null(await inner.FindAsync(id, default));
        Assert.Equal(0, inner.Count);
        Assert.Equal((id, SagaStates.Final.Name, 2, 1), Assert.Single(repository.Saved));
        Assert.Equal(1, repository.RemoveCount);
        Assert.Equal(1, locks.AcquireCount);
        Assert.Equal(1, locks.DisposeCount);
        Assert.False(locks.IsHeld);
        Assert.Equal(new[] { "acquire", "find", "save", "saved", "remove", "dispose" }, history);
    }

    public sealed record Finish(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record Finished(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record Expired(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class FinalState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int FinishCount { get; set; }
    }

    private sealed class FinalSaga : Saga<FinalState>
    {
        public static readonly SagaState Active = SagaStates.Define(nameof(Active));

        public FinalSaga()
        {
            RemoveWhenFinal();
            During(Active, When<Finish>()
                .Then(ctx => ctx.State.FinishCount++)
                .Publish(ctx => new Finished(ctx.CorrelationId))
                .Finalize());
        }
    }

    private sealed class FailingRemovalRepository(
        InMemorySagaRepository<FinalState> inner, Exception failure, RecordingLocks locks, List<string> history)
        : ISagaRepository<FinalState>
    {
        public int FindCount { get; private set; }
        public int SaveCount { get; private set; }
        public int RemoveCount { get; private set; }
        public bool RemoveFails { get; set; } = true;
        public List<(Guid Id, string State, int Version, int FinishCount)> Saved { get; } = [];

        public Task<FinalState?> FindAsync(Guid correlationId, CancellationToken ct)
        {
            Assert.True(locks.IsHeld);
            FindCount++;
            history.Add("find");
            return inner.FindAsync(correlationId, ct);
        }

        public async Task SaveAsync(FinalState state, CancellationToken ct)
        {
            Assert.True(locks.IsHeld);
            SaveCount++;
            history.Add("save");
            await inner.SaveAsync(state, ct);
            Saved.Add((state.CorrelationId, state.CurrentState, state.Version, state.FinishCount));
            history.Add("saved");
        }

        public Task RemoveAsync(Guid correlationId, CancellationToken ct)
        {
            Assert.True(locks.IsHeld);
            Assert.Equal(Assert.Single(Saved).Id, correlationId);
            RemoveCount++;
            history.Add("remove");
            return RemoveFails ? Task.FromException(failure) : inner.RemoveAsync(correlationId, ct);
        }
    }

    private sealed class RecordingTransport : ISagaTransport
    {
        public List<IEventRequest> Sent { get; } = [];

        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            Sent.Add(@event);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingTimeouts : ISagaTimeoutStore
    {
        public InMemorySagaTimeoutStore Inner { get; } = new();
        public int CancelAllCount { get; private set; }
        public List<string> OtherMutations { get; } = [];

        public Task CancelAllAsync(Guid correlationId, CancellationToken ct)
        {
            CancelAllCount++;
            return Inner.CancelAllAsync(correlationId, ct);
        }

        public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
        {
            OtherMutations.Add("schedule");
            return Inner.ScheduleAsync(timeout, ct);
        }

        public Task CancelAsync(Guid correlationId, string key, CancellationToken ct)
        {
            OtherMutations.Add("cancel");
            return Inner.CancelAsync(correlationId, key, ct);
        }

        public Task CompleteAsync(Guid timeoutId, CancellationToken ct)
        {
            OtherMutations.Add("complete");
            return Inner.CompleteAsync(timeoutId, ct);
        }

        public Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
            => Inner.GetDueAsync(now, maxCount, ct);
    }

    private sealed class RecordingLocks(List<string> history) : ISagaLockProvider
    {
        public int AcquireCount { get; private set; }
        public int DisposeCount { get; private set; }
        public bool IsHeld { get; private set; }

        public ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct)
        {
            Assert.False(IsHeld);
            Assert.Equal(nameof(FinalSaga), sagaName);
            AcquireCount++;
            IsHeld = true;
            history.Add("acquire");
            return ValueTask.FromResult<IAsyncDisposable>(new Handle(this, history));
        }

        private sealed class Handle(RecordingLocks owner, List<string> history) : IAsyncDisposable
        {
            public ValueTask DisposeAsync()
            {
                Assert.True(owner.IsHeld);
                owner.DisposeCount++;
                owner.IsHeld = false;
                history.Add("dispose");
                return ValueTask.CompletedTask;
            }
        }
    }
}
