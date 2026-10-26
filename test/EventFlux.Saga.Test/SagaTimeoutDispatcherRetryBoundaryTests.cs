using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.Test;

public class SagaTimeoutDispatcherRetryBoundaryTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 24, 8, 0, 0, TimeSpan.Zero);

    [Fact]
    public Task Router_operation_cancelled_exception_with_uncancelled_token_schedules_retry()
        => VerifyAsync(new OperationCanceledException(), false);

    [Fact]
    public Task Failed_timeout_schedules_retry_and_next_timeout_in_batch_completes()
        => VerifyAsync(new InvalidOperationException("Timeout delivery failed"), true);

    private static async Task VerifyAsync(Exception error, bool includeHealthy)
    {
        using var cancellation = new CancellationTokenSource();
        var failedId = Guid.NewGuid();
        var healthyId = Guid.NewGuid();
        var failed = new SagaTimeout(Guid.NewGuid(), failedId, "failed", Now.AddMinutes(-2), new Expired(failedId));
        var healthy = new SagaTimeout(Guid.NewGuid(), healthyId, "healthy", Now.AddMinutes(-1), new Expired(healthyId));
        var inner = new InMemorySagaTimeoutStore();
        await inner.ScheduleAsync(failed, CancellationToken.None);
        if (includeHealthy)
            await inner.ScheduleAsync(healthy, CancellationToken.None);
        var store = new RecordingStore(inner);
        var router = new FailingRouter(failed.Event, error);
        var services = new ServiceCollection();
        services.AddScoped<ISagaTimeoutStore>(_ => store);
        services.AddScoped<ISagaEventRouter>(_ => router);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var scopes = new RecordingScopeFactory(provider.GetRequiredService<IServiceScopeFactory>());
        var options = new SagaFluxOptions { TimeoutBatchSize = 10, TimeoutRetryDelay = TimeSpan.FromMinutes(5) };
        var dispatcher = new SagaTimeoutDispatcher(scopes, new FakeTimeProvider(Now), options,
            NullLogger<SagaTimeoutDispatcher>.Instance);

        var delivered = await dispatcher.DispatchDueAsync(cancellation.Token);

        Assert.False(cancellation.IsCancellationRequested);
        Assert.Equal(includeHealthy ? 1 : 0, delivered);
        Assert.Equal(1, store.ReadCount);
        Assert.Equal(Now, store.ReadAt);
        Assert.Equal(options.TimeoutBatchSize, store.ReadLimit);
        Assert.Equal(cancellation.Token, store.ReadToken);
        Assert.Equal(includeHealthy ? new[] { failed, healthy } : new[] { failed }, store.Returned);
        Assert.Equal(includeHealthy ? 2 : 1, router.Calls.Count);
        Assert.Same(failed.Event, router.Calls[0].Event);
        if (includeHealthy)
            Assert.Same(healthy.Event, router.Calls[1].Event);
        Assert.All(router.Calls, call => Assert.Equal(cancellation.Token, call.Token));
        var scheduled = Assert.Single(store.Scheduled);
        Assert.Equal(CancellationToken.None, scheduled.Token);
        var retry = scheduled.Timeout;
        Assert.NotEqual(Guid.Empty, retry.Id);
        Assert.NotEqual(failed.Id, retry.Id);
        Assert.Equal(Now + options.TimeoutRetryDelay, retry.DueAt);
        Assert.Equal(failed.CorrelationId, retry.CorrelationId);
        Assert.Equal(failed.Key, retry.Key);
        Assert.Same(failed.Event, retry.Event);
        Assert.DoesNotContain(store.Completed, call => call.Id == failed.Id);
        if (includeHealthy)
        {
            var completed = Assert.Single(store.Completed);
            Assert.Equal(healthy.Id, completed.Id);
            Assert.Equal(cancellation.Token, completed.Token);
        }
        else
            Assert.Empty(store.Completed);
        Assert.Equal(1, inner.Count);
        var retained = Assert.Single(await inner.GetDueAsync(DateTimeOffset.MaxValue, int.MaxValue, CancellationToken.None));
        Assert.Equal(retry, retained);
        Assert.Empty(await inner.GetDueAsync(Now, options.TimeoutBatchSize, CancellationToken.None));
        Assert.Equal(includeHealthy ? 3 : 2, scopes.Scopes.Count);
        Assert.All(scopes.Scopes, scope =>
        {
            Assert.Equal(1, scope.AsyncDisposeCount);
            Assert.Equal(0, scope.SyncDisposeCount);
        });
    }

    private sealed record Expired(Guid CorrelationId) : ICorrelatedEvent;

    private sealed class FailingRouter(object failedEvent, Exception error) : ISagaEventRouter
    {
        public List<(object Event, CancellationToken Token)> Calls { get; } = [];

        public Task RouteAsync(object @event, CancellationToken ct)
        {
            Calls.Add((@event, ct));
            return ReferenceEquals(@event, failedEvent) ? Task.FromException(error) : Task.CompletedTask;
        }
    }

    private sealed class RecordingStore(InMemorySagaTimeoutStore inner) : ISagaTimeoutStore
    {
        public List<(SagaTimeout Timeout, CancellationToken Token)> Scheduled { get; } = [];
        public List<(Guid Id, CancellationToken Token)> Completed { get; } = [];
        public int ReadCount { get; private set; }
        public DateTimeOffset ReadAt { get; private set; }
        public int ReadLimit { get; private set; }
        public CancellationToken ReadToken { get; private set; }
        public IReadOnlyList<SagaTimeout>? Returned { get; private set; }

        public async Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
        {
            ReadCount++;
            ReadAt = now;
            ReadLimit = maxCount;
            ReadToken = ct;
            Returned = await inner.GetDueAsync(now, maxCount, ct);
            return Returned;
        }

        public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
        {
            Scheduled.Add((timeout, ct));
            return inner.ScheduleAsync(timeout, ct);
        }

        public Task CompleteAsync(Guid timeoutId, CancellationToken ct)
        {
            Completed.Add((timeoutId, ct));
            return inner.CompleteAsync(timeoutId, ct);
        }

        public Task CancelAsync(Guid correlationId, string key, CancellationToken ct) => inner.CancelAsync(correlationId, key, ct);
        public Task CancelAllAsync(Guid correlationId, CancellationToken ct) => inner.CancelAllAsync(correlationId, ct);
    }

    private sealed class RecordingScopeFactory(IServiceScopeFactory inner) : IServiceScopeFactory
    {
        public List<RecordingScope> Scopes { get; } = [];

        public IServiceScope CreateScope()
        {
            var scope = new RecordingScope(inner.CreateAsyncScope());
            Scopes.Add(scope);
            return scope;
        }
    }

    private sealed class RecordingScope(AsyncServiceScope inner) : IServiceScope, IAsyncDisposable
    {
        public IServiceProvider ServiceProvider => inner.ServiceProvider;
        public int AsyncDisposeCount { get; private set; }
        public int SyncDisposeCount { get; private set; }

        public async ValueTask DisposeAsync()
        {
            AsyncDisposeCount++;
            await inner.DisposeAsync();
        }

        public void Dispose()
        {
            SyncDisposeCount++;
            inner.Dispose();
        }
    }
}
