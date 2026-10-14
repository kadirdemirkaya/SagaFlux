using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.Test;

public class SagaTimeoutDispatcherCancellationTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public Task Pre_cancelled_dispatch_stops_at_store_entry_and_disposes_read_scope()
        => VerifyAsync("before");

    [Fact]
    public Task Cancellation_after_due_read_stops_at_foreach_entry_and_disposes_read_scope()
        => VerifyAsync("after-read");

    [Fact]
    public Task Router_cancellation_bypasses_retry_and_disposes_both_scopes()
        => VerifyAsync("router");

    private static async Task VerifyAsync(string boundary)
    {
        using var cancellation = new CancellationTokenSource();
        var history = new List<string>();
        var id = Guid.NewGuid();
        var otherId = Guid.NewGuid();
        var inner = new InMemorySagaTimeoutStore();
        SagaTimeout[] seeded =
        [
            new(Guid.NewGuid(), id, "first", Now.AddMinutes(-2), new Expired(id)),
            new(Guid.NewGuid(), otherId, "first", Now.AddMinutes(-1), new Expired(otherId)),
            new(Guid.NewGuid(), id, "future", Now.AddMinutes(1), new Expired(id))
        ];
        foreach (var timeout in seeded)
            await inner.ScheduleAsync(timeout, CancellationToken.None);
        var store = new RecordingStore(inner, cancellation, boundary == "after-read", history);
        var router = new CancellingRouter(cancellation, history);
        var services = new ServiceCollection();
        services.AddScoped<ISagaTimeoutStore>(_ => store);
        services.AddScoped<ISagaEventRouter>(_ => router);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var scopes = new RecordingScopeFactory(provider.GetRequiredService<IServiceScopeFactory>(), history);
        var options = new SagaFluxOptions { TimeoutBatchSize = 10, TimeoutRetryDelay = TimeSpan.FromMinutes(5) };
        var dispatcher = new SagaTimeoutDispatcher(scopes, new FakeTimeProvider(Now), options,
            NullLogger<SagaTimeoutDispatcher>.Instance);
        if (boundary == "before")
            cancellation.Cancel();

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => dispatcher.DispatchDueAsync(cancellation.Token));

        Assert.Equal(cancellation.Token, error.CancellationToken);
        Assert.True(cancellation.IsCancellationRequested);
        Assert.Equal(1, store.GetDueCount);
        Assert.Equal(cancellation.Token, store.GetDueToken);
        Assert.Equal(Now, store.ReadAt);
        Assert.Equal(options.TimeoutBatchSize, store.ReadLimit);
        Assert.Equal(boundary == "before", store.CancelledAtReadEntry);
        Assert.Equal(0, store.CompleteCount);
        Assert.Equal(0, store.ScheduleCount);
        if (boundary == "before")
        {
            Assert.Null(store.Returned);
            Assert.Equal(new[] { "scope-1", "get-due", "dispose-1" }, history);
        }
        else
        {
            Assert.Equal(seeded.Take(2), Assert.IsAssignableFrom<IReadOnlyList<SagaTimeout>>(store.Returned));
            Assert.Equal(boundary == "router"
                ? new[] { "scope-1", "get-due", "read", "dispose-1", "scope-2", "route", "cancelled", "dispose-2" }
                : new[] { "scope-1", "get-due", "read", "cancelled", "dispose-1" }, history);
        }
        Assert.Equal(boundary == "router" ? 1 : 0, router.Calls.Count);
        if (boundary == "router")
        {
            var call = Assert.Single(router.Calls);
            Assert.Same(seeded[0].Event, call.Event);
            Assert.Equal(cancellation.Token, call.Token);
            Assert.False(call.CancelledAtEntry);
            Assert.Same(router.Error, error);
        }
        Assert.Equal(boundary == "router" ? 2 : 1, scopes.Scopes.Count);
        Assert.All(scopes.Scopes, scope =>
        {
            Assert.Equal(1, scope.AsyncDisposeCount);
            Assert.Equal(0, scope.SyncDisposeCount);
        });
        Assert.Equal(seeded.Length, inner.Count);
        var retained = await inner.GetDueAsync(DateTimeOffset.MaxValue, int.MaxValue, CancellationToken.None);
        Assert.Equal(seeded.Length, retained.Count);
        for (var i = 0; i < seeded.Length; i++)
        {
            Assert.Equal(seeded[i].Id, retained[i].Id);
            Assert.Equal(seeded[i].CorrelationId, retained[i].CorrelationId);
            Assert.Equal(seeded[i].Key, retained[i].Key);
            Assert.Equal(seeded[i].DueAt, retained[i].DueAt);
            Assert.Same(seeded[i].Event, retained[i].Event);
        }
        Assert.Equal(seeded.Length, inner.Count);
    }

    private sealed record Expired(Guid CorrelationId) : ICorrelatedEvent;
    private sealed record RouteCall(object Event, CancellationToken Token, bool CancelledAtEntry);

    private sealed class CancellingRouter(CancellationTokenSource cancellation, List<string> history) : ISagaEventRouter
    {
        public List<RouteCall> Calls { get; } = [];
        public OperationCanceledException? Error { get; private set; }

        public Task RouteAsync(object @event, CancellationToken ct)
        {
            Calls.Add(new RouteCall(@event, ct, ct.IsCancellationRequested));
            history.Add("route");
            cancellation.Cancel();
            history.Add("cancelled");
            Error = new OperationCanceledException(ct);
            return Task.FromException(Error);
        }
    }

    private sealed class RecordingStore(InMemorySagaTimeoutStore inner, CancellationTokenSource cancellation,
        bool cancelAfterRead, List<string> history) : ISagaTimeoutStore
    {
        public int GetDueCount { get; private set; }
        public int CompleteCount { get; private set; }
        public int ScheduleCount { get; private set; }
        public CancellationToken GetDueToken { get; private set; }
        public bool CancelledAtReadEntry { get; private set; }
        public DateTimeOffset ReadAt { get; private set; }
        public int ReadLimit { get; private set; }
        public IReadOnlyList<SagaTimeout>? Returned { get; private set; }

        public async Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
        {
            GetDueCount++;
            GetDueToken = ct;
            CancelledAtReadEntry = ct.IsCancellationRequested;
            ReadAt = now;
            ReadLimit = maxCount;
            history.Add("get-due");
            Returned = await inner.GetDueAsync(now, maxCount, ct);
            history.Add("read");
            if (cancelAfterRead)
            {
                cancellation.Cancel();
                history.Add("cancelled");
            }
            return Returned;
        }

        public Task CompleteAsync(Guid timeoutId, CancellationToken ct)
        {
            CompleteCount++;
            return inner.CompleteAsync(timeoutId, ct);
        }

        public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
        {
            ScheduleCount++;
            return inner.ScheduleAsync(timeout, ct);
        }

        public Task CancelAsync(Guid correlationId, string key, CancellationToken ct) => inner.CancelAsync(correlationId, key, ct);
        public Task CancelAllAsync(Guid correlationId, CancellationToken ct) => inner.CancelAllAsync(correlationId, ct);
    }

    private sealed class RecordingScopeFactory(IServiceScopeFactory inner, List<string> history) : IServiceScopeFactory
    {
        public List<RecordingScope> Scopes { get; } = [];

        public IServiceScope CreateScope()
        {
            var scope = new RecordingScope(inner.CreateAsyncScope(), Scopes.Count + 1, history);
            Scopes.Add(scope);
            history.Add($"scope-{Scopes.Count}");
            return scope;
        }
    }

    private sealed class RecordingScope(AsyncServiceScope inner, int number, List<string> history) : IServiceScope, IAsyncDisposable
    {
        public IServiceProvider ServiceProvider => inner.ServiceProvider;
        public int AsyncDisposeCount { get; private set; }
        public int SyncDisposeCount { get; private set; }

        public async ValueTask DisposeAsync()
        {
            AsyncDisposeCount++;
            await inner.DisposeAsync();
            history.Add($"dispose-{number}");
        }

        public void Dispose()
        {
            SyncDisposeCount++;
            inner.Dispose();
        }
    }
}
