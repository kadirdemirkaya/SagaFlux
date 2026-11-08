using System.Text.Json;
using EventFlux.Abstractions;
using EventFlux.Saga.Timeouts;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed class FinalRemovalOutboxTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Committed_outbox_delivery_is_independent_of_final_removal_cleanup(bool removalFails)
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var options = new DbContextOptionsBuilder<FinalOutboxContext>().UseSqlite(connection).Options;
        var id = Guid.NewGuid();
        await using (var context = new FinalOutboxContext(options))
        {
            await context.Database.EnsureCreatedAsync();
            context.Add(new FinalOutboxState { CorrelationId = id, CurrentState = FinalOutboxSaga.Active.Name, Version = 1 });
            await context.SaveChangesAsync();
        }

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 10, 29, 12, 0, 0, TimeSpan.Zero));
        var stagedAt = time.GetUtcNow();
        var probe = new Probe(removalFails);
        var timeout = new SagaTimeout(Guid.NewGuid(), id, SagaTimeoutKeys.For<Expired>(),
            stagedAt.AddHours(1), new Expired(id));
        var timeouts = new RecordingTimeouts(probe);
        await timeouts.Inner.ScheduleAsync(timeout, default);
        var transport = new RecordingTransport(probe);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(time);
        services.AddDbContext<FinalOutboxContext>(o => o.UseSqlite(connection));
        services.AddEventBus(typeof(FinalRemovalOutboxTests).Assembly);
        services.AddSagaFlux(o =>
            {
                o.MaxConcurrencyRetries = 3;
                o.EnableOutboxPolling = false;
            })
            .AddSaga<FinalOutboxSaga, FinalOutboxState>()
            .UseEfCore<FinalOutboxState, FinalOutboxContext>()
            .UseEfCoreOutbox<FinalOutboxContext>();
        services.AddScoped<EfCoreSagaRepository<FinalOutboxState>>();
        services.AddScoped<ISagaRepository<FinalOutboxState>>(sp => new RemovalRepository(
            sp.GetRequiredService<EfCoreSagaRepository<FinalOutboxState>>(), probe));
        services.AddScoped<EfCoreSagaOutbox>();
        services.AddScoped<ISagaOutbox>(sp => new RecordingOutbox(sp.GetRequiredService<EfCoreSagaOutbox>(), probe));
        services.AddSingleton<ISagaTimeoutStore>(timeouts);
        services.AddSingleton<ISagaTransport>(transport);
        services.AddSingleton<ISagaTransitionObserver>(new RecordingObserver(probe));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Assert.Equal(TimeSpan.FromSeconds(30), provider.GetRequiredService<SagaFluxOptions>().OutboxDispatchDelay);
        var request = new Finish(id);

        await using (var scope = provider.CreateAsyncScope())
        {
            var router = scope.ServiceProvider.GetRequiredService<ISagaEventRouter>();
            if (removalFails)
            {
                var error = await Assert.ThrowsAsync<InvalidOperationException>(() => router.RouteAsync(request, default));
                Assert.Same(probe.Failure, error);
            }
            else
            {
                await router.RouteAsync(request, default);
            }
        }

        var staged = Assert.Single(probe.Staged);
        Assert.NotEqual(Guid.Empty, staged.Id);
        Assert.Equal(stagedAt, staged.StagedAt);
        Assert.Equal(new Finished(id, 1), Assert.IsType<Finished>(staged.Event));
        Assert.Equal(1, probe.Saves);
        Assert.Equal(1, probe.Removals);
        Assert.Equal((id, SagaStates.Final.Name, 2, 1), Assert.Single(probe.Saved));
        Assert.Equal(0, probe.Discards);
        if (removalFails)
        {
            Assert.Equal(new[] { "stage", "save", "saved", "remove" }, probe.History);
            Assert.Empty(transport.Sent);
            Assert.Empty(probe.Completions);
            Assert.Empty(probe.Observed);
            Assert.Equal(0, timeouts.CancelAllCount);
            await AssertPendingRowAsync();
        }
        else
        {
            Assert.Equal(new[] { "stage", "save", "saved", "remove", "removed", "cancel-all", "observe", "send", "complete" }, probe.History);
            Assert.Same(staged.Event, Assert.Single(transport.Sent));
            Assert.Equal(staged.Id, Assert.Single(Assert.Single(probe.Completions)));
            var record = Assert.Single(probe.Observed);
            Assert.True(record.Removed);
            Assert.Equal(id, record.CorrelationId);
            Assert.Equal(SagaStates.Final.Name, record.ToState);
            Assert.Equal(2, record.Version);
            Assert.Equal(1, timeouts.CancelAllCount);
            await AssertNoOutboxRowsAsync();
        }
        await AssertCleanupStateAsync();

        var dispatcher = provider.GetRequiredService<ISagaOutboxDispatcher>();
        Assert.Equal(0, await dispatcher.DispatchPendingAsync());
        Assert.Empty(Assert.Single(probe.PendingBatches));
        Assert.Equal(removalFails ? 0 : 1, transport.Sent.Count);
        Assert.Equal(removalFails ? 0 : 1, probe.Completions.Count);
        if (removalFails)
            await AssertPendingRowAsync();
        await AssertCleanupStateAsync();

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(removalFails ? 1 : 0, await dispatcher.DispatchPendingAsync());
        Assert.Equal(2, probe.PendingBatches.Count);
        if (removalFails)
        {
            var recovered = Assert.Single(probe.PendingBatches[1]);
            Assert.Equal(staged.Id, recovered.Id);
            Assert.Equal(staged.StagedAt, recovered.StagedAt);
            Assert.Equal(staged.Event, Assert.IsType<Finished>(recovered.Event));
            Assert.NotSame(staged.Event, recovered.Event);
            Assert.Same(recovered.Event, Assert.Single(transport.Sent));
        }
        else
        {
            Assert.Empty(probe.PendingBatches[1]);
        }
        Assert.Single(transport.Sent);
        Assert.Equal(staged.Id, Assert.Single(Assert.Single(probe.Completions)));
        await AssertNoOutboxRowsAsync();
        await AssertCleanupStateAsync();

        Assert.Equal(0, await dispatcher.DispatchPendingAsync());
        Assert.Equal(3, probe.PendingBatches.Count);
        Assert.Empty(probe.PendingBatches[2]);
        Assert.Single(transport.Sent);
        Assert.Single(probe.Completions);
        Assert.Single(probe.Staged);
        Assert.Equal(0, probe.Discards);
        Assert.Equal(1, probe.Saves);
        Assert.Equal(1, probe.Removals);
        Assert.Equal(removalFails ? 0 : 1, probe.Observed.Count);
        Assert.Equal(removalFails ? 0 : 1, timeouts.CancelAllCount);
        Assert.Equal(0, timeouts.OtherMutations);
        await AssertNoOutboxRowsAsync();
        await AssertCleanupStateAsync();

        async Task AssertPendingRowAsync()
        {
            await using var context = new FinalOutboxContext(options);
            var row = Assert.Single(await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync());
            Assert.Equal(staged.Id, row.Id);
            Assert.Equal(SagaEventTypeNames.GetName(typeof(Finished)), row.EventType);
            Assert.Equal(JsonSerializer.Serialize((Finished)staged.Event), row.Payload);
            Assert.Equal(stagedAt.UtcTicks, row.StagedAtUtcTicks);
        }

        async Task AssertNoOutboxRowsAsync()
        {
            await using var context = new FinalOutboxContext(options);
            Assert.Empty(await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync());
        }

        async Task AssertCleanupStateAsync()
        {
            await using var context = new FinalOutboxContext(options);
            var rows = await context.Set<FinalOutboxState>().AsNoTracking().ToListAsync();
            var pending = await timeouts.Inner.GetDueAsync(DateTimeOffset.MaxValue, 10, default);
            if (removalFails)
            {
                var saved = Assert.Single(rows);
                Assert.Equal(id, saved.CorrelationId);
                Assert.Equal(SagaStates.Final.Name, saved.CurrentState);
                Assert.Equal(2, saved.Version);
                Assert.Equal(1, saved.FinishCount);
                var retained = Assert.Single(pending);
                Assert.Equal(timeout, retained);
                Assert.Same(timeout.Event, retained.Event);
            }
            else
            {
                Assert.Empty(rows);
                Assert.Empty(pending);
            }
        }
    }

    public sealed record Finish(Guid CorrelationId) : ICorrelatedEvent;
    public sealed record Finished(Guid CorrelationId, int FinishCount) : ICorrelatedEvent;
    public sealed record Expired(Guid CorrelationId) : ICorrelatedEvent;

    public sealed class FinalOutboxState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int FinishCount { get; set; }
    }

    private sealed class FinalOutboxSaga : Saga<FinalOutboxState>
    {
        public static readonly SagaState Active = SagaStates.Define(nameof(Active));

        public FinalOutboxSaga()
        {
            RemoveWhenFinal();
            During(Active, When<Finish>()
                .Then(ctx => ctx.State.FinishCount++)
                .Publish(ctx => new Finished(ctx.CorrelationId, ctx.State.FinishCount))
                .Finalize());
        }
    }

    private sealed class FinalOutboxContext(DbContextOptions<FinalOutboxContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureSagaState<FinalOutboxState>();
            modelBuilder.ConfigureSagaOutbox();
        }
    }

    private sealed class Probe(bool removalFails)
    {
        public bool RemovalFails { get; } = removalFails;
        public InvalidOperationException Failure { get; } = new("pre-delete storage failure");
        public List<string> History { get; } = [];
        public List<SagaOutboxMessage> Staged { get; } = [];
        public List<SagaOutboxMessage[]> PendingBatches { get; } = [];
        public List<Guid[]> Completions { get; } = [];
        public List<SagaTransitionRecord> Observed { get; } = [];
        public List<(Guid Id, string State, int Version, int FinishCount)> Saved { get; } = [];
        public int Saves { get; set; }
        public int Removals { get; set; }
        public int Discards { get; set; }
    }

    private sealed class RemovalRepository(EfCoreSagaRepository<FinalOutboxState> inner, Probe probe)
        : ISagaRepository<FinalOutboxState>
    {
        public Task<FinalOutboxState?> FindAsync(Guid correlationId, CancellationToken ct) => inner.FindAsync(correlationId, ct);

        public async Task SaveAsync(FinalOutboxState state, CancellationToken ct)
        {
            probe.Saves++;
            probe.History.Add("save");
            await inner.SaveAsync(state, ct);
            probe.Saved.Add((state.CorrelationId, state.CurrentState, state.Version, state.FinishCount));
            probe.History.Add("saved");
        }

        public async Task RemoveAsync(Guid correlationId, CancellationToken ct)
        {
            probe.Removals++;
            probe.History.Add("remove");
            Assert.Equal(Assert.Single(probe.Saved).Id, correlationId);
            if (probe.RemovalFails)
                throw probe.Failure;
            await inner.RemoveAsync(correlationId, ct);
            probe.History.Add("removed");
        }
    }

    private sealed class RecordingOutbox(EfCoreSagaOutbox inner, Probe probe) : ISagaOutbox
    {
        public void Stage(IReadOnlyList<SagaOutboxMessage> messages)
        {
            inner.Stage(messages);
            probe.Staged.AddRange(messages);
            probe.History.Add("stage");
        }

        public void Discard()
        {
            probe.Discards++;
            inner.Discard();
        }

        public async Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct)
        {
            await inner.CompleteAsync(ids, ct);
            probe.Completions.Add(ids.ToArray());
            probe.History.Add("complete");
        }

        public async Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct)
        {
            var pending = await inner.GetPendingAsync(stagedBefore, maxCount, ct);
            probe.PendingBatches.Add(pending.ToArray());
            return pending;
        }
    }

    private sealed class RecordingTransport(Probe probe) : ISagaTransport
    {
        public List<IEventRequest> Sent { get; } = [];

        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            Sent.Add(@event);
            probe.History.Add("send");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingObserver(Probe probe) : ISagaTransitionObserver
    {
        public Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct)
        {
            probe.Observed.Add(record);
            probe.History.Add("observe");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingTimeouts(Probe probe) : ISagaTimeoutStore
    {
        public InMemorySagaTimeoutStore Inner { get; } = new();
        public int CancelAllCount { get; private set; }
        public int OtherMutations { get; private set; }

        public Task CancelAllAsync(Guid correlationId, CancellationToken ct)
        {
            CancelAllCount++;
            probe.History.Add("cancel-all");
            return Inner.CancelAllAsync(correlationId, ct);
        }

        public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
        {
            OtherMutations++;
            return Inner.ScheduleAsync(timeout, ct);
        }

        public Task CancelAsync(Guid correlationId, string key, CancellationToken ct)
        {
            OtherMutations++;
            return Inner.CancelAsync(correlationId, key, ct);
        }

        public Task CompleteAsync(Guid timeoutId, CancellationToken ct)
        {
            OtherMutations++;
            return Inner.CompleteAsync(timeoutId, ct);
        }

        public Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
            => Inner.GetDueAsync(now, maxCount, ct);
    }
}
