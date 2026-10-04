using EfCore.Repository.Concretes;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed record HoldRequested(Guid CorrelationId) : ICorrelatedEvent;
public sealed record HoldPaid(Guid CorrelationId) : ICorrelatedEvent;
public sealed record HoldExpired(Guid CorrelationId, string Reason) : ICorrelatedEvent;

[SagaEventFormerName("Shop.Holds.HoldTimedOut, Shop.Holds")]
public sealed record HoldLapsed(Guid CorrelationId, string Reason) : ICorrelatedEvent;

public sealed class HoldState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public string? ExpiryReason { get; set; }
}

public sealed class HoldSaga : Saga<HoldState>
{
    public static readonly SagaState Waiting = SagaStates.Define(nameof(Waiting));
    public static readonly SagaState Expired = SagaStates.Define(nameof(Expired));

    public HoldSaga()
    {
        During(SagaStates.Initial,
            When<HoldRequested>()
                .Schedule(TimeSpan.FromMinutes(5), ctx => new HoldExpired(ctx.CorrelationId, "unpaid"))
                .TransitionTo(Waiting));
        During(Waiting,
            When<HoldPaid>().Finalize(),
            When<HoldExpired>().Then(ctx => ctx.State.ExpiryReason = ctx.Event.Reason).TransitionTo(Expired));
    }
}

public sealed class TimeoutDbContext(DbContextOptions<TimeoutDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureSagaState<HoldState>().ToTable("HoldSagas");
        modelBuilder.ConfigureSagaTimeouts();
    }
}

public sealed class EfCoreSagaTimeoutStoreTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly DateTimeOffset _now = new(2026, 9, 26, 12, 0, 0, TimeSpan.Zero);

    public EfCoreSagaTimeoutStoreTests()
    {
        _connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    private TimeoutDbContext CreateContext()
        => new(new DbContextOptionsBuilder<TimeoutDbContext>().UseSqlite(_connection).Options);

    private static EfCoreSagaTimeoutStore Store(TimeoutDbContext context)
        => new(new ReadRepository<SagaTimeoutRecord>(context, null!), new WriteRepository<SagaTimeoutRecord>(context, null!),
            NullLogger<EfCoreSagaTimeoutStore>.Instance);

    private SagaTimeout Timeout(Guid correlationId, TimeSpan after, string reason = "unpaid")
        => new(Guid.NewGuid(), correlationId, SagaTimeoutKeys.For<HoldExpired>(), _now + after, new HoldExpired(correlationId, reason));

    [Fact]
    public async Task Due_timeouts_round_trip_in_due_order()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        await using (var context = CreateContext())
        {
            await Store(context).ScheduleAsync(Timeout(a, TimeSpan.FromMinutes(10), "late"), default);
            await Store(context).ScheduleAsync(Timeout(b, TimeSpan.FromMinutes(5), "early"), default);
        }

        await using (var context = CreateContext())
        {
            Assert.Empty(await Store(context).GetDueAsync(_now + TimeSpan.FromMinutes(4), 10, default));

            var due = await Store(context).GetDueAsync(_now + TimeSpan.FromMinutes(10), 10, default);
            Assert.Equal(new[] { b, a }, due.Select(d => d.CorrelationId));
            var expired = Assert.IsType<HoldExpired>(due[0].Event);
            Assert.Equal("early", expired.Reason);
            Assert.Equal(_now + TimeSpan.FromMinutes(5), due[0].DueAt);

            Assert.Single(await Store(context).GetDueAsync(_now + TimeSpan.FromHours(1), 1, default));
        }
    }

    [Fact]
    public async Task Scheduling_the_same_key_replaces_and_cancel_removes()
    {
        var id = Guid.NewGuid();
        await using var context = CreateContext();
        var store = Store(context);

        await store.ScheduleAsync(Timeout(id, TimeSpan.FromMinutes(5)), default);
        var replacement = Timeout(id, TimeSpan.FromMinutes(30));
        await store.ScheduleAsync(replacement, default);

        var due = Assert.Single(await store.GetDueAsync(_now + TimeSpan.FromHours(1), 10, default));
        Assert.Equal(replacement.Id, due.Id);

        await store.CancelAsync(id, SagaTimeoutKeys.For<HoldExpired>(), default);
        Assert.Empty(await store.GetDueAsync(_now + TimeSpan.FromHours(1), 10, default));
    }

    [Fact]
    public async Task Complete_removes_only_that_scheduling_and_cancel_all_clears_the_instance()
    {
        var id = Guid.NewGuid();
        var other = Guid.NewGuid();
        await using var context = CreateContext();
        var store = Store(context);
        var first = Timeout(id, TimeSpan.FromMinutes(5));
        await store.ScheduleAsync(first, default);
        await store.ScheduleAsync(Timeout(other, TimeSpan.FromMinutes(5)), default);

        await store.CompleteAsync(Guid.NewGuid(), default);
        Assert.Equal(2, (await store.GetDueAsync(_now + TimeSpan.FromHours(1), 10, default)).Count);

        await store.CompleteAsync(first.Id, default);
        await store.CancelAllAsync(other, default);
        Assert.Empty(await store.GetDueAsync(_now + TimeSpan.FromHours(1), 10, default));
    }

    [Fact]
    public async Task Unresolvable_event_type_is_skipped_not_deleted()
    {
        await using var context = CreateContext();
        context.Set<SagaTimeoutRecord>().Add(new SagaTimeoutRecord
        {
            Id = Guid.NewGuid(),
            CorrelationId = Guid.NewGuid(),
            Key = "Removed",
            DueAtUtcTicks = _now.UtcTicks,
            EventType = "Removed.Event, Removed.Assembly",
            Payload = "{}"
        });
        await context.SaveChangesAsync();

        Assert.Empty(await Store(context).GetDueAsync(_now, 10, default));
        Assert.Equal(1, await context.Set<SagaTimeoutRecord>().CountAsync());
    }

    [Fact]
    public async Task A_record_stored_under_a_former_event_name_resolves_to_the_renamed_type()
    {
        var id = Guid.NewGuid();
        await using var context = CreateContext();
        context.Set<SagaTimeoutRecord>().Add(new SagaTimeoutRecord
        {
            Id = Guid.NewGuid(),
            CorrelationId = id,
            Key = "HoldTimedOut",
            DueAtUtcTicks = _now.UtcTicks,
            EventType = "Shop.Holds.HoldTimedOut, Shop.Holds",
            Payload = $$"""{"CorrelationId":"{{id}}","Reason":"unpaid"}"""
        });
        await context.SaveChangesAsync();

        var due = Assert.Single(await Store(context).GetDueAsync(_now, 10, default));

        Assert.Equal(new HoldLapsed(id, "unpaid"), due.Event);
        Assert.Equal(typeof(HoldLapsed), SagaEventTypeNames.Resolve(SagaEventTypeNames.GetName(typeof(HoldLapsed))));
        Assert.Null(SagaEventTypeNames.Resolve("Nowhere.Unknown, Nowhere"));
    }

    [Fact]
    public async Task Timeout_survives_a_restart_and_drives_the_saga()
    {
        var time = new FakeTimeProvider(_now);
        var id = Guid.NewGuid();

        ServiceProvider Build()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<TimeProvider>(time);
            services.AddDbContext<TimeoutDbContext>(o => o.UseSqlite(_connection));
            services.AddEventBus(typeof(EfCoreSagaTimeoutStoreTests).Assembly);
            services.AddSagaFlux()
                    .AddSaga<HoldSaga, HoldState>()
                    .UseEfCore<HoldState, TimeoutDbContext>()
                    .UseEfCoreTimeouts<TimeoutDbContext>();
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }

        await using (var before = Build())
        await using (var scope = before.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new HoldRequested(id));

        time.Advance(TimeSpan.FromMinutes(5));
        await using var after = Build();
        Assert.Equal(1, await after.GetRequiredService<ISagaTimeoutDispatcher>().DispatchDueAsync());

        await using var verify = CreateContext();
        var state = await verify.Set<HoldState>().SingleAsync(s => s.CorrelationId == id);
        Assert.Equal(HoldSaga.Expired.Name, state.CurrentState);
        Assert.Equal("unpaid", state.ExpiryReason);
        Assert.Equal(0, await verify.Set<SagaTimeoutRecord>().CountAsync());
    }

    [Fact]
    public async Task Finalizing_cancels_persisted_timeouts()
    {
        var id = Guid.NewGuid();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(_now));
        services.AddDbContext<TimeoutDbContext>(o => o.UseSqlite(_connection));
        services.AddEventBus(typeof(EfCoreSagaTimeoutStoreTests).Assembly);
        services.AddSagaFlux().AddSaga<HoldSaga, HoldState>().UseEfCore<HoldState, TimeoutDbContext>().UseEfCoreTimeouts<TimeoutDbContext>();
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var dispatcher = scope.ServiceProvider.GetRequiredService<IEventDispatcher>();
            await dispatcher.PublishAsync(new HoldRequested(id));
            await dispatcher.PublishAsync(new HoldPaid(id));
        }

        await using var verify = CreateContext();
        Assert.Equal(0, await verify.Set<SagaTimeoutRecord>().CountAsync());
    }

    public void Dispose() => _connection.Dispose();
}
