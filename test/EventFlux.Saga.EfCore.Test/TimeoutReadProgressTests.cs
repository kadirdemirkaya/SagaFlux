using System.Text.Json;
using EfCore.Repository.Concretes;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed class TimeoutReadProgressTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly DateTimeOffset _now = new(2026, 9, 26, 12, 0, 0, TimeSpan.Zero);

    public TimeoutReadProgressTests()
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

    private SagaTimeoutRecord Record(int index, bool valid, DateTimeOffset dueAt)
    {
        var correlationId = Guid.NewGuid();
        return new SagaTimeoutRecord
        {
            Id = Guid.Parse($"00000000-0000-0000-0000-{index:D12}"),
            CorrelationId = correlationId,
            Key = SagaTimeoutKeys.For<HoldExpired>(),
            DueAtUtcTicks = dueAt.UtcTicks,
            EventType = valid ? SagaEventTypeNames.GetName(typeof(HoldExpired)) : "Removed.Timeout, Removed.Assembly",
            Payload = JsonSerializer.Serialize(new HoldExpired(correlationId, $"expiry-{index}"))
        };
    }

    [Fact]
    public async Task Unresolved_first_row_does_not_block_a_due_timeout_or_dispatcher()
    {
        var invalid = Record(1, false, _now.AddMinutes(-2));
        var valid = Record(2, true, _now);
        var future = Record(3, true, _now.AddTicks(1));
        await using (var context = CreateContext())
        {
            context.AddRange(invalid, valid, future);
            context.Add(new HoldState { CorrelationId = valid.CorrelationId, CurrentState = HoldSaga.Waiting.Name, Version = 1 });
            await context.SaveChangesAsync();
        }
        await using (var context = CreateContext())
        {
            var due = Assert.Single(await Store(context).GetDueAsync(_now, 1, default).WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(valid.Id, due.Id);
            Assert.Equal(valid.CorrelationId, due.CorrelationId);
            Assert.Equal(valid.Key, due.Key);
            Assert.Equal(_now, due.DueAt);
            Assert.Equal(new HoldExpired(valid.CorrelationId, "expiry-2"), due.Event);
        }

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(_now));
        services.AddDbContext<TimeoutDbContext>(o => o.UseSqlite(_connection));
        services.AddEventBus(typeof(TimeoutReadProgressTests).Assembly);
        services.AddSagaFlux(options => options.TimeoutBatchSize = 1)
            .AddSaga<HoldSaga, HoldState>()
            .UseEfCore<HoldState, TimeoutDbContext>()
            .UseEfCoreTimeouts<TimeoutDbContext>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Assert.Equal(1, await provider.GetRequiredService<ISagaTimeoutDispatcher>().DispatchDueAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(0, await provider.GetRequiredService<ISagaTimeoutDispatcher>().DispatchDueAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        await using var verify = CreateContext();
        var state = await verify.Set<HoldState>().SingleAsync();
        Assert.Equal(HoldSaga.Expired.Name, state.CurrentState);
        Assert.Equal("expiry-2", state.ExpiryReason);
        var retained = await verify.Set<SagaTimeoutRecord>().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(new[] { invalid.Id, future.Id }, retained.Select(r => r.Id));
        AssertRecord(invalid, retained[0]);
        AssertRecord(future, retained[1]);
    }

    [Theory]
    [InlineData("xxxvxvxv", 2, false)]
    [InlineData("xxvvxvv", 3, false)]
    [InlineData("vvvv", 2, false)]
    [InlineData("xxxx", 2, false)]
    [InlineData("xxx", 2, false)]
    [InlineData("xvxxx", 2, false)]
    [InlineData("", 1, false)]
    [InlineData("xxxvxvxv", 2, true)]
    [InlineData("vv", 0, false)]
    [InlineData("vv", -1, false)]
    public async Task Reads_fill_only_valid_capacity_and_finish_at_the_end(string pattern, int maxCount, bool equalDueTimes)
    {
        var records = pattern.Select((kind, index) => Record(index + 1, kind == 'v',
            equalDueTimes ? _now : _now.AddMinutes(index - pattern.Length))).ToArray();
        await using var context = CreateContext();
        context.AddRange(records.AsEnumerable().Reverse());
        await context.SaveChangesAsync();
        context.ChangeTracker.Clear();

        var expected = records.Where((_, index) => pattern[index] == 'v').Take(Math.Max(0, maxCount)).ToArray();
        var due = await Store(context).GetDueAsync(_now, maxCount, default).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(expected.Select(r => r.Id), due.Select(t => t.Id));
        Assert.All(due, timeout => Assert.IsType<HoldExpired>(timeout.Event));
        Assert.Empty(context.ChangeTracker.Entries());
        var retained = await context.Set<SagaTimeoutRecord>().AsNoTracking().OrderBy(r => r.Id).ToListAsync();
        Assert.Equal(records.Length, retained.Count);
        for (var index = 0; index < records.Length; index++)
            AssertRecord(records[index], retained[index]);
    }

    [Fact]
    public async Task Equal_due_times_progress_across_successive_completed_batches()
    {
        var records = Enumerable.Range(1, 7).Select(index => Record(index, index % 2 == 0, _now)).ToArray();
        await using var context = CreateContext();
        context.AddRange(records.AsEnumerable().Reverse());
        await context.SaveChangesAsync();
        var store = Store(context);

        foreach (var expected in records.Where((_, index) => index % 2 == 1))
        {
            var due = Assert.Single(await store.GetDueAsync(_now, 1, default).WaitAsync(TimeSpan.FromSeconds(10)));
            Assert.Equal(expected.Id, due.Id);
            await store.CompleteAsync(due.Id, default);
        }
        Assert.Empty(await store.GetDueAsync(_now, 1, default).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(records.Where((_, index) => index % 2 == 0).Select(r => r.Id),
            await context.Set<SagaTimeoutRecord>().OrderBy(r => r.Id).Select(r => r.Id).ToListAsync());
    }

    [Fact]
    public async Task Former_name_still_resolves_after_an_unresolved_row()
    {
        var invalid = Record(1, false, _now.AddMinutes(-1));
        var renamed = Record(2, true, _now);
        renamed.EventType = "Shop.Holds.HoldTimedOut, Shop.Holds";
        await using var context = CreateContext();
        context.AddRange(invalid, renamed);
        await context.SaveChangesAsync();

        var due = Assert.Single(await Store(context).GetDueAsync(_now, 1, default).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(renamed.Id, due.Id);
        Assert.Equal(new HoldLapsed(renamed.CorrelationId, "expiry-2"), due.Event);
    }

    [Fact]
    public async Task Resolved_non_event_and_null_payloads_do_not_consume_valid_capacity()
    {
        var nonEvent = Record(1, true, _now.AddMinutes(-2));
        nonEvent.EventType = SagaEventTypeNames.GetName(typeof(string));
        nonEvent.Payload = "\"not an event\"";
        var nullEvent = Record(2, true, _now.AddMinutes(-1));
        nullEvent.Payload = "null";
        var valid = Record(3, true, _now);
        await using var context = CreateContext();
        context.AddRange(nonEvent, nullEvent, valid);
        await context.SaveChangesAsync();

        Assert.Equal(valid.Id, Assert.Single(await Store(context).GetDueAsync(_now, 1, default)
            .WaitAsync(TimeSpan.FromSeconds(10))).Id);
        Assert.Equal(3, await context.Set<SagaTimeoutRecord>().CountAsync());
    }

    [Fact]
    public async Task Malformed_json_still_throws_without_deleting_records()
    {
        var record = Record(1, true, _now);
        record.Payload = "{";
        await using var context = CreateContext();
        context.Add(record);
        await context.SaveChangesAsync();

        await Assert.ThrowsAsync<JsonException>(() => Store(context).GetDueAsync(_now, 1, default));
        AssertRecord(record, await context.Set<SagaTimeoutRecord>().AsNoTracking().SingleAsync());
    }

    private static void AssertRecord(SagaTimeoutRecord expected, SagaTimeoutRecord actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.CorrelationId, actual.CorrelationId);
        Assert.Equal(expected.Key, actual.Key);
        Assert.Equal(expected.DueAtUtcTicks, actual.DueAtUtcTicks);
        Assert.Equal(expected.EventType, actual.EventType);
        Assert.Equal(expected.Payload, actual.Payload);
    }

    public void Dispose() => _connection.Dispose();
}
