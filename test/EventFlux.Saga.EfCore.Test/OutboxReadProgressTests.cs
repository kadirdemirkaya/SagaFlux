using System.Text.Json;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed class OutboxReadProgressTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 9, 27, 12, 0, 0, TimeSpan.Zero));
    private readonly SwitchableTransport _transport = new();

    public OutboxReadProgressTests()
    {
        _connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    private OutboxDbContext CreateContext() => new(new DbContextOptionsBuilder<OutboxDbContext>().UseSqlite(_connection).Options);

    private ServiceProvider Build(int batchSize = 1)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(_time);
        services.AddDbContext<OutboxDbContext>(o => o.UseSqlite(_connection));
        services.AddSagaFlux(o =>
        {
            o.EnableOutboxPolling = false;
            o.OutboxBatchSize = batchSize;
        }).UseEfCoreOutbox<OutboxDbContext>();
        services.AddSingleton<ISagaTransport>(_transport);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private SagaOutboxRecord Record(int secondsAgo, bool resolvable = true) => new()
    {
        Id = Guid.NewGuid(),
        EventType = resolvable ? SagaEventTypeNames.GetName(typeof(EmailInvoice)) : "Gone.Event, Gone",
        Payload = resolvable ? JsonSerializer.Serialize(new EmailInvoice(Guid.NewGuid())) : "{}",
        StagedAtUtcTicks = _time.GetUtcNow().AddSeconds(-secondsAgo).UtcTicks,
    };

    private async Task SeedAsync(params SagaOutboxRecord[] records)
    {
        await using var context = CreateContext();
        context.AddRange(records);
        await context.SaveChangesAsync();
    }

    private async Task AssertStoredAsync(params SagaOutboxRecord[] expected)
    {
        await using var context = CreateContext();
        var stored = await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync();
        Assert.Equal(expected.Length, stored.Count);
        foreach (var record in expected)
        {
            var actual = Assert.Single(stored, r => r.Id == record.Id);
            Assert.Equal(record.EventType, actual.EventType);
            Assert.Equal(record.Payload, actual.Payload);
            Assert.Equal(record.StagedAtUtcTicks, actual.StagedAtUtcTicks);
        }
    }

    private async Task<IReadOnlyList<SagaOutboxMessage>> ReadAsync(ServiceProvider provider, int maxCount)
    {
        await using var scope = provider.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<ISagaOutbox>()
            .GetPendingAsync(_time.GetUtcNow().AddSeconds(-30), maxCount, CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task An_unresolvable_oldest_row_does_not_block_reading_or_dispatching_the_next_message()
    {
        var unknown = Record(90, false);
        var valid = Record(60);
        await SeedAsync(unknown, valid);
        await using var provider = Build();

        var message = Assert.Single(await ReadAsync(provider, 1));
        Assert.Equal(valid.Id, message.Id);
        Assert.Equal(JsonSerializer.Deserialize<EmailInvoice>(valid.Payload), Assert.IsType<EmailInvoice>(message.Event));
        await AssertStoredAsync(unknown, valid);

        Assert.Equal(1, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
        Assert.Equal(JsonSerializer.Deserialize<EmailInvoice>(valid.Payload), Assert.Single(_transport.Sent));
        await AssertStoredAsync(unknown);
        Assert.Equal(0, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
    }

    [Theory]
    [InlineData("xxvvxvv", 2)]
    [InlineData("xxxvxvxv", 1)]
    [InlineData("xvxxx", 3)]
    [InlineData("xxxx", 2)]
    [InlineData("xxx", 2)]
    [InlineData("", 1)]
    [InlineData("vvvv", 2)]
    [InlineData("xv", 0)]
    [InlineData("xv", -1)]
    public async Task Reading_returns_only_the_earliest_valid_messages_up_to_the_limit(string pattern, int maxCount)
    {
        var records = pattern.Select((kind, index) => Record(120 - index, kind == 'v')).ToArray();
        await SeedAsync(records.AsEnumerable().Reverse().ToArray());
        await using var provider = Build();

        var messages = await ReadAsync(provider, maxCount);

        Assert.Equal(records.Where((_, index) => pattern[index] == 'v').Take(Math.Max(0, maxCount)).Select(r => r.Id),
            messages.Select(m => m.Id));
        foreach (var message in messages)
        {
            var record = Assert.Single(records, r => r.Id == message.Id);
            Assert.Equal(new DateTimeOffset(record.StagedAtUtcTicks, TimeSpan.Zero), message.StagedAt);
            Assert.Equal(JsonSerializer.Deserialize<EmailInvoice>(record.Payload), Assert.IsType<EmailInvoice>(message.Event));
        }
        await AssertStoredAsync(records);
    }

    [Fact]
    public async Task Equal_staging_times_do_not_lose_messages_between_pages()
    {
        var records = Enumerable.Range(1, 6).Select(index =>
        {
            var record = Record(60, index % 2 == 0);
            record.Id = Guid.Parse($"00000000-0000-0000-0000-{index:D12}");
            return record;
        }).ToArray();
        await SeedAsync(records.AsEnumerable().Reverse().ToArray());
        await using var provider = Build(2);

        Assert.Equal(new[] { records[1].Id, records[3].Id }, (await ReadAsync(provider, 2)).Select(m => m.Id));
        Assert.Equal(2, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
        Assert.Equal(1, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
        Assert.Equal(0, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
        Assert.Equal(records.Where((_, index) => index % 2 == 1).Select(r => JsonSerializer.Deserialize<EmailInvoice>(r.Payload)),
            _transport.Sent.Cast<EmailInvoice>());
        await AssertStoredAsync(records.Where((_, index) => index % 2 == 0).ToArray());
    }

    [Fact]
    public async Task Reading_and_dispatching_exclude_rows_at_or_after_the_cutoff()
    {
        var unknown = Record(120, false);
        var valid = Record(31);
        var boundary = Record(30);
        var young = Record(29);
        var future = Record(-60);
        await SeedAsync(future, young, boundary, valid, unknown);
        await using var provider = Build(2);

        Assert.Equal(valid.Id, Assert.Single(await ReadAsync(provider, 2)).Id);
        Assert.Equal(1, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());
        Assert.Equal(JsonSerializer.Deserialize<EmailInvoice>(valid.Payload), Assert.Single(_transport.Sent));
        await AssertStoredAsync(unknown, boundary, young, future);
    }

    [Fact]
    public async Task Resolvable_non_events_and_null_payloads_are_skipped_without_changing_rows()
    {
        var nonEvent = Record(120);
        nonEvent.EventType = SagaEventTypeNames.GetName(typeof(string));
        nonEvent.Payload = "\"not an event\"";
        var nullEvent = Record(90);
        nullEvent.Payload = "null";
        var valid = Record(60);
        await SeedAsync(nonEvent, nullEvent, valid);
        await using var provider = Build();

        Assert.Equal(valid.Id, Assert.Single(await ReadAsync(provider, 1)).Id);
        await AssertStoredAsync(nonEvent, nullEvent, valid);
    }

    [Fact]
    public async Task Malformed_json_for_a_resolved_event_still_throws_without_changing_rows()
    {
        var invalid = Record(90);
        invalid.Payload = "{";
        var valid = Record(60);
        await SeedAsync(invalid, valid);
        await using var provider = Build();

        await Assert.ThrowsAsync<JsonException>(() => ReadAsync(provider, 1));
        await AssertStoredAsync(invalid, valid);
        Assert.Empty(_transport.Sent);
    }

    public void Dispose() => _connection.Dispose();
}
