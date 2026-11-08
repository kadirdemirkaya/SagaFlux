using System.Data.Common;
using System.Diagnostics.Metrics;
using System.Text.Json;
using EfCore.Repository.Concretes;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SagaStoreMetricsCollection
{
    public const string Name = "SagaStoreMetrics";
}

[Collection(SagaStoreMetricsCollection.Name)]
public sealed class SagaStoreMetricsTests : IDisposable
{
    private const string Outbox = "outbox";
    private const string Timeout = "timeout";
    private const string StoreMeter = "EventFlux.Saga.EfCore";
    private const string CoreMeter = "EventFlux.Saga";
    private const string StoreInstrument = "sagaflux.store.unresolved_row.observations";
    private static readonly DateTimeOffset Now = new(2026, 11, 8, 9, 0, 0, TimeSpan.Zero);

    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly List<(LogLevel Level, string Message)> _logs = [];

    public SagaStoreMetricsTests()
    {
        _connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    public static TheoryData<string> Kinds => new() { Outbox, Timeout };

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Each_skip_cause_is_observed_on_every_read_that_encounters_it(string kind)
    {
        var rows = Rows("xnzvf");
        await SeedAsync(kind, rows);
        using var capture = new MetricCapture(StoreMeter);

        var first = await ReadAsync(kind, 10);
        Assert.Equal(new[] { rows[3].Id, rows[4].Id }, first);
        Assert.Equal(Enumerable.Repeat($"{StoreInstrument} {kind}", 3), capture.Observations);
        Assert.Equal(3, _logs.Count(l => l.Level == LogLevel.Warning));

        var second = await ReadAsync(kind, 10);
        Assert.Equal(first, second);
        Assert.Equal(Enumerable.Repeat($"{StoreInstrument} {kind}", 6), capture.Observations);
        Assert.Equal(6, _logs.Count(l => l.Level == LogLevel.Warning));
        await AssertRowsAsync(kind, rows);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Malformed_json_still_throws_and_only_rows_skipped_before_it_are_observed(string kind)
    {
        var rows = Rows("xxnmv");
        await SeedAsync(kind, rows);
        using var capture = new MetricCapture(StoreMeter);

        await Assert.ThrowsAsync<JsonException>(() => ReadAsync(kind, 10));

        Assert.Equal(Enumerable.Repeat($"{StoreInstrument} {kind}", 3), capture.Observations);
        Assert.Equal(3, _logs.Count(l => l.Level == LogLevel.Warning));
        await AssertRowsAsync(kind, rows);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Bad_rows_across_pages_count_actual_encounters_and_listening_adds_no_queries(string kind)
    {
        var rows = Rows("xxxvxxvx");
        await SeedAsync(kind, rows);

        var quietCommands = new List<string>();
        var quiet = await ReadAsync(kind, 2, quietCommands);
        Assert.Equal(new[] { rows[3].Id, rows[6].Id }, quiet);

        using var capture = new MetricCapture(StoreMeter);
        var listenedCommands = new List<string>();
        var listened = await ReadAsync(kind, 2, listenedCommands);

        Assert.Equal(quiet, listened);
        Assert.Equal(quietCommands, listenedCommands);
        Assert.Equal(6, listenedCommands.Count);
        Assert.Equal(Enumerable.Repeat($"{StoreInstrument} {kind}", 5), capture.Observations);
        await AssertRowsAsync(kind, rows);
    }

    [Fact]
    public async Task Only_the_store_counter_is_published_and_two_listeners_share_it()
    {
        await SeedAsync(Outbox, Rows("xv"));
        await SeedAsync(Timeout, Rows("zv"));
        using var first = new MetricCapture(StoreMeter);
        using var second = new MetricCapture(StoreMeter);

        await ReadAsync(Outbox, 10);
        await ReadAsync(Timeout, 10);

        var expected = new[] { $"{StoreInstrument} {Outbox}", $"{StoreInstrument} {Timeout}" };
        Assert.Equal(expected, first.Observations);
        Assert.Equal(expected, second.Observations);
        var instrument = Assert.Single(first.Published);
        Assert.Same(instrument, Assert.Single(second.Published));
        Assert.IsType<Counter<long>>(instrument);
        Assert.Equal(StoreInstrument, instrument.Name);
        Assert.Equal("{observation}", instrument.Unit);
        Assert.Equal(typeof(EfCoreSagaOutbox).Assembly.GetName().Version?.ToString(), instrument.Meter.Version);
        Assert.StartsWith("Unresolvable or unusable stored-event row encounters", instrument.Description);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Counter_stays_disabled_without_an_enabled_listener(string kind)
    {
        var rows = Rows("xv");
        await SeedAsync(kind, rows);
        using var observer = new MetricCapture(StoreMeter, enable: false);

        Assert.Equal(new[] { rows[1].Id }, await ReadAsync(kind, 10));

        var instrument = Assert.Single(observer.Published);
        Assert.False(instrument.Enabled);
        Assert.Empty(observer.Observations);
        Assert.Single(_logs, l => l.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task Store_and_dispatcher_subscriptions_are_independent()
    {
        await SeedAsync(Outbox, Rows("x"));
        using var core = new MetricCapture(CoreMeter);
        using var store = new MetricCapture(StoreMeter);

        Assert.Empty(await ReadAsync(Outbox, 10));
        Assert.Equal(new[] { $"{StoreInstrument} {Outbox}" }, store.Observations);
        Assert.Empty(core.Observations);

        using var other = new SqliteConnection("DataSource=:memory:");
        other.Open();
        await using (var context = new MetricsDbContext(new DbContextOptionsBuilder<MetricsDbContext>().UseSqlite(other).Options))
        {
            context.Database.EnsureCreated();
            context.AddRange(OutboxRecords(Rows("v")));
            await context.SaveChangesAsync();
        }
        var transport = new RecordingTransport();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(Now.AddHours(1)));
        services.AddDbContext<MetricsDbContext>(o => o.UseSqlite(other));
        services.AddSagaFlux(o => o.EnableOutboxPolling = false).UseEfCoreOutbox<MetricsDbContext>();
        services.AddSingleton<ISagaTransport>(transport);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        Assert.Equal(1, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());

        Assert.Single(transport.Sent);
        Assert.Equal(new[]
        {
            "sagaflux.dispatcher.dispatch.operations outbox success",
            "sagaflux.dispatcher.completion.operations outbox success",
        }, core.Observations);
        Assert.Equal(new[] { $"{StoreInstrument} {Outbox}" }, store.Observations);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Throwing_store_listener_changes_neither_results_nor_warnings(string kind)
    {
        var rows = Rows("xnvzv");
        await SeedAsync(kind, rows);

        var quiet = await ReadAsync(kind, 10);
        var quietLogs = _logs.ToArray();
        _logs.Clear();

        var callbacks = 0;
        using var capture = new MetricCapture(StoreMeter, onMeasurement: () =>
        {
            callbacks++;
            throw new InvalidOperationException("listener failed");
        });
        var listened = await ReadAsync(kind, 10);

        Assert.Equal(new[] { rows[2].Id, rows[4].Id }, listened);
        Assert.Equal(quiet, listened);
        Assert.Equal(quietLogs, _logs);
        Assert.Equal(3, callbacks);
        Assert.Equal(3, capture.Observations.Count);
        await AssertRowsAsync(kind, rows);
    }

    public void Dispose() => _connection.Dispose();

    private MetricsDbContext CreateContext(List<string>? commands = null)
    {
        var builder = new DbContextOptionsBuilder<MetricsDbContext>().UseSqlite(_connection);
        if (commands is not null)
            builder.AddInterceptors(new CommandRecorder(commands));
        return new MetricsDbContext(builder.Options);
    }

    private static (Guid Id, string EventType, string Payload, long Ticks)[] Rows(string pattern)
        => pattern.Select((kind, index) =>
        {
            var id = Guid.Parse($"00000000-0000-0000-0000-{index + 1:D12}");
            var ticks = Now.AddSeconds(index - 1000).UtcTicks;
            return kind switch
            {
                'x' => (id, "Gone.Event, Gone", "{}", ticks),
                'n' => (id, SagaEventTypeNames.GetName(typeof(string)), "\"not an event\"", ticks),
                'z' => (id, SagaEventTypeNames.GetName(typeof(HoldExpired)), "null", ticks),
                'm' => (id, SagaEventTypeNames.GetName(typeof(HoldExpired)), "{", ticks),
                'f' => (id, "Shop.Holds.HoldTimedOut, Shop.Holds", JsonSerializer.Serialize(new HoldLapsed(id, "former")), ticks),
                _ => (id, SagaEventTypeNames.GetName(typeof(HoldExpired)), JsonSerializer.Serialize(new HoldExpired(id, "valid")), ticks),
            };
        }).ToArray();

    private static IEnumerable<SagaOutboxRecord> OutboxRecords(IEnumerable<(Guid Id, string EventType, string Payload, long Ticks)> rows)
        => rows.Select(r => new SagaOutboxRecord { Id = r.Id, EventType = r.EventType, Payload = r.Payload, StagedAtUtcTicks = r.Ticks });

    private static IEnumerable<SagaTimeoutRecord> TimeoutRecords(IEnumerable<(Guid Id, string EventType, string Payload, long Ticks)> rows)
        => rows.Select(r => new SagaTimeoutRecord
        {
            Id = r.Id, CorrelationId = r.Id, Key = $"key-{r.Id}", DueAtUtcTicks = r.Ticks, EventType = r.EventType, Payload = r.Payload,
        });

    private async Task SeedAsync(string kind, (Guid Id, string EventType, string Payload, long Ticks)[] rows)
    {
        await using var context = CreateContext();
        if (kind == Outbox)
            context.AddRange(OutboxRecords(rows.AsEnumerable().Reverse()));
        else
            context.AddRange(TimeoutRecords(rows.AsEnumerable().Reverse()));
        await context.SaveChangesAsync();
    }

    private async Task<Guid[]> ReadAsync(string kind, int maxCount, List<string>? commands = null)
    {
        await using var context = CreateContext(commands);
        if (kind == Outbox)
        {
            var outbox = new EfCoreSagaOutbox(new ReadRepository<SagaOutboxRecord>(context, null!),
                new WriteRepository<SagaOutboxRecord>(context, null!), new RecordingLogger<EfCoreSagaOutbox>(_logs));
            return (await outbox.GetPendingAsync(Now, maxCount, CancellationToken.None)).Select(m => m.Id).ToArray();
        }

        var store = new EfCoreSagaTimeoutStore(new ReadRepository<SagaTimeoutRecord>(context, null!),
            new WriteRepository<SagaTimeoutRecord>(context, null!), new RecordingLogger<EfCoreSagaTimeoutStore>(_logs));
        return (await store.GetDueAsync(Now, maxCount, CancellationToken.None)).Select(t => t.Id).ToArray();
    }

    private async Task AssertRowsAsync(string kind, (Guid Id, string EventType, string Payload, long Ticks)[] rows)
    {
        await using var context = CreateContext();
        var stored = kind == Outbox
            ? (await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync())
                .Select(r => (r.Id, r.EventType, r.Payload, r.StagedAtUtcTicks)).ToList()
            : (await context.Set<SagaTimeoutRecord>().AsNoTracking().ToListAsync())
                .Select(r => (r.Id, r.EventType, r.Payload, r.DueAtUtcTicks)).ToList();
        Assert.Equal(rows.OrderBy(r => r.Id), stored.OrderBy(r => r.Id));
    }

    private sealed class MetricsDbContext(DbContextOptions<MetricsDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureSagaOutbox();
            modelBuilder.ConfigureSagaTimeouts();
        }
    }

    private sealed class CommandRecorder(List<string> commands) : DbCommandInterceptor
    {
        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData,
            InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            commands.Add(command.CommandText);
            return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData,
            InterceptionResult<object> result, CancellationToken cancellationToken = default)
        {
            commands.Add(command.CommandText);
            return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData,
            InterceptionResult<DbDataReader> result)
        {
            commands.Add(command.CommandText);
            return base.ReaderExecuting(command, eventData, result);
        }
    }

    private sealed class RecordingTransport : ISagaTransport
    {
        public List<object> Sent { get; } = [];

        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            Sent.Add(@event);
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingLogger<T>(List<(LogLevel Level, string Message)> logs) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => logs.Add((logLevel, formatter(state, exception)));
    }

    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly object _gate = new();
        private readonly List<string> _observations = [];
        private readonly List<Instrument> _published = [];

        public MetricCapture(string meterName, bool enable = true, Action? onMeasurement = null)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name != meterName) return;
                lock (_gate) _published.Add(instrument);
                if (enable) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            {
                var observed = Describe(instrument, value, tags);
                lock (_gate) _observations.Add(observed);
                onMeasurement?.Invoke();
            });
            _listener.Start();
        }

        public IReadOnlyList<string> Observations
        {
            get { lock (_gate) return _observations.ToArray(); }
        }

        public IReadOnlyList<Instrument> Published
        {
            get { lock (_gate) return _published.ToArray(); }
        }

        public void Dispose()
        {
            foreach (var instrument in Published)
                _listener.DisableMeasurementEvents(instrument);
            _listener.Dispose();
        }

        private static string Describe(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var pairs = tags.ToArray();
            if (value != 1)
                return $"invalid value {value}";
            if (instrument.Name == StoreInstrument)
                return pairs.Length == 1 && pairs[0].Key == "kind" && pairs[0].Value is Outbox or Timeout && instrument.Unit == "{observation}"
                    ? $"{instrument.Name} {pairs[0].Value}"
                    : $"invalid {string.Join(",", pairs.Select(p => $"{p.Key}={p.Value}"))}";
            return pairs.Length == 2 && pairs[0].Key == "kind" && pairs[1].Key == "outcome" && instrument.Unit == "{operation}"
                ? $"{instrument.Name} {pairs[0].Value} {pairs[1].Value}"
                : $"invalid {string.Join(",", pairs.Select(p => $"{p.Key}={p.Value}"))}";
        }
    }
}
