using System.Diagnostics;
using System.Diagnostics.Metrics;
using EventFlux.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.Test;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SagaOperationalMetricsCollection
{
    public const string Name = "SagaOperationalMetrics";
}

[Collection(SagaOperationalMetricsCollection.Name)]
public sealed class SagaOperationalMetricsTests
{
    private const string Outbox = "outbox";
    private const string Timeout = "timeout";
    private static readonly DateTimeOffset Now = new(2026, 11, 8, 9, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMinutes(5);

    public enum Phase { Dispatch, Completion }

    public enum Scenario
    {
        CallerCancelledOperationCanceled,
        UnrelatedOperationCanceledWithCallerCancelled,
        OperationCanceledWithCallerUncancelled,
        OrdinaryErrorWithCallerCancelled,
        NormalReturnAfterCancellation,
    }

    public static TheoryData<string> Kinds => new() { Outbox, Timeout };

    public static TheoryData<string, Phase, Scenario> CancellationCases
    {
        get
        {
            var data = new TheoryData<string, Phase, Scenario>();
            foreach (var kind in new[] { Outbox, Timeout })
                foreach (var phase in Enum.GetValues<Phase>())
                    foreach (var scenario in Enum.GetValues<Scenario>())
                        data.Add(kind, phase, scenario);
            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Successful_dispatch_and_completion_are_observed_once_per_item(string kind)
    {
        await using var harness = Harness.Create(kind, 2);
        using var capture = new MetricCapture();

        Assert.Equal(2, await harness.DispatchAsync(CancellationToken.None));

        Assert.Equal(new[] { "read", "dispatch:0", "complete:0", "dispatch:1", "complete:1" }, harness.Calls);
        Assert.Equal(new[]
        {
            Obs("dispatch", kind, "success"), Obs("completion", kind, "success"),
            Obs("dispatch", kind, "success"), Obs("completion", kind, "success"),
        }, capture.Observations);
        Assert.Empty(harness.Logs);
        Assert.Empty(harness.Retries);
        Assert.Equal(kind == Outbox ? 1 : 3, harness.Disposals);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Dispatch_failure_is_observed_without_a_completion_observation(string kind)
    {
        await using var harness = Harness.Create(kind, 2);
        var error = new InvalidOperationException("dispatch failed");
        harness.OnDispatch = (index, _) => index == 0 ? Task.FromException(error) : Task.CompletedTask;
        using var capture = new MetricCapture();

        var delivered = await harness.DispatchAsync(CancellationToken.None);

        var log = Assert.Single(harness.Logs);
        Assert.Equal(LogLevel.Error, log.Level);
        Assert.Same(error, log.Error);
        if (kind == Outbox)
        {
            Assert.Equal(0, delivered);
            Assert.Equal(new[] { "read", "dispatch:0" }, harness.Calls);
            Assert.Equal(new[] { Obs("dispatch", kind, "failure") }, capture.Observations);
            Assert.Empty(harness.Retries);
            Assert.Equal(1, harness.Disposals);
        }
        else
        {
            Assert.Equal(1, delivered);
            Assert.Equal(new[] { "read", "dispatch:0", "schedule:0", "dispatch:1", "complete:1" }, harness.Calls);
            Assert.Equal(new[]
            {
                Obs("dispatch", kind, "failure"),
                Obs("dispatch", kind, "success"), Obs("completion", kind, "success"),
            }, capture.Observations);
            harness.AssertSingleRetryOf(0);
            Assert.Equal(3, harness.Disposals);
        }
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Completion_failure_after_dispatch_success_is_not_a_dispatch_failure_and_a_redelivery_is_observed_again(string kind)
    {
        await using var harness = Harness.Create(kind, 2);
        var error = new InvalidOperationException("completion failed");
        harness.OnComplete = (index, _) => index == 0 ? Task.FromException(error) : Task.CompletedTask;
        using var capture = new MetricCapture();

        if (kind == Outbox)
        {
            Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => harness.DispatchAsync(CancellationToken.None)));
            Assert.Equal(new[] { "read", "dispatch:0", "complete:0" }, harness.Calls);
            Assert.Equal(new[] { Obs("dispatch", kind, "success"), Obs("completion", kind, "failure") }, capture.Observations);
            Assert.Empty(harness.Logs);
            Assert.Equal(1, harness.Disposals);
        }
        else
        {
            Assert.Equal(1, await harness.DispatchAsync(CancellationToken.None));
            Assert.Equal(new[] { "read", "dispatch:0", "complete:0", "schedule:0", "dispatch:1", "complete:1" }, harness.Calls);
            Assert.Equal(new[]
            {
                Obs("dispatch", kind, "success"), Obs("completion", kind, "failure"),
                Obs("dispatch", kind, "success"), Obs("completion", kind, "success"),
            }, capture.Observations);
            Assert.Same(error, Assert.Single(harness.Logs).Error);
            harness.AssertSingleRetryOf(0);
            Assert.Equal(3, harness.Disposals);
            harness.Time.Advance(RetryDelay);
        }

        harness.OnComplete = null;
        harness.Calls.Clear();
        capture.Clear();

        Assert.Equal(kind == Outbox ? 2 : 1, await harness.DispatchAsync(CancellationToken.None));

        Assert.Equal(kind == Outbox
                ? new[] { "read", "dispatch:0", "complete:0", "dispatch:1", "complete:1" }
                : new[] { "read", "dispatch:0", "complete:0" },
            harness.Calls);
        Assert.Equal(Obs("dispatch", kind, "success"), capture.Observations[0]);
        Assert.Equal(kind == Outbox ? 4 : 2, capture.Observations.Count);
        Assert.All(capture.Observations, o => Assert.EndsWith(" success", o));
    }

    [Fact]
    public async Task Retry_schedule_failure_adds_no_phase_observation()
    {
        await using var harness = Harness.Create(Timeout, 2);
        var routeError = new InvalidOperationException("route failed");
        var scheduleError = new InvalidOperationException("schedule failed");
        harness.OnDispatch = (_, _) => Task.FromException(routeError);
        harness.ScheduleError = scheduleError;
        using var capture = new MetricCapture();

        Assert.Same(scheduleError, await Assert.ThrowsAsync<InvalidOperationException>(() => harness.DispatchAsync(CancellationToken.None)));

        Assert.Equal(new[] { "read", "dispatch:0", "schedule:0" }, harness.Calls);
        Assert.Equal(new[] { Obs("dispatch", Timeout, "failure") }, capture.Observations);
        Assert.Same(routeError, Assert.Single(harness.Logs).Error);
        Assert.Equal(2, harness.Disposals);
    }

    [Theory]
    [MemberData(nameof(CancellationCases))]
    public async Task Phase_outcome_follows_the_existing_cancellation_filter(string kind, Phase phase, Scenario scenario)
    {
        await using var harness = Harness.Create(kind, 1);
        using var caller = new CancellationTokenSource();
        using var unrelated = new CancellationTokenSource();
        Exception? thrown = scenario switch
        {
            Scenario.CallerCancelledOperationCanceled => new OperationCanceledException(caller.Token),
            Scenario.UnrelatedOperationCanceledWithCallerCancelled => new OperationCanceledException(unrelated.Token),
            Scenario.OperationCanceledWithCallerUncancelled => new OperationCanceledException(),
            Scenario.OrdinaryErrorWithCallerCancelled => new InvalidOperationException("ordinary"),
            _ => null,
        };
        Func<int, CancellationToken, Task> behaviour = (_, _) =>
        {
            if (scenario != Scenario.OperationCanceledWithCallerUncancelled)
                caller.Cancel();
            return thrown is null ? Task.CompletedTask : Task.FromException(thrown);
        };
        if (phase == Phase.Dispatch)
            harness.OnDispatch = behaviour;
        else
            harness.OnComplete = behaviour;
        using var capture = new MetricCapture();

        var cancelledOutcome = scenario is Scenario.CallerCancelledOperationCanceled or Scenario.UnrelatedOperationCanceledWithCallerCancelled;
        var outcome = thrown is null ? "success" : cancelledOutcome ? "cancelled" : "failure";
        var propagates = thrown is not null && (cancelledOutcome || (kind == Outbox && phase == Phase.Completion));
        var caught = thrown is not null && !propagates;

        int? delivered = null;
        if (propagates)
            Assert.Same(thrown, await Assert.ThrowsAnyAsync<Exception>(() => harness.DispatchAsync(caller.Token)));
        else
            delivered = await harness.DispatchAsync(caller.Token);

        var expected = new List<string> { Obs("dispatch", kind, phase == Phase.Dispatch ? outcome : "success") };
        if (phase == Phase.Completion)
            expected.Add(Obs("completion", kind, outcome));
        else if (thrown is null)
            expected.Add(Obs("completion", kind, "success"));
        Assert.Equal(expected, capture.Observations);

        var calls = new List<string> { "read", "dispatch:0" };
        if (phase == Phase.Completion || thrown is null)
            calls.Add("complete:0");
        if (caught && kind == Timeout)
            calls.Add("schedule:0");
        Assert.Equal(calls, harness.Calls);

        if (!propagates)
            Assert.Equal(thrown is null ? 1 : 0, delivered);
        if (caught)
            Assert.Same(thrown, Assert.Single(harness.Logs).Error);
        else
            Assert.Empty(harness.Logs);
        if (caught && kind == Timeout)
            harness.AssertSingleRetryOf(0);
        else
            Assert.Empty(harness.Retries);
        Assert.Equal(kind == Outbox ? 1 : 2, harness.Disposals);
    }

    [Theory]
    [MemberData(nameof(Kinds))]
    public async Task Work_outside_the_phases_adds_no_observation(string kind)
    {
        using var capture = new MetricCapture();

        await using (var preCancelled = Harness.Create(kind, 1))
        {
            using var caller = new CancellationTokenSource();
            caller.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => preCancelled.DispatchAsync(caller.Token));
            Assert.Equal(new[] { "read" }, preCancelled.Calls);
        }

        await using (var cancelledAfterRead = Harness.Create(kind, 1))
        {
            using var caller = new CancellationTokenSource();
            cancelledAfterRead.AfterRead = caller.Cancel;
            var error = await Assert.ThrowsAsync<OperationCanceledException>(() => cancelledAfterRead.DispatchAsync(caller.Token));
            Assert.Equal(caller.Token, error.CancellationToken);
            Assert.Equal(new[] { "read" }, cancelledAfterRead.Calls);
        }

        await using (var empty = Harness.Create(kind, 0))
            Assert.Equal(0, await empty.DispatchAsync(CancellationToken.None));

        await using (var readFailure = Harness.Create(kind, 1))
        {
            var error = new InvalidOperationException("query failed");
            readFailure.ReadError = error;
            Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => readFailure.DispatchAsync(CancellationToken.None)));
        }

        await using (var resolutionFailure = Harness.Create(kind, 1, failResolution: true))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => resolutionFailure.DispatchAsync(CancellationToken.None));
            Assert.DoesNotContain("dispatch:0", resolutionFailure.Calls);
        }

        await using (var disposalFailure = Harness.Create(kind, 0))
        {
            var error = new InvalidOperationException("disposal failed");
            disposalFailure.DisposeError = error;
            Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => disposalFailure.DispatchAsync(CancellationToken.None)));
        }

        Assert.Empty(capture.Observations);
    }

    [Fact]
    public async Task Only_the_two_documented_counters_are_published_with_fixed_unit_and_version()
    {
        using var capture = new MetricCapture();
        await using (var outbox = Harness.Create(Outbox, 1))
            await outbox.DispatchAsync(CancellationToken.None);
        await using (var timeout = Harness.Create(Timeout, 1))
            await timeout.DispatchAsync(CancellationToken.None);

        var published = capture.Published;
        Assert.Equal(new[] { "sagaflux.dispatcher.completion.operations", "sagaflux.dispatcher.dispatch.operations" },
            published.Select(i => i.Name).Order());
        Assert.All(published, instrument =>
        {
            Assert.IsType<Counter<long>>(instrument);
            Assert.Equal("{operation}", instrument.Unit);
            Assert.Equal(typeof(ISagaOutbox).Assembly.GetName().Version?.ToString(), instrument.Meter.Version);
            Assert.Contains("dispatcher", instrument.Description);
        });
        Assert.Equal(4, capture.Observations.Count);
    }

    [Fact]
    public async Task Counters_stay_disabled_without_an_enabled_listener_even_when_tracing_listens()
    {
        using var tracing = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SagaFluxDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(tracing);
        using var observer = new MetricCapture(enable: false);

        await using (var outbox = Harness.Create(Outbox, 1))
            Assert.Equal(1, await outbox.DispatchAsync(CancellationToken.None));
        await using (var timeout = Harness.Create(Timeout, 1))
            Assert.Equal(1, await timeout.DispatchAsync(CancellationToken.None));

        Assert.Equal(2, observer.Published.Count);
        Assert.All(observer.Published, instrument => Assert.False(instrument.Enabled));
        Assert.Empty(observer.Observations);
        Assert.True(SagaFluxDiagnostics.Source.HasListeners());
    }

    [Fact]
    public async Task Metric_listening_does_not_turn_on_tracing()
    {
        using var capture = new MetricCapture();
        await using var harness = Harness.Create(Timeout, 1);

        Assert.Equal(1, await harness.DispatchAsync(CancellationToken.None));

        Assert.Equal(2, capture.Observations.Count);
        Assert.False(SagaFluxDiagnostics.Source.HasListeners());
    }

    [Fact]
    public async Task Late_attach_dispose_and_reattach_observe_only_work_done_while_listening()
    {
        await using var harness = Harness.Create(Outbox, 3);
        harness.KeepPending = true;

        await harness.DispatchAsync(CancellationToken.None);
        var counts = new List<int>();
        var first = new MetricCapture();
        try
        {
            counts.Add(first.Observations.Count);
            await harness.DispatchAsync(CancellationToken.None);
            counts.Add(first.Observations.Count);
        }
        finally
        {
            first.Dispose();
        }

        await harness.DispatchAsync(CancellationToken.None);
        counts.Add(first.Observations.Count);

        using var second = new MetricCapture();
        counts.Add(second.Observations.Count);
        await harness.DispatchAsync(CancellationToken.None);
        counts.Add(second.Observations.Count);
        counts.Add(first.Observations.Count);

        Assert.Equal(new[] { 0, 6, 6, 0, 6, 6 }, counts);
    }

    [Fact]
    public async Task Two_listeners_and_two_providers_share_one_instrument_per_name()
    {
        using var first = new MetricCapture();
        using var second = new MetricCapture();
        await using var one = Harness.Create(Outbox, 1);
        await using var two = Harness.Create(Outbox, 2);

        Assert.Equal(1, await one.DispatchAsync(CancellationToken.None));
        Assert.Equal(2, await two.DispatchAsync(CancellationToken.None));

        Assert.Equal(6, first.Observations.Count);
        Assert.Equal(first.Observations, second.Observations);
        Assert.Equal(2, first.Published.Count);
        Assert.Equal(2, first.Published.Select(i => i.Name).Distinct().Count());
        Assert.Equal(first.Published.OrderBy(i => i.Name).ToArray(), second.Published.OrderBy(i => i.Name).ToArray());
    }

    [Fact]
    public async Task Throwing_outbox_listener_changes_neither_result_nor_failure_identity()
    {
        var sendError = new InvalidOperationException("send failed");
        var completionError = new InvalidOperationException("completion failed");

        async Task<(int Delivered, string[] Calls, (LogLevel, Exception?)[] Logs, int Disposals, Exception? Thrown, string[] Observed)> RunAsync(bool throwing, bool failCompletion)
        {
            await using var harness = Harness.Create(Outbox, 3);
            harness.OnDispatch = (index, _) => index == 1 && !failCompletion ? Task.FromException(sendError) : Task.CompletedTask;
            harness.OnComplete = (index, _) => index == 1 && failCompletion ? Task.FromException(completionError) : Task.CompletedTask;
            var callbacks = 0;
            using var capture = new MetricCapture(onMeasurement: () =>
            {
                callbacks++;
                if (throwing) throw new InvalidOperationException("listener failed");
            });
            Exception? thrown = null;
            var delivered = -1;
            try
            {
                delivered = await harness.DispatchAsync(CancellationToken.None);
            }
            catch (Exception ex)
            {
                thrown = ex;
            }
            Assert.Equal(capture.Observations.Count, callbacks);
            return (delivered, harness.Calls.ToArray(), harness.Logs.Select(l => (l.Level, l.Error)).ToArray(), harness.Disposals, thrown,
                capture.Observations.ToArray());
        }

        foreach (var failCompletion in new[] { false, true })
        {
            var normal = await RunAsync(false, failCompletion);
            var throwing = await RunAsync(true, failCompletion);

            Assert.Equal(normal.Delivered, throwing.Delivered);
            Assert.Equal(normal.Calls, throwing.Calls);
            Assert.Equal(normal.Logs, throwing.Logs);
            Assert.Equal(normal.Disposals, throwing.Disposals);
            Assert.Same(normal.Thrown, throwing.Thrown);
            Assert.Equal(normal.Observed, throwing.Observed);
            Assert.NotEmpty(throwing.Observed);
        }
    }

    [Fact]
    public async Task Throwing_timeout_listener_changes_neither_retries_nor_completions()
    {
        var routeError = new InvalidOperationException("route failed");
        var completionError = new InvalidOperationException("completion failed");

        async Task<(int Delivered, string[] Calls, (LogLevel, Exception?)[] Logs, int Disposals, int Retries, string[] Observed)> RunAsync(bool throwing)
        {
            await using var harness = Harness.Create(Timeout, 3);
            harness.OnDispatch = (index, _) => index == 0 ? Task.FromException(routeError) : Task.CompletedTask;
            harness.OnComplete = (index, _) => index == 1 ? Task.FromException(completionError) : Task.CompletedTask;
            using var capture = new MetricCapture(onMeasurement: () =>
            {
                if (throwing) throw new InvalidOperationException("listener failed");
            });
            var delivered = await harness.DispatchAsync(CancellationToken.None);
            Assert.All(harness.Retries, retry => Assert.Equal(CancellationToken.None, retry.Token));
            return (delivered, harness.Calls.ToArray(), harness.Logs.Select(l => (l.Level, l.Error)).ToArray(), harness.Disposals,
                harness.Retries.Count, capture.Observations.ToArray());
        }

        var normal = await RunAsync(false);
        var throwing = await RunAsync(true);

        Assert.Equal(1, throwing.Delivered);
        Assert.Equal(new[] { "read", "dispatch:0", "schedule:0", "dispatch:1", "complete:1", "schedule:1", "dispatch:2", "complete:2" }, throwing.Calls);
        Assert.Equal(new[] { (LogLevel.Error, (Exception?)routeError), (LogLevel.Error, (Exception?)completionError) }, throwing.Logs);
        Assert.Equal(2, throwing.Retries);
        Assert.Equal(4, throwing.Disposals);
        Assert.Equal(new[]
        {
            Obs("dispatch", Timeout, "failure"),
            Obs("dispatch", Timeout, "success"), Obs("completion", Timeout, "failure"),
            Obs("dispatch", Timeout, "success"), Obs("completion", Timeout, "success"),
        }, throwing.Observed);
        Assert.Equal(normal.Delivered, throwing.Delivered);
        Assert.Equal(normal.Calls, throwing.Calls);
        Assert.Equal(normal.Logs, throwing.Logs);
        Assert.Equal(normal.Disposals, throwing.Disposals);
        Assert.Equal(normal.Retries, throwing.Retries);
        Assert.Equal(normal.Observed, throwing.Observed);
    }

    private static string Obs(string instrument, string kind, string outcome) => $"{instrument} {kind} {outcome}";

    private sealed record Note(int Index) : IEventRequest;

    private sealed record Expired(Guid CorrelationId, int Index) : ICorrelatedEvent;

    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly object _gate = new();
        private readonly List<string> _observations = [];
        private readonly List<Instrument> _published = [];

        public MetricCapture(bool enable = true, Action? onMeasurement = null)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name != "EventFlux.Saga") return;
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

        public void Clear()
        {
            lock (_gate) _observations.Clear();
        }

        public void Dispose()
        {
            foreach (var instrument in Published)
                _listener.DisableMeasurementEvents(instrument);
            _listener.Dispose();
        }

        private static string Describe(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var phase = instrument.Name switch
            {
                "sagaflux.dispatcher.dispatch.operations" => "dispatch",
                "sagaflux.dispatcher.completion.operations" => "completion",
                _ => $"unexpected:{instrument.Name}",
            };
            var pairs = tags.ToArray();
            var valid = value == 1
                && instrument.Unit == "{operation}"
                && pairs.Length == 2
                && pairs[0].Key == "kind" && pairs[0].Value is "outbox" or "timeout"
                && pairs[1].Key == "outcome" && pairs[1].Value is "success" or "failure" or "cancelled";
            return valid
                ? $"{phase} {pairs[0].Value} {pairs[1].Value}"
                : $"invalid {instrument.Name} {value} {string.Join(",", pairs.Select(p => $"{p.Key}={p.Value}"))}";
        }
    }

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly List<(int Index, IEventRequest Event, Guid Id, DateTimeOffset DueAt)> _pending = [];
        private readonly string _kind;

        private Harness(string kind, int items, bool failResolution)
        {
            _kind = kind;
            for (var index = 0; index < items; index++)
            {
                var correlationId = Guid.NewGuid();
                IEventRequest @event = kind == Outbox ? new Note(index) : new Expired(correlationId, index);
                _pending.Add((index, @event, Guid.NewGuid(), Now.AddMinutes(index - items)));
            }

            var services = new ServiceCollection();
            services.AddLogging(builder => builder.AddProvider(new RecordingLoggerProvider(Logs)));
            services.AddSingleton<TimeProvider>(Time);
            var builder = services.AddSagaFlux(o =>
            {
                o.EnableOutboxPolling = false;
                o.EnableTimeoutPolling = false;
                o.OutboxBatchSize = 10;
                o.TimeoutBatchSize = 10;
                o.TimeoutRetryDelay = RetryDelay;
            });
            if (kind == Outbox)
            {
                builder.UseOutbox<ScopedOutbox>();
                services.RemoveAll<ISagaOutbox>();
                services.AddScoped<ISagaOutbox>(_ => new ScopedOutbox(this));
                services.RemoveAll<ISagaTransport>();
                if (!failResolution)
                    services.AddSingleton<ISagaTransport>(new ProbeTransport(this));
            }
            else
            {
                var resolutions = 0;
                services.RemoveAll<ISagaTimeoutStore>();
                services.AddScoped<ISagaTimeoutStore>(_ => failResolution && ++resolutions > 1
                    ? throw new InvalidOperationException("store resolution failed")
                    : new ScopedTimeoutStore(this));
                services.RemoveAll<ISagaEventRouter>();
                services.AddScoped<ISagaEventRouter>(_ => new ProbeRouter(this));
            }
            _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        }

        public FakeTimeProvider Time { get; } = new(Now);
        public List<string> Calls { get; } = [];
        public List<(LogLevel Level, Exception? Error)> Logs { get; } = [];
        public List<(SagaTimeout Timeout, CancellationToken Token, Guid OriginalId)> Retries { get; } = [];
        public Func<int, CancellationToken, Task>? OnDispatch { get; set; }
        public Func<int, CancellationToken, Task>? OnComplete { get; set; }
        public Action? AfterRead { get; set; }
        public Exception? ReadError { get; set; }
        public Exception? ScheduleError { get; set; }
        public Exception? DisposeError { get; set; }
        public bool KeepPending { get; set; }
        public int Disposals { get; private set; }

        public static Harness Create(string kind, int items, bool failResolution = false) => new(kind, items, failResolution);

        public Task<int> DispatchAsync(CancellationToken ct) => _kind == Outbox
            ? _provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync(ct)
            : _provider.GetRequiredService<ISagaTimeoutDispatcher>().DispatchDueAsync(ct);

        public void AssertSingleRetryOf(int index)
        {
            var retry = Assert.Single(Retries);
            Assert.Equal(CancellationToken.None, retry.Token);
            Assert.NotEqual(retry.OriginalId, retry.Timeout.Id);
            Assert.Equal(Time.GetUtcNow() + RetryDelay, retry.Timeout.DueAt);
            Assert.Equal(index, ((Expired)retry.Timeout.Event).Index);
        }

        public ValueTask DisposeAsync() => _provider.DisposeAsync();

        private int IndexOf(object @event) => @event switch
        {
            Note note => note.Index,
            Expired expired => expired.Index,
            _ => throw new InvalidOperationException(),
        };

        private void Read(CancellationToken ct)
        {
            Calls.Add("read");
            ct.ThrowIfCancellationRequested();
            if (ReadError is not null) throw ReadError;
        }

        private async Task DispatchItemAsync(object @event, CancellationToken ct)
        {
            var index = IndexOf(@event);
            Calls.Add($"dispatch:{index}");
            if (OnDispatch is not null) await OnDispatch(index, ct);
        }

        private async Task CompleteItemAsync(Guid id, CancellationToken ct)
        {
            var item = _pending.Single(p => p.Id == id);
            Calls.Add($"complete:{item.Index}");
            if (OnComplete is not null) await OnComplete(item.Index, ct);
            if (!KeepPending) _pending.Remove(item);
        }

        private async ValueTask DisposeScopeAsync()
        {
            Disposals++;
            await Task.Yield();
            if (DisposeError is not null) throw DisposeError;
        }

        private sealed class ScopedOutbox(Harness harness) : ISagaOutbox, IAsyncDisposable
        {
            public void Stage(IReadOnlyList<SagaOutboxMessage> messages) => throw new NotSupportedException();

            public void Discard()
            {
            }

            public Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct) => harness.CompleteItemAsync(Assert.Single(ids), ct);

            public Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct)
            {
                harness.Read(ct);
                IReadOnlyList<SagaOutboxMessage> pending = harness._pending.Take(maxCount)
                    .Select(p => new SagaOutboxMessage(p.Id, p.Event, p.DueAt)).ToList();
                harness.AfterRead?.Invoke();
                return Task.FromResult(pending);
            }

            public ValueTask DisposeAsync() => harness.DisposeScopeAsync();
        }

        private sealed class ProbeTransport(Harness harness) : ISagaTransport
        {
            public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
                => harness.DispatchItemAsync(@event, ct);
        }

        private sealed class ScopedTimeoutStore(Harness harness) : ISagaTimeoutStore, IAsyncDisposable
        {
            public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
            {
                var index = harness.IndexOf(timeout.Event);
                harness.Calls.Add($"schedule:{index}");
                var original = harness._pending.Single(p => p.Index == index);
                harness.Retries.Add((timeout, ct, original.Id));
                if (harness.ScheduleError is not null) throw harness.ScheduleError;
                harness._pending.Remove(original);
                harness._pending.Add((index, timeout.Event, timeout.Id, timeout.DueAt));
                return Task.CompletedTask;
            }

            public Task CancelAsync(Guid correlationId, string key, CancellationToken ct) => throw new NotSupportedException();

            public Task CancelAllAsync(Guid correlationId, CancellationToken ct) => throw new NotSupportedException();

            public Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
            {
                harness.Read(ct);
                IReadOnlyList<SagaTimeout> due = harness._pending.Where(p => p.DueAt <= now).OrderBy(p => p.DueAt).Take(maxCount)
                    .Select(p => new SagaTimeout(p.Id, ((Expired)p.Event).CorrelationId, $"key{p.Index}", p.DueAt, p.Event)).ToList();
                harness.AfterRead?.Invoke();
                return Task.FromResult(due);
            }

            public Task CompleteAsync(Guid timeoutId, CancellationToken ct) => harness.CompleteItemAsync(timeoutId, ct);

            public ValueTask DisposeAsync() => harness.DisposeScopeAsync();
        }

        private sealed class ProbeRouter(Harness harness) : ISagaEventRouter
        {
            public Task RouteAsync(object @event, CancellationToken ct) => harness.DispatchItemAsync(@event, ct);
        }
    }

    private sealed class RecordingLoggerProvider(List<(LogLevel Level, Exception? Error)> logs) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => categoryName.Contains("Dispatcher")
            ? new RecordingLogger(logs)
            : Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

        public void Dispose()
        {
        }

        private sealed class RecordingLogger(List<(LogLevel Level, Exception? Error)> logs) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                lock (logs) logs.Add((logLevel, exception));
            }
        }
    }
}
