using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.Saga.Test.Fixtures;
using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.Test;

public sealed class ReleaseSeatHandler : IEventHandler<ReleaseSeat>
{
    public static ConcurrentQueue<ReleaseSeat> Released { get; } = new();

    public Task Handle(ReleaseSeat @event, CancellationToken cancellationToken)
    {
        Released.Enqueue(@event);
        return Task.CompletedTask;
    }
}

[Collection(nameof(ReservationSaga))]
public sealed class TimeoutTests : IAsyncLifetime
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 9, 26, 12, 0, 0, TimeSpan.Zero));
    private readonly Guid _id = Guid.NewGuid();
    private ServiceProvider _provider = null!;

    private IEventDispatcher Dispatcher => _provider.GetRequiredService<IEventDispatcher>();
    private InMemorySagaTimeoutStore Store => (InMemorySagaTimeoutStore)_provider.GetRequiredService<ISagaTimeoutStore>();
    private ISagaTimeoutDispatcher Timeouts => _provider.GetRequiredService<ISagaTimeoutDispatcher>();

    public Task InitializeAsync()
    {
        ReservationSaga.FailExpiryTimes = 0;
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddSingleton<TimeProvider>(_time);
        services.AddEventBus(typeof(TimeoutTests).Assembly);
        services.AddSagaFlux(o => o.TimeoutRetryDelay = TimeSpan.FromMinutes(1)).AddSaga<ReservationSaga, ReservationState>();
        _provider = services.BuildServiceProvider();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _provider.DisposeAsync();

    private async Task<ReservationState> StateAsync()
        => (await _provider.GetRequiredService<ISagaRepository<ReservationState>>().FindAsync(_id, default))!;

    [Fact]
    public async Task Scheduled_timeout_is_stored_with_its_due_time()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));

        var due = await Store.GetDueAsync(_time.GetUtcNow() + ReservationSaga.HoldFor, 10, default);
        var timeout = Assert.Single(due);
        Assert.Equal(_id, timeout.CorrelationId);
        Assert.Equal(_time.GetUtcNow() + ReservationSaga.HoldFor, timeout.DueAt);
        Assert.IsType<ReservationExpired>(timeout.Event);
    }

    [Fact]
    public async Task Timeout_fires_only_when_due_and_drives_the_saga()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));

        _time.Advance(ReservationSaga.HoldFor - TimeSpan.FromSeconds(1));
        Assert.Equal(0, await Timeouts.DispatchDueAsync());
        Assert.Equal(ReservationSaga.AwaitingPayment.Name, (await StateAsync()).CurrentState);

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await Timeouts.DispatchDueAsync());

        Assert.Equal(ReservationSaga.Expired.Name, (await StateAsync()).CurrentState);
        Assert.Single(ReleaseSeatHandler.Released, r => r.ReservationId == _id);
        Assert.Equal(0, Store.Count);
    }

    [Fact]
    public async Task Unscheduled_timeout_never_fires()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));
        await Dispatcher.PublishAsync(new ReservationPaid(_id));

        Assert.Equal(0, Store.Count);
        _time.Advance(TimeSpan.FromHours(1));
        Assert.Equal(0, await Timeouts.DispatchDueAsync());
        Assert.Equal(ReservationSaga.Confirmed.Name, (await StateAsync()).CurrentState);
    }

    [Fact]
    public async Task Scheduling_again_replaces_the_pending_timeout()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));
        _time.Advance(TimeSpan.FromMinutes(10));
        await Dispatcher.PublishAsync(new ReservationExtended(_id));

        _time.Advance(ReservationSaga.HoldFor);
        Assert.Equal(0, await Timeouts.DispatchDueAsync());
        Assert.Equal(1, Store.Count);

        _time.Advance(ReservationSaga.ExtendBy - ReservationSaga.HoldFor);
        Assert.Equal(1, await Timeouts.DispatchDueAsync());
        Assert.Equal(ReservationSaga.Expired.Name, (await StateAsync()).CurrentState);
    }

    [Fact]
    public async Task Reaching_final_cancels_pending_timeouts()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));
        await Dispatcher.PublishAsync(new ReservationClosed(_id));

        Assert.Equal(0, Store.Count);
    }

    [Fact]
    public async Task Failed_delivery_is_retried_after_the_retry_delay()
    {
        ReservationSaga.FailExpiryTimes = 1;
        await Dispatcher.PublishAsync(new ReservationRequested(_id));
        _time.Advance(ReservationSaga.HoldFor);

        Assert.Equal(0, await Timeouts.DispatchDueAsync());
        Assert.Equal(ReservationSaga.AwaitingPayment.Name, (await StateAsync()).CurrentState);
        Assert.Equal(1, Store.Count);

        _time.Advance(TimeSpan.FromSeconds(59));
        Assert.Equal(0, await Timeouts.DispatchDueAsync());

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, await Timeouts.DispatchDueAsync());
        Assert.Equal(ReservationSaga.Expired.Name, (await StateAsync()).CurrentState);
    }

    [Fact]
    public async Task Hosted_service_delivers_due_timeouts()
    {
        await Dispatcher.PublishAsync(new ReservationRequested(_id));
        var hosted = Assert.Single(_provider.GetServices<IHostedService>());
        await hosted.StartAsync(default);

        _time.Advance(ReservationSaga.HoldFor + TimeSpan.FromSeconds(1));
        for (var i = 0; i < 100 && (await StateAsync()).CurrentState != ReservationSaga.Expired.Name; i++)
            await Task.Delay(20);

        await hosted.StopAsync(default);
        Assert.Equal(ReservationSaga.Expired.Name, (await StateAsync()).CurrentState);
    }
}

[CollectionDefinition(nameof(ReservationSaga), DisableParallelization = true)]
public sealed class ReservationSagaCollection;

public class TimeoutRegistrationTests
{
    [Fact]
    public void Polling_is_registered_only_for_sagas_that_schedule()
    {
        var without = new ServiceCollection();
        without.AddSagaFlux().AddSaga<OrderSaga, OrderState>();
        Assert.DoesNotContain(without, d => d.ServiceType == typeof(IHostedService));

        var with = new ServiceCollection();
        with.AddSagaFlux().AddSaga<ReservationSaga, ReservationState>().UseTimeoutPolling();
        Assert.Single(with, d => d.ServiceType == typeof(IHostedService));
    }

    [Fact]
    public async Task Engine_reports_scheduled_and_unscheduled_timeouts()
    {
        var saga = new ReservationSaga();
        var id = Guid.NewGuid();

        var requested = await saga.ExecuteAsync(null, new ReservationRequested(id));
        var paid = await saga.ExecuteAsync(requested.State, new ReservationPaid(id));

        var scheduled = Assert.Single(requested.Scheduled);
        Assert.Equal(ReservationSaga.HoldFor, scheduled.Delay);
        Assert.Equal(SagaTimeoutKeys.For<ReservationExpired>(), scheduled.Key);
        Assert.Equal(new[] { SagaTimeoutKeys.For<ReservationExpired>() }, paid.Unscheduled);
    }

    [Fact]
    public void Scheduling_an_uncorrelatable_timeout_fails_validation()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new UncorrelatedTimeoutSaga().Validate());
        Assert.Contains(nameof(ReleaseSeat), ex.Message);
    }

    [Fact]
    public void Negative_delay_is_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NegativeDelaySaga());
    }

    private sealed class UncorrelatedTimeoutSaga : Saga<ReservationState>
    {
        public UncorrelatedTimeoutSaga()
            => During(SagaStates.Initial, When<ReservationRequested>().Schedule(TimeSpan.FromMinutes(1), ctx => new ReleaseSeat(ctx.CorrelationId)));
    }

    private sealed class NegativeDelaySaga : Saga<ReservationState>
    {
        public NegativeDelaySaga()
            => During(SagaStates.Initial, When<ReservationRequested>().Schedule(TimeSpan.FromSeconds(-1), ctx => new ReservationExpired(ctx.CorrelationId)));
    }
}
