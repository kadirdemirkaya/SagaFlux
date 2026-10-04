using System.Collections.Concurrent;
using System.Diagnostics;
using EventFlux.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public sealed class SagaDiagnosticsTests : IDisposable
{
    private readonly ConcurrentQueue<Activity> _activities = new();
    private readonly ActivityListener _listener;

    public SagaDiagnosticsTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == SagaFluxDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _activities.Enqueue,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    private static ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<Recorder>();
        services.AddSingleton<UndoLog>();
        services.AddEventBus(typeof(SagaDiagnosticsTests).Assembly);
        services.AddSagaFlux().AddSaga<SagaDslExtensionTests.TicketSaga, SagaDslExtensionTests.TicketState>();
        return services.BuildServiceProvider();
    }

    private List<Activity> For(Guid id)
        => _activities.Where(a => (string?)a.GetTagItem(SagaFluxDiagnostics.TagCorrelationId) == id.ToString()).ToList();

    [Fact]
    public async Task Each_applied_event_is_one_activity_with_transition_tags()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();

        await dispatcher.PublishAsync(new SagaDslExtensionTests.TicketOpened(id));
        await dispatcher.PublishAsync(new SagaDslExtensionTests.TicketPinged(id, "invalid"));
        await dispatcher.PublishAsync(new SagaDslExtensionTests.TicketClosed(id));

        var activities = For(id);
        Assert.Equal(
            new[] { "TicketSaga TicketOpened", "TicketSaga TicketPinged", "TicketSaga TicketClosed" },
            activities.Select(a => a.DisplayName));
        var opened = activities[0];
        Assert.Equal(("TicketSaga", "TicketOpened", true, "Initial", "Open", 1),
            ((string?)opened.GetTagItem(SagaFluxDiagnostics.TagSaga), (string?)opened.GetTagItem(SagaFluxDiagnostics.TagEvent),
             (bool?)opened.GetTagItem(SagaFluxDiagnostics.TagHandled), (string?)opened.GetTagItem(SagaFluxDiagnostics.TagFromState),
             (string?)opened.GetTagItem(SagaFluxDiagnostics.TagToState), (int?)opened.GetTagItem(SagaFluxDiagnostics.TagVersion)));
        Assert.Equal("InvalidOperationException", activities[1].GetTagItem(SagaFluxDiagnostics.TagFault));
        Assert.Equal(true, activities[2].GetTagItem(SagaFluxDiagnostics.TagRemoved));
        Assert.All(activities, a => Assert.Equal(ActivityStatusCode.Unset, a.Status));
    }

    [Fact]
    public async Task A_failing_event_marks_the_activity_as_an_error()
    {
        await using var provider = Build();
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        var id = Guid.NewGuid();
        await dispatcher.PublishAsync(new SagaDslExtensionTests.TicketOpened(id));

        await Assert.ThrowsAsync<TimeoutException>(() => dispatcher.PublishAsync(new SagaDslExtensionTests.TicketPinged(id, "timeout")));

        var failed = For(id).Last();
        Assert.Equal(ActivityStatusCode.Error, failed.Status);
        Assert.Equal(typeof(TimeoutException).FullName, failed.Events.Single().Tags.First(t => t.Key == "exception.type").Value);
    }

    [Fact]
    public async Task An_ignored_event_is_tagged_as_not_handled()
    {
        await using var provider = Build();
        var id = Guid.NewGuid();

        await provider.GetRequiredService<IEventDispatcher>().PublishAsync(new SagaDslExtensionTests.TicketClosed(id));

        var ignored = Assert.Single(For(id));
        Assert.Equal(false, ignored.GetTagItem(SagaFluxDiagnostics.TagHandled));
        Assert.Null(ignored.GetTagItem(SagaFluxDiagnostics.TagToState));
    }

    public void Dispose() => _listener.Dispose();
}
