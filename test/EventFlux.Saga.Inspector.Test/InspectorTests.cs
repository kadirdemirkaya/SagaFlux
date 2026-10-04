using System.Net;
using System.Net.Http.Json;
using EventFlux.Abstractions;
using EventFlux.Saga.Inspector;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Inspector.Test;

public sealed record OrderPlaced(Guid CorrelationId) : ICorrelatedEvent;
public sealed record OrderPaid(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipOrder(Guid OrderId) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public OrderSaga()
    {
        During(SagaStates.Initial, When<OrderPlaced>().TransitionTo(AwaitingPayment));
        During(AwaitingPayment, When<OrderPaid>().Publish(ctx => new ShipOrder(ctx.CorrelationId)).Finalize());
    }
}

public sealed record TimelineEntry(Guid CorrelationId, string SagaType, string EventType, string FromState, string ToState,
    bool Created, int Version, string[] Published, DateTimeOffset OccurredAt);

public sealed class InspectorTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddEventBus(typeof(InspectorTests).Assembly);
        builder.Services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseInspector(o => o.MaxEntries = 100);
        _app = builder.Build();
        _app.MapSagaInspector();
        await _app.StartAsync();
        _client = _app.GetTestClient();
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    private async Task RunOrderAsync(Guid id)
    {
        await using var scope = _app.Services.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IEventDispatcher>();
        await dispatcher.PublishAsync(new OrderPlaced(id));
        await dispatcher.PublishAsync(new OrderPaid(id));
    }

    [Fact]
    public async Task Instance_timeline_is_served_as_json_oldest_first()
    {
        var id = Guid.NewGuid();
        await RunOrderAsync(id);

        var timeline = await _client.GetFromJsonAsync<TimelineEntry[]>($"/saga-inspector/instances/{id}");

        Assert.NotNull(timeline);
        Assert.Equal(2, timeline!.Length);
        Assert.Equal(("Initial", "AwaitingPayment", true), (timeline[0].FromState, timeline[0].ToState, timeline[0].Created));
        Assert.Equal(("AwaitingPayment", "Final"), (timeline[1].FromState, timeline[1].ToState));
        Assert.Equal(nameof(OrderPaid), timeline[1].EventType);
        Assert.Equal(new[] { nameof(ShipOrder) }, timeline[1].Published);
        Assert.Equal(nameof(OrderSaga), timeline[1].SagaType);
    }

    [Fact]
    public async Task Unknown_instance_is_not_found()
    {
        var response = await _client.GetAsync($"/saga-inspector/instances/{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Recent_records_are_newest_first_and_limited()
    {
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        await RunOrderAsync(first);
        await RunOrderAsync(second);

        var recent = await _client.GetFromJsonAsync<TimelineEntry[]>("/saga-inspector/recent?take=3");

        Assert.Equal(3, recent!.Length);
        Assert.Equal(second, recent[0].CorrelationId);
        Assert.Equal("Final", recent[0].ToState);
    }

    [Fact]
    public async Task Html_page_is_served()
    {
        var response = await _client.GetAsync("/saga-inspector");

        response.EnsureSuccessStatusCode();
        Assert.Equal("text/html", response.Content.Headers.ContentType!.MediaType);
        Assert.Contains("<title>Saga Inspector</title>", await response.Content.ReadAsStringAsync());
    }
}

public class TimelineStoreTests
{
    private static SagaTransitionRecord Record(Guid id, string to)
        => new(id, "S", "E", "A", to, false, 1, Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>(), Array.Empty<string>(), DateTimeOffset.UtcNow);

    [Fact]
    public async Task Store_evicts_oldest_records_beyond_the_limit()
    {
        var store = new InMemorySagaTimelineStore(new SagaInspectorOptions { MaxEntries = 3 });
        var id = Guid.NewGuid();

        foreach (var state in new[] { "1", "2", "3", "4" })
            await store.AppendAsync(Record(id, state), default);

        Assert.Equal(3, store.Count);
        Assert.Equal(new[] { "2", "3", "4" }, (await store.GetTimelineAsync(id, default)).Select(r => r.ToState));
        Assert.Equal("4", (await store.GetRecentAsync(1, default))[0].ToState);
    }

    [Fact]
    public void UseInspector_registers_one_observer_and_keeps_options()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux().UseInspector().UseInspector(o => o.MaxEntries = 5);

        Assert.Single(services, d => d.ServiceType == typeof(ISagaTransitionObserver));
        var options = (SagaInspectorOptions)Assert.Single(services, d => d.ServiceType == typeof(SagaInspectorOptions)).ImplementationInstance!;
        Assert.Equal(5, options.MaxEntries);
    }

    [Fact]
    public void Max_entries_must_be_positive()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SagaInspectorOptions { MaxEntries = 0 });
    }
}
