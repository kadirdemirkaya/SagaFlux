using EventFlux.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Inspector.EfCore.Test;

public sealed class EfCoreSagaTimelineStoreTests : IDisposable
{
    private readonly SqliteDatabase _db = new();

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<InspectorDbContext>(o => o.UseSqlite(_db.Connection));
        services.AddEventBus(typeof(EfCoreSagaTimelineStoreTests).Assembly);
        services.AddSagaFlux()
            .AddSaga<OrderSaga, OrderState>()
            .UseEfCore<OrderState, InspectorDbContext>()
            .UseEfCoreTimeline<InspectorDbContext>();
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static SagaTransitionRecord Record(Guid id, int version, DateTimeOffset at, string to) => new(
        id, "OrderSaga", "OrderPlaced", "Initial", to, version == 1, version,
        ["ChargeCustomer"], ["refund"], ["PaymentTimeout"], [], at);

    [Fact]
    public async Task Stores_and_reads_back_timelines_and_recent_records()
    {
        await using var provider = Build();
        var store = provider.GetRequiredService<ISagaTimelineStore>();
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var t0 = new DateTimeOffset(2026, 9, 27, 10, 0, 0, TimeSpan.Zero);

        await store.AppendAsync(Record(a, 1, t0, "AwaitingPayment"), default);
        await store.AppendAsync(Record(b, 1, t0.AddSeconds(1), "AwaitingPayment"), default);
        await store.AppendAsync(Record(a, 2, t0.AddSeconds(2), "Final") with { Fault = "TimeoutException: slow", Removed = true }, default);

        var timeline = await store.GetTimelineAsync(a, default);
        Assert.Equal(new[] { 1, 2 }, timeline.Select(r => r.Version));
        var first = timeline[0];
        Assert.Equal(("OrderSaga", "OrderPlaced", "Initial", "AwaitingPayment", true), (first.SagaType, first.EventType, first.FromState, first.ToState, first.Created));
        Assert.Equal(["ChargeCustomer"], first.Published);
        Assert.Equal(["refund"], first.Compensated);
        Assert.Equal(["PaymentTimeout"], first.Scheduled);
        Assert.Empty(first.Unscheduled);
        Assert.Equal(t0, first.OccurredAt);

        Assert.Equal(("TimeoutException: slow", true), (timeline[1].Fault, timeline[1].Removed));
        Assert.Equal((null, false), (first.Fault, first.Removed));

        var recent = await store.GetRecentAsync(2, default);
        Assert.Equal(new[] { (a, 2), (b, 1) }, recent.Select(r => (r.CorrelationId, r.Version)));
        Assert.Empty(await store.GetRecentAsync(0, default));
    }

    [Fact]
    public async Task Prune_deletes_only_entries_older_than_the_cutoff()
    {
        await using var provider = Build();
        var store = (EfCoreSagaTimelineStore)provider.GetRequiredService<ISagaTimelineStore>();
        var id = Guid.NewGuid();
        var t0 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await store.AppendAsync(Record(id, 1, t0, "AwaitingPayment"), default);
        await store.AppendAsync(Record(id, 2, t0.AddDays(10), "Final"), default);

        Assert.Equal(1, await store.PruneAsync(t0.AddDays(5), default));

        Assert.Equal(new[] { 2 }, (await store.GetTimelineAsync(id, default)).Select(r => r.Version));
        Assert.Equal(0, await store.PruneAsync(t0.AddDays(5), default));
    }

    [Fact]
    public async Task Saga_transitions_are_recorded_and_survive_a_restart()
    {
        var orderId = Guid.NewGuid();
        await using (var provider = Build())
        {
            await using (var scope = provider.CreateAsyncScope())
                await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new OrderPlaced(orderId, 5m));
            await using (var scope = provider.CreateAsyncScope())
                await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new PaymentReceived(orderId));
        }

        // A new container over the same database stands for a restarted (or another) node.
        await using var restarted = Build();
        var timeline = await restarted.GetRequiredService<ISagaTimelineStore>().GetTimelineAsync(orderId, default);

        Assert.Equal(
            new[] { ("Initial", "AwaitingPayment"), ("AwaitingPayment", SagaStates.Final.Name) },
            timeline.Select(r => (r.FromState, r.ToState)));
        Assert.Equal(["ChargeCustomer"], timeline[0].Published);
    }

    [Fact]
    public void UseEfCoreTimeline_replaces_the_in_memory_store_whatever_the_call_order()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux().UseInspector().UseEfCoreTimeline<InspectorDbContext>().UseInspector();

        var store = Assert.Single(services, d => d.ServiceType == typeof(ISagaTimelineStore));
        Assert.Equal(typeof(EfCoreSagaTimelineStore), store.ImplementationType);
        Assert.Equal(ServiceLifetime.Singleton, store.Lifetime);
        Assert.Single(services, d => d.ServiceType == typeof(ISagaTransitionObserver));
    }

    public void Dispose() => _db.Dispose();
}
