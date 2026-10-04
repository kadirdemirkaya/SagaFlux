using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.EfCore.Test;

public sealed record StockArrived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record InvoicePaid(Guid CorrelationId) : ICorrelatedEvent;

public sealed class DeliveryState : IJoinableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public List<string> JoinedEvents { get; set; } = new();
}

public sealed class DeliverySaga : Saga<DeliveryState>
{
    public static readonly SagaState Deliverable = SagaStates.Define(nameof(Deliverable));

    public DeliverySaga() => During(SagaStates.Initial, WhenAll<StockArrived, InvoicePaid>().TransitionTo(Deliverable));
}

public sealed class DeliveryDbContext(DbContextOptions<DeliveryDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureSagaState<DeliveryState>().ToTable("DeliverySagas");
}

public sealed class JoinPersistenceTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    public JoinPersistenceTests()
    {
        _connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    private DeliveryDbContext CreateContext() => new(new DbContextOptionsBuilder<DeliveryDbContext>().UseSqlite(_connection).Options);

    [Fact]
    public async Task Join_progress_is_persisted_between_events()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<DeliveryDbContext>(o => o.UseSqlite(_connection));
        services.AddEventBus(typeof(JoinPersistenceTests).Assembly);
        services.AddSagaFlux().AddSaga<DeliverySaga, DeliveryState>().UseEfCore<DeliveryState, DeliveryDbContext>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var id = Guid.NewGuid();

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new InvoicePaid(id));

        await using (var context = CreateContext())
        {
            var waiting = await context.Set<DeliveryState>().SingleAsync(s => s.CorrelationId == id);
            Assert.Equal((SagaStates.Initial.Name, 1), (waiting.CurrentState, waiting.Version));
            Assert.Equal(new[] { "StockArrived+InvoicePaid/InvoicePaid" }, waiting.JoinedEvents);
        }

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new StockArrived(id));

        await using (var context = CreateContext())
        {
            var done = await context.Set<DeliveryState>().SingleAsync(s => s.CorrelationId == id);
            Assert.Equal((DeliverySaga.Deliverable.Name, 2), (done.CurrentState, done.Version));
            Assert.Empty(done.JoinedEvents);
        }
    }

    public void Dispose() => _connection.Dispose();
}
