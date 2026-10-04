using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.EfCore.Test;

public sealed record OrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipOrder(Guid OrderId) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public OrderSaga()
    {
        During(SagaStates.Initial,
            When<OrderPlaced>().Then(ctx => ctx.State.Amount = ctx.Event.Amount).TransitionTo(AwaitingPayment));
        During(AwaitingPayment,
            When<PaymentReceived>().Publish(ctx => new ShipOrder(ctx.CorrelationId)).Finalize());
    }
}

public sealed class SagaDbContext(DbContextOptions<SagaDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureSagaState<OrderState>().ToTable("OrderSagas");
        modelBuilder.ConfigureSagaState<TripState>().ToTable("TripSagas");
    }
}

/// <summary>A SQLite in-memory database that lives as long as the fixture (one open connection).</summary>
public sealed class SqliteDatabase : IDisposable
{
    public SqliteDatabase()
    {
        Connection = new SqliteConnection("DataSource=:memory:");
        Connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    public SqliteConnection Connection { get; }

    public DbContextOptions<SagaDbContext> Options => new DbContextOptionsBuilder<SagaDbContext>().UseSqlite(Connection).Options;

    public SagaDbContext CreateContext() => new(Options);

    public void Dispose() => Connection.Dispose();
}

public sealed class TripState : ICompensableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public List<CompensationEntry> Compensations { get; set; } = new();
}
