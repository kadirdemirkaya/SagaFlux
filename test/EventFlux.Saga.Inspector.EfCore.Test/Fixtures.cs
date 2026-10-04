using EventFlux.Abstractions;
using EventFlux.Saga.EfCore;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.Inspector.EfCore.Test;

public sealed record OrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;

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
        During(SagaStates.Initial,
            When<OrderPlaced>()
                .Publish(ctx => new ChargeCustomer(ctx.CorrelationId, ctx.Event.Amount))
                .TransitionTo(AwaitingPayment));
        During(AwaitingPayment,
            When<PaymentReceived>().Finalize());
    }
}

public sealed class InspectorDbContext(DbContextOptions<InspectorDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureSagaState<OrderState>().ToTable("OrderSagas");
        modelBuilder.ConfigureSagaTimeline();
    }
}

/// <summary>A SQLite in-memory database that lives as long as the fixture (one open connection).</summary>
public sealed class SqliteDatabase : IDisposable
{
    public SqliteDatabase()
    {
        Connection = new SqliteConnection("DataSource=:memory:");
        Connection.Open();
        using var context = new InspectorDbContext(new DbContextOptionsBuilder<InspectorDbContext>().UseSqlite(Connection).Options);
        context.Database.EnsureCreated();
    }

    public SqliteConnection Connection { get; }

    public void Dispose() => Connection.Dispose();
}
