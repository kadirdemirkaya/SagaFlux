using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.EfCore.Test;

public class ConfigureSagaStateTableNameTests
{
    private sealed class NamedTableContext(DbContextOptions<NamedTableContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.ConfigureSagaState<OrderState>("CustomOrderSagas", 64);
            modelBuilder.ConfigureSagaState<TripState>();
        }
    }

    [Fact]
    public void The_table_name_overload_maps_the_saga_state_into_the_named_table()
    {
        using var connection = new SqliteConnection("DataSource=:memory:");
        connection.Open();
        using var context = new NamedTableContext(new DbContextOptionsBuilder<NamedTableContext>().UseSqlite(connection).Options);
        context.Database.EnsureCreated();

        var named = context.Model.FindEntityType(typeof(OrderState))!;
        Assert.Equal("CustomOrderSagas", named.GetTableName());
        Assert.Equal(nameof(ISagaState.CorrelationId), Assert.Single(named.FindPrimaryKey()!.Properties).Name);
        Assert.Equal(64, named.FindProperty(nameof(ISagaState.CurrentState))!.GetMaxLength());
        Assert.True(named.FindProperty(nameof(ISagaState.Version))!.IsConcurrencyToken);

        var unnamed = context.Model.FindEntityType(typeof(TripState))!;
        Assert.Equal(nameof(TripState), unnamed.GetTableName());
        Assert.Equal(128, unnamed.FindProperty(nameof(ISagaState.CurrentState))!.GetMaxLength());

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'CustomOrderSagas'";
        Assert.Equal("CustomOrderSagas", command.ExecuteScalar());
    }

    [Fact]
    public void The_default_overload_keeps_the_table_name_given_afterwards()
    {
        using var database = new SqliteDatabase();
        using var context = database.CreateContext();

        Assert.Equal("OrderSagas", context.Model.FindEntityType(typeof(OrderState))!.GetTableName());
        Assert.Equal(128, context.Model.FindEntityType(typeof(OrderState))!.FindProperty(nameof(ISagaState.CurrentState))!.GetMaxLength());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void A_missing_table_name_is_rejected(string? tableName)
    {
        Assert.ThrowsAny<ArgumentException>(() => new ModelBuilder().ConfigureSagaState<OrderState>(tableName!));
    }

    [Fact]
    public void The_existing_overload_keeps_its_signature()
    {
        var method = Assert.Single(typeof(SagaModelBuilderExtensions).GetMethods(),
            m => m.Name == nameof(SagaModelBuilderExtensions.ConfigureSagaState)
                 && m.GetParameters().Select(p => p.ParameterType).SequenceEqual(new[] { typeof(ModelBuilder), typeof(int) }));

        Assert.Equal(128, method.GetParameters()[1].DefaultValue);
        Assert.Equal(2, typeof(SagaModelBuilderExtensions).GetMethods().Count(m => m.Name == nameof(SagaModelBuilderExtensions.ConfigureSagaState)));
    }
}
