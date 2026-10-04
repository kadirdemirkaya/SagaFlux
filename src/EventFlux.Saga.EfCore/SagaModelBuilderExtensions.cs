using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.EfCore;

/// <summary>
/// EF Core model configuration for saga state entities.
/// </summary>
public static class SagaModelBuilderExtensions
{
    /// <summary>
    /// Maps <typeparamref name="TState"/> as a saga instance: <see cref="ISagaState.CorrelationId"/> is the key,
    /// <see cref="ISagaState.CurrentState"/> is required and <see cref="ISagaState.Version"/> is a concurrency token.
    /// For an <see cref="ICompensableSagaState"/>, the compensation stack is stored as a JSON text column, and so are
    /// the pending join events of an <see cref="IJoinableSagaState"/>.
    /// </summary>
    /// <typeparam name="TState">The saga state entity.</typeparam>
    /// <param name="modelBuilder">The model builder of your <see cref="DbContext"/>.</param>
    /// <param name="stateNameMaxLength">Maximum length of the persisted state name.</param>
    /// <returns>The entity builder, for further configuration (table name, business columns).</returns>
    public static Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<TState> ConfigureSagaState<TState>(
        this ModelBuilder modelBuilder, int stateNameMaxLength = 128)
        where TState : class, ISagaState
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        var entity = modelBuilder.Entity<TState>();
        entity.HasKey(s => s.CorrelationId);
        entity.Property(s => s.CorrelationId).ValueGeneratedNever();
        entity.Property(s => s.CurrentState).IsRequired().HasMaxLength(stateNameMaxLength);
        entity.Property(s => s.Version).IsConcurrencyToken();

        if (typeof(ICompensableSagaState).IsAssignableFrom(typeof(TState)))
        {
            entity.Property<List<CompensationEntry>>(nameof(ICompensableSagaState.Compensations))
                  .HasConversion(CompensationStackConverter.Instance, CompensationStackConverter.Comparer)
                  .IsRequired();
        }

        if (typeof(IJoinableSagaState).IsAssignableFrom(typeof(TState)))
        {
            entity.Property<List<string>>(nameof(IJoinableSagaState.JoinedEvents))
                  .HasConversion(JoinedEventsConverter.Instance, JoinedEventsConverter.Comparer)
                  .IsRequired();
        }

        return entity;
    }

    /// <summary>
    /// Maps <see cref="SagaTimeoutRecord"/> for <see cref="EfCoreSagaTimeoutStore"/>: one pending timeout per
    /// instance and key, indexed by due time.
    /// </summary>
    /// <param name="modelBuilder">The model builder of your <see cref="DbContext"/>.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The entity builder, for further configuration.</returns>
    public static Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<SagaTimeoutRecord> ConfigureSagaTimeouts(
        this ModelBuilder modelBuilder, string tableName = "SagaTimeouts")
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var entity = modelBuilder.Entity<SagaTimeoutRecord>();
        entity.ToTable(tableName);
        entity.HasKey(r => r.Id);
        entity.Property(r => r.Id).ValueGeneratedNever();
        entity.Property(r => r.Key).IsRequired().HasMaxLength(512);
        entity.Property(r => r.EventType).IsRequired().HasMaxLength(1024);
        entity.Property(r => r.Payload).IsRequired();
        entity.HasIndex(r => new { r.CorrelationId, r.Key }).IsUnique();
        entity.HasIndex(r => r.DueAtUtcTicks);
        return entity;
    }

    /// <summary>
    /// Maps <see cref="SagaOutboxRecord"/> for <see cref="EfCoreSagaOutbox"/>, indexed by staging time.
    /// </summary>
    /// <param name="modelBuilder">The model builder of your <see cref="DbContext"/>.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The entity builder, for further configuration.</returns>
    public static Microsoft.EntityFrameworkCore.Metadata.Builders.EntityTypeBuilder<SagaOutboxRecord> ConfigureSagaOutbox(
        this ModelBuilder modelBuilder, string tableName = "SagaOutbox")
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var entity = modelBuilder.Entity<SagaOutboxRecord>();
        entity.ToTable(tableName);
        entity.HasKey(r => r.Id);
        entity.Property(r => r.Id).ValueGeneratedNever();
        entity.Property(r => r.EventType).IsRequired().HasMaxLength(1024);
        entity.Property(r => r.Payload).IsRequired();
        entity.HasIndex(r => r.StagedAtUtcTicks);
        return entity;
    }
}
