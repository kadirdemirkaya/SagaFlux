using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace EventFlux.Saga.Inspector.EfCore;

/// <summary>
/// EF Core model configuration for the saga inspector timeline.
/// </summary>
public static class SagaTimelineModelBuilderExtensions
{
    /// <summary>
    /// Maps <see cref="SagaTimelineEntry"/> for <see cref="EfCoreSagaTimelineStore"/>, indexed by instance and by time.
    /// </summary>
    /// <param name="modelBuilder">The model builder of your <see cref="DbContext"/>.</param>
    /// <param name="tableName">The table name.</param>
    /// <returns>The entity builder, for further configuration.</returns>
    public static EntityTypeBuilder<SagaTimelineEntry> ConfigureSagaTimeline(this ModelBuilder modelBuilder, string tableName = "SagaTimeline")
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var entity = modelBuilder.Entity<SagaTimelineEntry>();
        entity.ToTable(tableName);
        entity.HasKey(e => e.Id);
        entity.Property(e => e.Id).ValueGeneratedNever();
        entity.Property(e => e.SagaType).IsRequired().HasMaxLength(256);
        entity.Property(e => e.EventType).IsRequired().HasMaxLength(256);
        entity.Property(e => e.FromState).IsRequired().HasMaxLength(128);
        entity.Property(e => e.ToState).IsRequired().HasMaxLength(128);
        entity.Property(e => e.Published).IsRequired();
        entity.Property(e => e.Compensated).IsRequired();
        entity.Property(e => e.Scheduled).IsRequired();
        entity.Property(e => e.Unscheduled).IsRequired();
        entity.Property(e => e.Fault).HasMaxLength(2048);
        entity.HasIndex(e => new { e.CorrelationId, e.OccurredAtUtcTicks });
        entity.HasIndex(e => e.OccurredAtUtcTicks);
        return entity;
    }
}
