using EfCore.Repository.Abstractions;
using EfCore.Repository.Extensions;
using EventFlux.Saga;
using EventFlux.Saga.Inspector;
using EventFlux.Saga.Inspector.EfCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the EF Core timeline store of the saga inspector.
/// </summary>
public static class SagaFluxInspectorEfCoreBuilderExtensions
{
    /// <summary>
    /// Records the inspector timeline in <typeparamref name="TDbContext"/> instead of memory, so it survives restarts
    /// and every node shows the same history. Calls <c>UseInspector()</c> if it was not called yet; map the table
    /// with <c>modelBuilder.ConfigureSagaTimeline()</c>.
    /// </summary>
    /// <typeparam name="TDbContext">The context mapping <see cref="SagaTimelineEntry"/>; register it yourself.</typeparam>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseEfCoreTimeline<TDbContext>(this SagaFluxBuilder builder)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseInspector();
        var services = builder.Services;

        if (!services.Any(d => d.ServiceType == typeof(IReadRepository<SagaTimelineEntry>) || d.ServiceType == typeof(IWriteRepository<SagaTimelineEntry>)))
            services.AddEfCoreRepository<SagaTimelineEntry, TDbContext>(ServiceLifetime.Scoped);

        services.RemoveAll<ISagaTimelineStore>();
        services.AddSingleton<ISagaTimelineStore, EfCoreSagaTimelineStore>();
        return builder;
    }
}
