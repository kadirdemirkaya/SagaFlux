using EfCore.Repository.Abstractions;
using EfCore.Repository.Extensions;
using EventFlux.Saga;
using EventFlux.Saga.EfCore;
using Microsoft.EntityFrameworkCore;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers EF Core persistence for SagaFlux.
/// </summary>
public static class SagaFluxEfCoreBuilderExtensions
{
    /// <summary>
    /// Stores <typeparamref name="TState"/> instances in <typeparamref name="TDbContext"/> through EfCore.RepositoryPack.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register <typeparamref name="TDbContext"/> yourself (for example with <c>AddDbContext</c>). The EfCore.RepositoryPack
    /// <see cref="IReadRepository{TEntity}"/> / <see cref="IWriteRepository{TEntity}"/> of <typeparamref name="TState"/> are
    /// registered as scoped services over that same context with EfCore.RepositoryPack's own
    /// <c>AddEfCoreRepository</c> (unless already registered), and
    /// <see cref="ISagaRepository{TState}"/> becomes a scoped <see cref="EfCoreSagaRepository{TState}"/>.
    /// </para>
    /// <para>
    /// Because the repository is scoped, dispatch saga events inside a DI scope (for example an ASP.NET Core request)
    /// or enable EventFlux's <c>CreateScopePerEvent</c> option.
    /// </para>
    /// </remarks>
    /// <typeparam name="TState">The saga state entity.</typeparam>
    /// <typeparam name="TDbContext">The context mapping <typeparamref name="TState"/>.</typeparam>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseEfCore<TState, TDbContext>(this SagaFluxBuilder builder)
        where TState : class, ISagaState, new()
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        AddRepositories<TState, TDbContext>(services);
        return builder.UseRepository<TState, EfCoreSagaRepository<TState>>(ServiceLifetime.Scoped);
    }

    /// <summary>
    /// Stores pending saga timeouts in <typeparamref name="TDbContext"/> through EfCore.RepositoryPack, so they survive
    /// restarts. Map them with <c>modelBuilder.ConfigureSagaTimeouts()</c>.
    /// </summary>
    /// <typeparam name="TDbContext">The context mapping <see cref="SagaTimeoutRecord"/>.</typeparam>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseEfCoreTimeouts<TDbContext>(this SagaFluxBuilder builder)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        AddRepositories<SagaTimeoutRecord, TDbContext>(services);
        return builder.UseTimeoutStore<EfCoreSagaTimeoutStore>(ServiceLifetime.Scoped);
    }

    /// <summary>
    /// Keeps the events sagas publish in a transactional outbox table of <typeparamref name="TDbContext"/>: they are
    /// saved with the instance and sent afterwards, so a crash between the save and the send cannot lose them. Map it
    /// with <c>modelBuilder.ConfigureSagaOutbox()</c>.
    /// </summary>
    /// <remarks>
    /// The outbox commits with the EF Core saga repository of the same scope, so use it for sagas persisted with
    /// <see cref="UseEfCore{TState, TDbContext}"/> on the same context. Pending messages are sent by a hosted service
    /// (<see cref="SagaFluxOptions.OutboxPollingInterval"/>); delivery is at least once.
    /// </remarks>
    /// <typeparam name="TDbContext">The context mapping <see cref="SagaOutboxRecord"/>.</typeparam>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseEfCoreOutbox<TDbContext>(this SagaFluxBuilder builder)
        where TDbContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        AddRepositories<SagaOutboxRecord, TDbContext>(builder.Services);
        return builder.UseOutbox<EfCoreSagaOutbox>(ServiceLifetime.Scoped);
    }

    // AddEfCoreRepository resolves TDbContext from the container per scope; it uses Add, so skip it when the entity's
    // repositories are already registered (by the application or an earlier call).
    private static void AddRepositories<TEntity, TDbContext>(IServiceCollection services)
        where TEntity : class, new()
        where TDbContext : DbContext
    {
        if (!services.Any(d => d.ServiceType == typeof(IReadRepository<TEntity>) || d.ServiceType == typeof(IWriteRepository<TEntity>)))
            services.AddEfCoreRepository<TEntity, TDbContext>(ServiceLifetime.Scoped);
    }
}
