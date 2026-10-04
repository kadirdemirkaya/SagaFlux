using System.Reflection;
using EventFlux.Saga;
using EventFlux.Saga.Concurrency;
using EventFlux.Saga.Internal;
using EventFlux.Saga.Timeouts;
using EventFlux.Saga.Transports;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers SagaFlux on top of EventFlux.
/// </summary>
public static class SagaFluxServiceCollectionExtensions
{
    /// <summary>
    /// Registers SagaFlux and the sagas declared in <paramref name="assemblies"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Also registers the EventFlux <c>IEventDispatcher</c> (<c>AddEventDispatcher()</c>) and the
    /// <see cref="SagaCorrelationBehavior{TEvent}"/> / <see cref="SagaCorrelationBehavior{TRequest, TResponse}"/>
    /// pipeline behaviors. Call EventFlux's <c>AddEventBus(...)</c> for your own handlers and <c>AddLogging()</c> as usual.
    /// </para>
    /// <para>
    /// By default sagas run in-process (<see cref="InProcessSagaTransport"/>) with in-memory instances.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional runtime options.</param>
    /// <param name="assemblies">Assemblies to scan for sagas; more can be added with <see cref="SagaFluxBuilder.AddSaga{TSaga, TState}"/>.</param>
    /// <returns>A builder to configure sagas, persistence and transport.</returns>
    public static SagaFluxBuilder AddSagaFlux(this IServiceCollection services, Action<SagaFluxOptions>? configure, params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = services.LastOrDefault(d => d.ServiceType == typeof(SagaFluxOptions))?.ImplementationInstance as SagaFluxOptions;
        if (options is null)
        {
            options = new SagaFluxOptions();
            services.AddSingleton(options);
        }
        configure?.Invoke(options);

        services.AddEventDispatcher();
        services.AddEventOpenBehavior(typeof(SagaCorrelationBehavior<>));
        services.AddEventOpenBehavior(typeof(SagaCorrelationBehavior<,>));
        services.TryAddTransient<ISagaEventRouter, SagaEventRouter>();
        services.TryAddTransient<ISagaTransport, InProcessSagaTransport>();
        services.TryAddTransient<ISagaRequester, EventFluxSagaRequester>();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<ISagaLockProvider, NoSagaLockProvider>();
        services.TryAddSingleton<ISagaTimeoutStore, InMemorySagaTimeoutStore>();
        services.TryAddSingleton<ISagaTimeoutDispatcher, SagaTimeoutDispatcher>();

        var catalog = services.LastOrDefault(d => d.ServiceType == typeof(ISagaEventCatalog))?.ImplementationInstance as SagaEventCatalog;
        if (catalog is null)
        {
            catalog = new SagaEventCatalog();
            services.AddSingleton<ISagaEventCatalog>(catalog);
        }

        var builder = new SagaFluxBuilder(services, catalog);
        if (assemblies is { Length: > 0 })
            builder.AddSagas(assemblies);
        return builder;
    }

    /// <summary>
    /// Registers SagaFlux and the sagas declared in <paramref name="assemblies"/> with default options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="assemblies">Assemblies to scan for sagas.</param>
    /// <returns>A builder to configure sagas, persistence and transport.</returns>
    public static SagaFluxBuilder AddSagaFlux(this IServiceCollection services, params Assembly[] assemblies)
        => services.AddSagaFlux(null, assemblies);
}
