using System.Reflection;
using EventFlux.Abstractions;
using EventFlux.Saga;
using EventFlux.Saga.Concurrency;
using EventFlux.Saga.Internal;
using EventFlux.Saga.Repositories;
using EventFlux.Saga.Timeouts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Configures SagaFlux: which sagas run, where their instances are stored and how their events are transported.
/// Returned by <see cref="SagaFluxServiceCollectionExtensions.AddSagaFlux(IServiceCollection, Action{SagaFluxOptions}?, Assembly[])"/>.
/// </summary>
public sealed class SagaFluxBuilder
{
    private readonly SagaEventCatalog _catalog;

    internal SagaFluxBuilder(IServiceCollection services, SagaEventCatalog catalog)
    {
        Services = services;
        _catalog = catalog;
    }

    /// <summary>
    /// Gets the service collection, for transport and persistence packages to extend.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers a saga definition.
    /// </summary>
    /// <remarks>
    /// The saga is created once and shared. Its state type uses <see cref="InMemorySagaRepository{TState}"/> unless
    /// another <see cref="ISagaRepository{TState}"/> is registered.
    /// </remarks>
    /// <typeparam name="TSaga">The saga type.</typeparam>
    /// <typeparam name="TState">The saga state type.</typeparam>
    /// <returns>This builder.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the saga cannot correlate one of its events.</exception>
    public SagaFluxBuilder AddSaga<TSaga, TState>()
        where TSaga : Saga<TState>, new()
        where TState : class, ISagaState, new()
    {
        if (Services.Any(d => d.ServiceType == typeof(TSaga)))
            return this;

        var saga = new TSaga();
        saga.Validate();

        Services.AddSingleton(saga);
        Services.TryAddSingleton<ISagaRepository<TState>, InMemorySagaRepository<TState>>();
        Services.AddTransient<ISagaRunner, SagaRunner<TSaga, TState>>();
        if (saga.TimeoutTypes.Count > 0)
            UseTimeoutPolling();

        foreach (var eventType in saga.EventTypes.Where(t => typeof(IEventRequest).IsAssignableFrom(t)))
        {
            _catalog.Add(eventType);
            Services.TryAddEnumerable(ServiceDescriptor.Transient(
                typeof(IEventHandler<>).MakeGenericType(eventType),
                typeof(SagaEventHandler<>).MakeGenericType(eventType)));
        }

        return this;
    }

    /// <summary>
    /// Registers every non-abstract saga type (with a public parameterless constructor) declared in the assemblies.
    /// </summary>
    /// <remarks>Private nested types are skipped.</remarks>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder AddSagas(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);
        var addSaga = typeof(SagaFluxBuilder).GetMethod(nameof(AddSaga))!;

        foreach (var type in assemblies.Distinct().SelectMany(SafeTypes))
        {
            if (type.IsAbstract || type.IsGenericTypeDefinition || type.IsNestedPrivate || type.GetConstructor(Type.EmptyTypes) is null)
                continue;
            var stateType = FindStateType(type);
            if (stateType is not null)
                addSaga.MakeGenericMethod(type, stateType).Invoke(this, BindingFlags.DoNotWrapExceptions, null, null, null);
        }

        return this;
    }

    /// <summary>
    /// Replaces the transport used to publish saga events (default: in-process through EventFlux).
    /// Broker packages such as <c>EventFlux.Saga.RabbitFlow</c> call this from their <c>UseXxx()</c> extension.
    /// </summary>
    /// <typeparam name="TTransport">The transport type.</typeparam>
    /// <param name="lifetime">The transport lifetime.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseTransport<TTransport>(ServiceLifetime lifetime = ServiceLifetime.Transient)
        where TTransport : class, ISagaTransport
    {
        Services.RemoveAll<ISagaTransport>();
        Services.Add(new ServiceDescriptor(typeof(ISagaTransport), typeof(TTransport), lifetime));
        return this;
    }

    /// <summary>
    /// Registers the hosted service that delivers due timeouts every <see cref="SagaFluxOptions.TimeoutPollingInterval"/>.
    /// </summary>
    /// <remarks>
    /// Added automatically when a registered saga declares <c>Schedule&lt;TTimeout&gt;(...)</c> on a transition. Call it
    /// yourself when timeouts are only scheduled from actions through <c>SagaContext.Schedule(...)</c>.
    /// </remarks>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseTimeoutPolling()
    {
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SagaTimeoutHostedService>());
        return this;
    }

    /// <summary>
    /// Replaces the store of pending timeouts (default: <see cref="EventFlux.Saga.Timeouts.InMemorySagaTimeoutStore"/>).
    /// </summary>
    /// <typeparam name="TStore">The store type.</typeparam>
    /// <param name="lifetime">The store lifetime.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseTimeoutStore<TStore>(ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where TStore : class, ISagaTimeoutStore
    {
        Services.RemoveAll<ISagaTimeoutStore>();
        Services.Add(new ServiceDescriptor(typeof(ISagaTimeoutStore), typeof(TStore), lifetime));
        return this;
    }

    /// <summary>
    /// Keeps published events in a transactional outbox (<typeparamref name="TOutbox"/>) and registers the hosted
    /// service that sends the ones left pending (see <see cref="ISagaOutbox"/>).
    /// </summary>
    /// <typeparam name="TOutbox">The outbox type; it must share its unit of work with the saga repository.</typeparam>
    /// <param name="lifetime">The outbox lifetime.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseOutbox<TOutbox>(ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where TOutbox : class, ISagaOutbox
    {
        Services.RemoveAll<ISagaOutbox>();
        Services.Add(new ServiceDescriptor(typeof(ISagaOutbox), typeof(TOutbox), lifetime));
        Services.TryAddSingleton<ISagaOutboxDispatcher, EventFlux.Saga.Outbox.SagaOutboxDispatcher>();
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, EventFlux.Saga.Outbox.SagaOutboxHostedService>());
        return this;
    }

    /// <summary>
    /// Replaces the per-instance lock (default: <see cref="NoSagaLockProvider"/>, optimistic concurrency only).
    /// Distributed transport packages offer ready-made providers, for example <c>UseRedisDistributedLock()</c>.
    /// </summary>
    /// <typeparam name="TProvider">The lock provider type.</typeparam>
    /// <param name="lifetime">The provider lifetime.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseLockProvider<TProvider>(ServiceLifetime lifetime = ServiceLifetime.Singleton)
        where TProvider : class, ISagaLockProvider
    {
        Services.RemoveAll<ISagaLockProvider>();
        Services.Add(new ServiceDescriptor(typeof(ISagaLockProvider), typeof(TProvider), lifetime));
        return this;
    }

    /// <summary>
    /// Replaces the repository of <typeparamref name="TState"/>.
    /// </summary>
    /// <typeparam name="TState">The saga state type.</typeparam>
    /// <typeparam name="TRepository">The repository type.</typeparam>
    /// <param name="lifetime">The repository lifetime.</param>
    /// <returns>This builder.</returns>
    public SagaFluxBuilder UseRepository<TState, TRepository>(ServiceLifetime lifetime = ServiceLifetime.Scoped)
        where TState : class, ISagaState
        where TRepository : class, ISagaRepository<TState>
    {
        Services.RemoveAll<ISagaRepository<TState>>();
        Services.Add(new ServiceDescriptor(typeof(ISagaRepository<TState>), typeof(TRepository), lifetime));
        return this;
    }

    private static Type? FindStateType(Type type)
    {
        for (var t = type.BaseType; t is not null; t = t.BaseType)
        {
            if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Saga<>))
                return t.GetGenericArguments()[0];
        }
        return null;
    }

    private static IEnumerable<Type> SafeTypes(Assembly assembly)
    {
        try { return assembly.GetTypes(); }
        catch (ReflectionTypeLoadException ex) { return ex.Types.OfType<Type>(); }
    }
}
