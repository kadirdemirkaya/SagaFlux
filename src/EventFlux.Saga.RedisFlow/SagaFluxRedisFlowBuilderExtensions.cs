using EventFlux.Saga.RedisFlow;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the Redis Streams transport for SagaFlux.
/// </summary>
public static class SagaFluxRedisFlowBuilderExtensions
{
    /// <summary>
    /// Publishes saga events to the EventFlux.RedisFlow stream. Saga definitions do not change.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register EventFlux.RedisFlow itself with its <c>AddRedisEventQueue(configuration, assemblies)</c> method; this
    /// adapter only uses its <c>IRedisStreamPublisher</c>. No subscription step is needed: the RedisFlow worker reads
    /// every entry of the stream and dispatches it through EventFlux's <c>IEventBus</c>, which SagaFlux already routes
    /// to the sagas.
    /// </para>
    /// <para>
    /// Delivery guarantees come from EventFlux.RedisFlow: an entry whose saga step throws is not acknowledged; it stays
    /// pending, is retried after <c>RedisStreamOptions.RetryIdleTime</c> and moves to the dead-letter stream after
    /// <c>MaxDeliveryAttempts</c>.
    /// </para>
    /// </remarks>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseRedisFlow(this SagaFluxBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        // Hosted services start in registration order; put the resolver registration ahead of the RedisFlow worker.
        if (!services.Any(d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(RedisFlowSagaEventRegistration)))
            services.Insert(0, ServiceDescriptor.Singleton<IHostedService, RedisFlowSagaEventRegistration>());

        return builder.UseTransport<RedisFlowSagaTransport>(ServiceLifetime.Singleton);
    }

    /// <summary>
    /// Serializes the processing of each saga instance across nodes with a Redis lock (DistributedLock.Redis), on
    /// the <c>IConnectionMultiplexer</c> that EventFlux.RedisFlow registers.
    /// </summary>
    /// <remarks>
    /// Optional: optimistic concurrency already keeps instances consistent. The lock avoids repeated conflicting
    /// attempts when many events for the same instance are processed on several nodes at once. It works with any
    /// transport and repository.
    /// </remarks>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <param name="configure">Optional lock options.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseRedisDistributedLock(this SagaFluxBuilder builder, Action<RedisSagaLockOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        var options = services.LastOrDefault(d => d.ServiceType == typeof(RedisSagaLockOptions))?.ImplementationInstance as RedisSagaLockOptions;
        if (options is null)
        {
            options = new RedisSagaLockOptions();
            services.AddSingleton(options);
        }
        configure?.Invoke(options);

        return builder.UseLockProvider<RedisSagaLockProvider>();
    }
}
