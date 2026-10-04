using EventFlux.Saga.RabbitFlow;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the RabbitFlow transport for SagaFlux.
/// </summary>
public static class SagaFluxRabbitFlowBuilderExtensions
{
    /// <summary>
    /// Publishes saga events through EventFlux.RabbitFlow and, by default, subscribes to the events the registered
    /// sagas react to, so sagas can run across services. Saga definitions do not change.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Register EventFlux.RabbitFlow itself with its <c>AddEventFluxRabbitFlow(...)</c> method; this adapter only uses
    /// its <c>IEventBroker</c>, which EventFlux.RabbitFlow registers as a singleton.
    /// </para>
    /// <para>
    /// Delivery guarantees come from EventFlux.RabbitFlow: a message whose saga step throws is not acknowledged; it is
    /// requeued or dead-lettered according to <c>RabbitFlowOptions.OnFailure</c> and <c>MaxDeliveryAttempts</c>.
    /// </para>
    /// </remarks>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <param name="configure">Optional transport options.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseRabbitFlow(this SagaFluxBuilder builder, Action<RabbitFlowSagaOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        var options = services.LastOrDefault(d => d.ServiceType == typeof(RabbitFlowSagaOptions))?.ImplementationInstance as RabbitFlowSagaOptions;
        if (options is null)
        {
            options = new RabbitFlowSagaOptions();
            services.AddSingleton(options);
        }
        configure?.Invoke(options);

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, RabbitFlowSagaSubscriber>());
        return builder.UseTransport<RabbitFlowSagaTransport>(ServiceLifetime.Singleton);
    }
}
