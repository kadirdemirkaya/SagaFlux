using EventFlux.Saga;
using EventFlux.Saga.Inspector;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Registers the saga inspector.
/// </summary>
public static class SagaFluxInspectorBuilderExtensions
{
    /// <summary>
    /// Records every saga transition in an <see cref="ISagaTimelineStore"/> (in-memory and bounded unless another
    /// store is registered). Expose it with <c>app.MapSagaInspector()</c>.
    /// </summary>
    /// <param name="builder">The SagaFlux builder.</param>
    /// <param name="configure">Optional inspector options.</param>
    /// <returns>The builder.</returns>
    public static SagaFluxBuilder UseInspector(this SagaFluxBuilder builder, Action<SagaInspectorOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var services = builder.Services;

        var options = services.LastOrDefault(d => d.ServiceType == typeof(SagaInspectorOptions))?.ImplementationInstance as SagaInspectorOptions;
        if (options is null)
        {
            options = new SagaInspectorOptions();
            services.AddSingleton(options);
        }
        configure?.Invoke(options);

        services.TryAddSingleton<ISagaTimelineStore, InMemorySagaTimelineStore>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ISagaTransitionObserver, SagaTimelineObserver>());
        return builder;
    }
}
