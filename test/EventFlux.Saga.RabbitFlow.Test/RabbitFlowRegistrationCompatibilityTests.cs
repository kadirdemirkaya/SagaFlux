using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;

namespace EventFlux.Saga.RabbitFlow.Test;

public class RabbitFlowRegistrationCompatibilityTests
{
    [Fact]
    public void RabbitFlow_registration_runs_next_to_EventFlux_2_with_a_host_lifetime_broker()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        services.AddEventFluxRabbitFlow(typeof(RabbitFlowRegistrationCompatibilityTests).Assembly, new ConnectionFactory(), "orders");
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseRabbitFlow();

        Assert.Equal(ServiceLifetime.Singleton, Assert.Single(services, d => d.ServiceType == typeof(IEventBroker)).Lifetime);
        Assert.Contains(services, d => d.ServiceType == typeof(EventFlux.Abstractions.IEventBus));
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }
}
