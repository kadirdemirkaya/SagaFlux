using EventFlux.Abstractions;
using EventFlux.RabbitMQ;
using EventFlux.RabbitMQ.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;

namespace EventFlux.Saga.Integration.Test;

public sealed record RabbitOrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record RabbitPaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record RabbitChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;
public sealed record RabbitOrderExploded(Guid CorrelationId) : ICorrelatedEvent;

public sealed class RabbitOrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class RabbitOrderSaga : Saga<RabbitOrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public RabbitOrderSaga()
    {
        During(SagaStates.Initial,
            When<RabbitOrderPlaced>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Publish(ctx => new RabbitChargeCustomer(ctx.CorrelationId, ctx.State.Amount))
                .TransitionTo(AwaitingPayment),
            When<RabbitOrderExploded>().Then(_ => throw new InvalidOperationException("boom")));
        During(AwaitingPayment,
            When<RabbitPaymentReceived>().Finalize());
    }
}

/// <summary>The "payments service": charges the customer and answers through the broker.</summary>
public sealed class RabbitPaymentService(IEventBroker broker) : IEventHandler<RabbitChargeCustomer>
{
    public Task Handle(RabbitChargeCustomer @event, CancellationToken cancellationToken)
        => broker.PublishAsync(new RabbitPaymentReceived(@event.OrderId), cancellationToken);
}

public class RabbitFlowSagaIntegrationTests
{
    private static IHost BuildHost(string serviceName, Action<RabbitFlowOptions>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddEventFluxRabbitFlow(
            typeof(RabbitFlowSagaIntegrationTests).Assembly,
            new ConnectionFactory { HostName = RabbitMqFactAttribute.Host! },
            serviceName,
            o =>
            {
                o.AutoSubscribe = true;
                configure?.Invoke(o);
            });
        builder.Services.AddSagaFlux().AddSaga<RabbitOrderSaga, RabbitOrderState>().UseRabbitFlow();
        return builder.Build();
    }

    private static async Task DeleteTopologyAsync(string serviceName)
    {
        var factory = new ConnectionFactory { HostName = RabbitMqFactAttribute.Host! };
        await using var connection = await factory.CreateConnectionAsync();
        await using var channel = await connection.CreateChannelAsync();
        // AutoSubscribe declares a queue for every handled event of the assembly.
        var events = typeof(RabbitFlowSagaIntegrationTests).Assembly.GetTypes().Where(t => typeof(IEventRequest).IsAssignableFrom(t));
        foreach (var name in events.Select(t => t.Name))
        {
            await channel.QueueDeleteAsync($"{serviceName}_{name}");
            await channel.QueueDeleteAsync($"{serviceName}_{name}_dead_letter");
        }
        await channel.ExchangeDeleteAsync(serviceName);
        await channel.ExchangeDeleteAsync($"{serviceName}_dead_letter");
    }

    [RabbitMqFact]
    public async Task Saga_runs_to_completion_through_RabbitMQ()
    {
        var serviceName = $"sagaflux-it-{Guid.NewGuid():N}";
        using var host = BuildHost(serviceName);
        await host.StartAsync();
        try
        {
            var orderId = Guid.NewGuid();
            await host.Services.GetRequiredService<IEventBroker>().PublishAsync(new RabbitOrderPlaced(orderId, 42m));

            var repository = host.Services.GetRequiredService<ISagaRepository<RabbitOrderState>>();
            var state = await Eventually.WaitAsync(
                () => repository.FindAsync(orderId, default),
                s => s.CurrentState == SagaStates.Final.Name);

            Assert.Equal(42m, state.Amount);
            Assert.Equal(2, state.Version);
        }
        finally
        {
            await host.StopAsync();
            await DeleteTopologyAsync(serviceName);
        }
    }

    [RabbitMqFact]
    public async Task A_failing_saga_step_is_dead_lettered_not_lost()
    {
        var serviceName = $"sagaflux-it-{Guid.NewGuid():N}";
        using var host = BuildHost(serviceName, o => o.OnFailure = RabbitFlowFailureMode.DeadLetter);
        await host.StartAsync();
        try
        {
            var id = Guid.NewGuid();
            await host.Services.GetRequiredService<IEventBroker>().PublishAsync(new RabbitOrderExploded(id));

            var factory = new ConnectionFactory { HostName = RabbitMqFactAttribute.Host! };
            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();
            var deadLettered = await Eventually.WaitAsync(
                () => channel.BasicGetAsync($"{serviceName}_{nameof(RabbitOrderExploded)}_dead_letter", autoAck: true),
                _ => true);

            Assert.Contains(id.ToString(), System.Text.Encoding.UTF8.GetString(deadLettered.Body.Span));
            Assert.Null(await host.Services.GetRequiredService<ISagaRepository<RabbitOrderState>>().FindAsync(id, default));
        }
        finally
        {
            await host.StopAsync();
            await DeleteTopologyAsync(serviceName);
        }
    }
}
