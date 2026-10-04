using System.Collections.Concurrent;
using EventFlux.Abstractions;
using EventFlux.RabbitMQ.Abstractions;

namespace EventFlux.Saga.RabbitFlow.Test;

public sealed record OrderPlaced(Guid CorrelationId, decimal Amount) : ICorrelatedEvent;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ChargeCustomer(Guid OrderId, decimal Amount) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public OrderSaga()
    {
        During(SagaStates.Initial,
            When<OrderPlaced>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .Publish(ctx => new ChargeCustomer(ctx.CorrelationId, ctx.State.Amount))
                .TransitionTo(AwaitingPayment));
        During(AwaitingPayment,
            When<PaymentReceived>().Finalize());
    }
}

public sealed class FakeBroker : IEventBroker, IDisposable
{
    public ConcurrentQueue<(IEventRequest Event, string? Exchange)> Published { get; } = new();
    public ConcurrentQueue<CancellationToken> PublishTokens { get; } = new();
    public ConcurrentQueue<(Type Event, Type Handler, string? Exchange)> Subscriptions { get; } = new();
    public bool Disposed { get; private set; }

    public Task PublishAsync(IEventRequest @event)
    {
        Published.Enqueue((@event, null));
        return Task.CompletedTask;
    }

    public Task PublishAsync(IEventRequest @event, string exchangeName)
    {
        Published.Enqueue((@event, exchangeName));
        return Task.CompletedTask;
    }

    public Task PublishAsync(IEventRequest @event, CancellationToken cancellationToken)
    {
        PublishTokens.Enqueue(cancellationToken);
        return PublishAsync(@event);
    }

    public Task PublishAsync(IEventRequest @event, string exchangeName, CancellationToken cancellationToken)
    {
        PublishTokens.Enqueue(cancellationToken);
        return PublishAsync(@event, exchangeName);
    }

    public Task SubscribeAsync<T, TH>(string exchangeName = null!) where T : IEventRequest where TH : IEventHandler<T>
    {
        Subscriptions.Enqueue((typeof(T), typeof(TH), exchangeName));
        return Task.CompletedTask;
    }

    public void Unsubscribe<T, TH>() where T : IEventRequest where TH : IEventHandler<T>
    {
    }

    public void Dispose() => Disposed = true;
}
