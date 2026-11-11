<p align="center"><img src="https://raw.githubusercontent.com/kadirdemirkaya/SagaFlux/main/assets/icon.png" alt="SagaFlux logo" width="112" /></p>

# SagaFlux (EventFlux.Saga)

[![NuGet](https://img.shields.io/nuget/v/EventFlux.Saga.svg)](https://www.nuget.org/packages/EventFlux.Saga) [![Downloads](https://img.shields.io/nuget/dt/EventFlux.Saga.svg)](https://www.nuget.org/packages/EventFlux.Saga) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/kadirdemirkaya/SagaFlux/blob/main/LICENSE.txt)

Saga / process-manager state machines for the [EventFlux](https://github.com/kadirdemirkaya/EventFlux) mediator: in-process first, distributed as an option, with compensation and unit testing built in.

```bash
dotnet add package EventFlux.Saga
```

```csharp
public sealed record OrderPlaced(Guid OrderId, decimal Amount) : IEventRequest;
public sealed record PaymentReceived(Guid CorrelationId) : ICorrelatedEvent;
public sealed record ShipOrder(Guid OrderId) : IEventRequest;

public sealed class OrderState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = "";
    public int Version { get; set; }
    public decimal Amount { get; set; }
}

public sealed class OrderSaga : Saga<OrderState>
{
    public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));

    public OrderSaga()
    {
        CorrelateBy<OrderPlaced>(e => e.OrderId);

        During(SagaStates.Initial,
            When<OrderPlaced>().Then(ctx => ctx.State.Amount = ctx.Event.Amount).TransitionTo(AwaitingPayment));

        During(AwaitingPayment,
            When<PaymentReceived>().Publish(ctx => new ShipOrder(ctx.CorrelationId)).Finalize());
    }
}

services.AddLogging();
services.AddEventBus(typeof(Program).Assembly);
services.AddSagaFlux(typeof(Program).Assembly); // in memory and in-process until you add persistence or a transport
await dispatcher.PublishAsync(new OrderPlaced(orderId, 42m)); // IEventDispatcher or IEventBus
```

## What it is, what it is not

**It is** a state machine DSL with correlation, optimistic concurrency, compensation, durable timeouts, an outbox,
thin RabbitFlow / RedisFlow adapters, an inspector, a state generator and a test harness. Sagas never reference a transport.
**It is not** a broker client, a wire protocol of its own, a routing-slip engine or a visual designer.

## Packages

All packages target .NET 8, 9 and 10 with EventFlux 2.1.1 and share version **1.2.0**, with Source Link and symbols.

| Package | Purpose |
|---|---|
| `EventFlux.Saga` | Core: `Saga<TState>`, state machine DSL, in-process execution |
| `EventFlux.Saga.Testing` | Given/When/Then test harness |
| `EventFlux.Saga.EfCore` | EF Core persistence, timeouts and outbox on `EfCore.RepositoryPack` |
| `EventFlux.Saga.RabbitFlow` | RabbitMQ transport on `EventFlux.RabbitFlow` (>= 1.3.2) |
| `EventFlux.Saga.RedisFlow` | Redis Streams transport on `EventFlux.RedisFlow` (>= 1.1.1), optional Redis instance lock |
| `EventFlux.Saga.Generators` | State definitions generated from a `[SagaStateSet]` enum |
| `EventFlux.Saga.Inspector` | Transition timeline with JSON and HTML endpoints |
| `EventFlux.Saga.Inspector.EfCore` | Durable EF Core timeline store for the inspector |

## Documentation

- [Correlation and delivery](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/correlation-and-delivery.md): correlating events, duplicates
- [State machine](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/state-machine.md): branches, faults, joins, requests, compensation, timeouts, generated states
- [Persistence and outbox](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/persistence.md): in-memory, EF Core, outbox, errors after persistence
- [Transports and distributed lock](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/transports.md): RabbitMQ, Redis Streams, instance lock
- [Observability](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/observability.md): inspector, tracing, metrics, retention
- [Testing](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/testing.md): sagas without database, broker or DI
- [Extension points and stability](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/extension-points-and-stability.md): builder methods, options, stable formats

## License

MIT, see [LICENSE.txt](https://github.com/kadirdemirkaya/SagaFlux/blob/main/LICENSE.txt).
