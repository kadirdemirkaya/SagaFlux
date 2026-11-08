# SagaFlux (EventFlux.Saga)

A lightweight, MIT-licensed saga / process-manager layer for the [EventFlux](https://github.com/kadirdemirkaya/EventFlux) mediator. Sagas run in-process first, with distribution as an option, and compensation and isolated unit testing are first-class features.

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
            When<OrderPlaced>()
                .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
                .TransitionTo(AwaitingPayment));

        During(AwaitingPayment,
            When<PaymentReceived>()
                .Publish(ctx => new ShipOrder(ctx.CorrelationId))
                .Finalize());
    }
}
```

```csharp
services.AddLogging();
services.AddEventBus(typeof(Program).Assembly);
services.AddSagaFlux(typeof(Program).Assembly);

await dispatcher.PublishAsync(new OrderPlaced(orderId, 42m));
```

Events reach the sagas whether they are published through `IEventDispatcher` or `IEventBus`. One dispatch
runs each matching saga once, even though both paths are wired. By default instances live in memory and
published events stay in-process. For what happens when a broker redelivers a message, see
[Delivery and duplicates](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/correlation-and-delivery.md#delivery-and-duplicates).

## What it is, what it is not

> SagaFlux is a lightweight library that lets you implement the saga / process-manager pattern in the EventFlux
> ecosystem without a heavy broker topology or a commercial license. It runs in-process first and distributed
> as an option, and it makes testability and compensation logic first-class citizens.

**It is:**
- a state machine DSL (`During` / `When` / `Then` / `Publish` / `TransitionTo`)
- correlation and optimistic concurrency
- compensating actions
- durable timeouts (`Schedule` / `Unschedule`)
- a transactional outbox for published events
- thin transport adapters for RabbitFlow and RedisFlow
- a transition timeline inspector
- a source generator for state definitions
- a Given/When/Then test harness

**It is not:**
- a broker client or wire protocol of its own: publishing always delegates to EventFlux (in-process) or to the
  RabbitFlow / RedisFlow message format
- a routing-slip engine
- a visual designer

The saga definition never references a transport. Publishing goes through the small `ISagaTransport` seam, so
switching to a broker only changes the registration.

## Packages

All packages are at **1.0.0** and share one version. Versioning follows SemVer: the public API is stable,
and a breaking change needs a new major version.

| Package | Purpose | Version |
|---|---|---|
| `EventFlux.Saga` | Core: `Saga<TState>`, state machine DSL, in-process execution | 1.0.0 |
| `EventFlux.Saga.Testing` | Given/When/Then test harness | 1.0.0 |
| `EventFlux.Saga.EfCore` | Persistence on top of `EfCore.RepositoryPack` | 1.0.0 |
| `EventFlux.Saga.RabbitFlow` | RabbitMQ transport adapter on `EventFlux.RabbitFlow` (>= 1.3.2) | 1.0.0 |
| `EventFlux.Saga.RedisFlow` | Redis Streams transport adapter on `EventFlux.RedisFlow` (>= 1.1.1), optional Redis instance lock | 1.0.0 |
| `EventFlux.Saga.Generators` | Compile-time state definitions from a `[SagaStateSet]` enum | 1.0.0 |
| `EventFlux.Saga.Inspector` | Transition timeline with JSON and HTML endpoints | 1.0.0 |
| `EventFlux.Saga.Inspector.EfCore` | Durable EF Core timeline store for the inspector | 1.0.0 |

All packages target .NET 8 and EventFlux 2.1.1. .NET 8 is the baseline because it is the current LTS release
that EventFlux 2.x, RabbitFlow and RedisFlow support; other target frameworks can be added on request.
Packages ship with Source Link and `.snupkg` symbols.

## Documentation

Detailed topics live in separate pages ([index](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/README.md)):

- [Correlation and delivery](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/correlation-and-delivery.md): correlating events, delivery and duplicates
- [State machine](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/state-machine.md): more building blocks, waiting for several events, asking for a decision, compensating actions, timeouts, generated states
- [Persistence and outbox](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/persistence.md): in-memory persistence, EF Core, transactional outbox, errors after persistence and redelivery
- [Transports and distributed lock](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/transports.md): RabbitMQ, Redis Streams, distributed instance lock
- [Observability and retention](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/observability.md): inspector, tracing, metrics, retention
- [Testing](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/testing.md): testing a saga with no database, no broker and no DI
- [Extension points and stability](https://github.com/kadirdemirkaya/SagaFlux/blob/main/documentation/extension-points-and-stability.md): builder methods, options, identity formats, infrastructure types

## Building from source

```bash
dotnet build EventFlux.Saga.sln -m:1
dotnet test EventFlux.Saga.sln -m:1
```

The end-to-end tests in `test/EventFlux.Saga.Integration.Test` need real brokers and are skipped unless these
variables are set:

```bash
SAGAFLUX_RABBITMQ_HOST=localhost SAGAFLUX_REDIS=localhost:6379 dotnet test test/EventFlux.Saga.Integration.Test
```

## License

MIT — see [LICENSE.txt](https://github.com/kadirdemirkaya/SagaFlux/blob/main/LICENSE.txt).
