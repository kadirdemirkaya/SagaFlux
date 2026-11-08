# Transports and distributed lock

Running sagas across services with the RabbitFlow and RedisFlow adapters, and the optional instance lock.

## Distributed sagas (RabbitMQ)

```bash
dotnet add package EventFlux.Saga.RabbitFlow
```

```csharp
services.AddEventFluxRabbitFlow(typeof(Program).Assembly, connectionFactory, "orders");
services.AddSagaFlux(typeof(Program).Assembly)
        .UseRabbitFlow(options => options.ExchangeName = "sagas");
```

- Saga events are published through the EventFlux.RabbitFlow broker.
- On startup, a hosted service subscribes to every event the sagas react to, so an instance can be driven
  by events from other services. The saga definitions stay unchanged.
- The broker is a singleton, so subscriptions live until the host stops.
- A message whose saga step throws is not acknowledged: EventFlux.RabbitFlow requeues it or moves it to a
  dead-letter queue (`RabbitFlowOptions.OnFailure`, `MaxDeliveryAttempts`). Messages are not lost.

## Distributed sagas (Redis Streams)

```bash
dotnet add package EventFlux.Saga.RedisFlow
```

```csharp
services.AddRedisEventQueue(configuration, typeof(Program).Assembly); // reads the "RedisStream" section
services.AddSagaFlux(typeof(Program).Assembly)
        .UseRedisFlow();
```

- Saga events are appended to the EventFlux.RedisFlow stream with its typed `PublishAsync<TEvent>`, in the
  package's own format.
- No subscription step is needed: the RedisFlow worker reads the stream and dispatches through `IEventBus`,
  which SagaFlux routes to the sagas.
- A failing saga step leaves the entry pending. RedisFlow retries it after `RetryIdleTime` and moves it to the
  dead-letter stream after `MaxDeliveryAttempts`.

## Distributed instance lock (optional)

```csharp
services.AddSagaFlux(typeof(Program).Assembly)
        .UseRabbitFlow()                      // any transport
        .UseRedisDistributedLock(o => o.AcquireTimeout = TimeSpan.FromSeconds(10));
```

- Optimistic concurrency already keeps instances consistent. The lock only stops nodes from repeating
  conflicting work when many events for the same instance arrive at once.
- It builds on a mature third-party distributed lock library, on the RedisFlow connection. SagaFlux does not
  implement a lock of its own.
- The lock covers loading, executing and saving an instance. Published events are sent after it is released.
- Any other `ISagaLockProvider` can be plugged in with `UseLockProvider<T>()`.

See also: [Delivery and duplicates](correlation-and-delivery.md#delivery-and-duplicates), [Extension points and options](extension-points-and-stability.md#extension-points-and-options) (adapter options).

---

[Documentation index](README.md)
