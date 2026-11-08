# Transports and distributed lock

Running sagas across services with the RabbitFlow and RedisFlow adapters. Saga definitions do not change.

## RabbitMQ (RabbitFlow)

```bash
dotnet add package EventFlux.Saga.RabbitFlow
```

```csharp
services.AddEventFluxRabbitFlow(typeof(Program).Assembly, connectionFactory, "orders");
services.AddSagaFlux(typeof(Program).Assembly).UseRabbitFlow(options => options.ExchangeName = "sagas");
```

- Saga events are published through the EventFlux.RabbitFlow broker; a hosted service subscribes to every event
  the sagas react to until the host stops.
- A message whose saga step throws is not acknowledged: RabbitFlow requeues or dead-letters it
  (`RabbitFlowOptions.OnFailure`, `MaxDeliveryAttempts`).

## Redis Streams (RedisFlow)

```bash
dotnet add package EventFlux.Saga.RedisFlow
```

```csharp
services.AddRedisEventQueue(configuration, typeof(Program).Assembly); // reads the "RedisStream" section
services.AddSagaFlux(typeof(Program).Assembly).UseRedisFlow();
```

- Saga events are appended to the EventFlux.RedisFlow stream; its worker dispatches them through `IEventBus`,
  so no subscription step is needed.
- A failing saga step leaves the entry pending; RedisFlow retries it after `RetryIdleTime` and dead-letters it
  after `MaxDeliveryAttempts`.

## Distributed instance lock (optional)

```csharp
services.AddSagaFlux(typeof(Program).Assembly)
        .UseRabbitFlow()
        .UseRedisDistributedLock(o => o.AcquireTimeout = TimeSpan.FromSeconds(10));
```

- Optimistic concurrency already keeps instances consistent; the lock only avoids repeated conflicting work when
  many events for one instance arrive at once. It works with any transport and uses the RedisFlow connection.
- It covers load, execute and save; published events are sent after release. Another provider:
  `UseLockProvider<T>()`. Options: [Extension points](extension-points-and-stability.md#extension-points-and-options).

See also: [Delivery and duplicates](correlation-and-delivery.md#delivery-and-duplicates).
