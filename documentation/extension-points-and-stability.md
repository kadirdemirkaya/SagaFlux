# Extension points and stability

Replacing defaults, tuning options, and the formats that are part of the public contract.

## Extension points and options

`AddSagaFlux(...)` returns a `SagaFluxBuilder`; every default can be replaced from it.

| Builder method | What it does | Default lifetime |
|---|---|---|
| `AddSaga<TSaga, TState>()` | Registers one saga | — |
| `AddSagas(params Assembly[] assemblies)` | Registers every saga with a public parameterless constructor in the assemblies (`AddSagaFlux(assemblies)` already calls it) | — |
| `UseRepository<TState, TRepository>(lifetime)` | Replaces the `ISagaRepository<TState>` of one state type (default: in-memory) | `Scoped` |
| `UseTransport<TTransport>(lifetime)` | Replaces the `ISagaTransport` that sends published events (default: in-process) | `Transient` |
| `UseTimeoutStore<TStore>(lifetime)` | Replaces the `ISagaTimeoutStore` (default: in-memory) | `Scoped` |
| `UseOutbox<TOutbox>(lifetime)` | Stages published events in an `ISagaOutbox` and registers the outbox dispatcher | `Scoped` |
| `UseLockProvider<TProvider>(lifetime)` | Replaces the `ISagaLockProvider` (default: none, optimistic concurrency only) | `Singleton` |
| `UseTimeoutPolling()` | Registers the timeout hosted service; added automatically when a saga declares `Schedule`, call it when timeouts are only scheduled from actions with `ctx.Schedule(...)` | — |

`SagaFluxOptions` (the `configure` argument of `AddSagaFlux(configure, assemblies)`):

- `MaxConcurrencyRetries`: attempts on a version conflict.
- `EnableTimeoutPolling` (default `true`): set `false` to call `ISagaTimeoutDispatcher.DispatchDueAsync()`
  yourself. `TimeoutPollingInterval`, `TimeoutBatchSize` and `TimeoutRetryDelay` tune the hosted service.
- `EnableOutboxPolling` (default `true`): set `false` to call `ISagaOutboxDispatcher.DispatchPendingAsync()`
  yourself. `OutboxPollingInterval`, `OutboxDispatchDelay` and `OutboxBatchSize` tune the hosted service.

Adapter options:

- `UseRabbitFlow(o => ...)`: `ExchangeName` (default: the EventFlux.RabbitFlow default exchange) and
  `SubscribeToSagaEvents` (default `true`; set `false` when the events the sagas react to are subscribed
  elsewhere).
- `UseRedisDistributedLock(o => ...)`: `KeyPrefix` (default `sagaflux:lock:`, key `{prefix}{saga}:{correlation id}`),
  `AcquireTimeout` (default 30 seconds) and `Expiry` (lifetime of a lock whose holder stopped renewing it;
  `null` keeps the library default of 30 seconds).

## Stability and identity formats

These formats are part of the public contract and stay as they are:

- **Saga name:** the short saga type name (`OrderSaga`) is used in the distributed lock key, the activity name
  and tags, and the inspector timeline.
- **Timeout key:** the full name of the timeout event type (`SagaTimeoutKeys.For<T>()`), so scheduling the same
  type again replaces the pending timeout.
- **Persisted event type name:** `"Namespace.Type, Assembly"` without a version (`SagaEventTypeNames.GetName`),
  used for EF Core timeouts and outbox rows. This format is permanently frozen; rename or move a type with
  `[SagaEventFormerName(...)]`.

Infrastructure types:

- `ISagaEventRouter`, `ISagaEventCatalog` and `SagaEventTypeNames` are public so that adapters and stores can use
  them; application code does not need them.
- The lock abstraction (`ISagaLockProvider`, `NoSagaLockProvider`) lives in the `EventFlux.Saga.Concurrency`
  namespace.
- Register the EF Core and transport adapters through `UseEfCore<TState, TDbContext>()`,
  `UseEfCoreTimeouts<TDbContext>()`, `UseEfCoreOutbox<TDbContext>()`, `UseRabbitFlow()`, `UseRedisFlow()` and
  `UseRedisDistributedLock()` instead of constructing their types yourself.
- A few `Saga<TState>` and `SagaPublishedEvent` members that the runtime calls (`Handles`, `GetCorrelationId`,
  `Validate`, `EventTypes`, `TimeoutTypes`, `CompensationKeys`, `RemovesWhenFinal`, `FromEvent`, `SendAsync`) are
  hidden from IntelliSense. They remain public and keep working.

See also: [Persistence and outbox](persistence.md), [Transports and distributed lock](transports.md).

---

[Documentation index](README.md)
