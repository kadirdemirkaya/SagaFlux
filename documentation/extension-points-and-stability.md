# Extension points and stability

Replacing defaults, tuning options, and the formats that stay stable.

## Extension points and options

`AddSagaFlux(...)` returns a `SagaFluxBuilder`:

| Builder method | Replaces / registers | Default lifetime |
|---|---|---|
| `AddSaga<TSaga, TState>()` / `AddSagas(assemblies)` | One saga / every saga with a public parameterless constructor (`AddSagaFlux(assemblies)` calls `AddSagas`) | — |
| `AddSaga<TSaga, TState>(saga)` | An already created saga, for constructors with parameters | — |
| `UseRepository<TState, TRepository>(lifetime)` | `ISagaRepository<TState>` (default: in-memory) | `Scoped` |
| `UseTransport<TTransport>(lifetime)` | `ISagaTransport` (default: in-process) | `Transient` |
| `UseTimeoutStore<TStore>(lifetime)` | `ISagaTimeoutStore` (default: in-memory) | `Scoped` |
| `UseOutbox<TOutbox>(lifetime)` | `ISagaOutbox` plus the outbox dispatcher | `Scoped` |
| `UseLockProvider<TProvider>(lifetime)` | `ISagaLockProvider` (default: none) | `Singleton` |
| `UseTimeoutPolling()` | Timeout hosted service; automatic with `Schedule`, call it when only `ctx.Schedule(...)` is used | — |

`SagaFluxOptions` (`AddSagaFlux(configure, assemblies)`):

- `MaxConcurrencyRetries`: attempts on a version conflict.
- `EnableTimeoutPolling` (default `true`; `false` = call `ISagaTimeoutDispatcher.DispatchDueAsync()` yourself),
  `TimeoutPollingInterval`, `TimeoutBatchSize`, `TimeoutRetryDelay`.
- `EnableOutboxPolling` (default `true`; `false` = call `ISagaOutboxDispatcher.DispatchPendingAsync()` yourself),
  `OutboxPollingInterval`, `OutboxDispatchDelay`, `OutboxBatchSize`.

Adapter options:

- `UseRabbitFlow(o => ...)`: `ExchangeName` (default: the EventFlux.RabbitFlow exchange), `SubscribeToSagaEvents`
  (default `true`; `false` when the saga events are subscribed elsewhere).
- `UseRedisDistributedLock(o => ...)`: `KeyPrefix` (default `sagaflux:lock:`, key `{prefix}{saga}:{correlation id}`),
  `AcquireTimeout` (default 30 s), `Expiry` (lock lifetime without renewal; `null` = 30 s).

## Stability and identity formats

These formats are part of the public contract:

- **Saga name:** the short type name (`OrderSaga`), used in the lock key, activity name/tags and the timeline.
- **Timeout key:** the full timeout event type name (`SagaTimeoutKeys.For<T>()`).
- **Persisted event type name:** `"Namespace.Type, Assembly"` without a version (`SagaEventTypeNames.GetName`),
  used by EF Core timeouts and outbox rows. Rename or move a type with `[SagaEventFormerName(...)]`.

Infrastructure types (`ISagaEventRouter`, `ISagaEventCatalog`, `SagaEventTypeNames`, `ISagaLockProvider` /
`NoSagaLockProvider` in `EventFlux.Saga.Concurrency`) exist for adapters and stores. Register adapters with their
`Use…()` methods instead of constructing their types. Runtime members of `Saga<TState>` and `SagaPublishedEvent`
(`Handles`, `GetCorrelationId`, `Validate`, `EventTypes`, `TimeoutTypes`, `CompensationKeys`, `RemovesWhenFinal`,
`FromEvent`, `SendAsync`) are hidden from IntelliSense but remain public.
