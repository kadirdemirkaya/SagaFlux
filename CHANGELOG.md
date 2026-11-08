# Changelog

All packages of the repository share one version. The release number and the NuGet publish are chosen by the
repository owner.

## 1.1.0 - 2026-11-08

- Every package now targets .NET 8, 9 and 10 (`net8.0`, `net9.0`, `net10.0`); `EventFlux.Saga.Generators` stays `netstandard2.0`.
- `EventFlux.Saga` depends on `Microsoft.Extensions.Hosting.Abstractions` 8.0.1 / 9.0.20 / 10.0.12 per target framework.
- No public API or behaviour changes.

## 1.0.0 - 2026-11-08

First stable release of the EventFlux.Saga package family. Every package targets .NET 8 and EventFlux 2.1.1.

### EventFlux.Saga
- State machine DSL: `During` / `When` / `Then` / `ThenAsync` / `Publish` / `TransitionTo` / `Finalize` with `SagaState` values.
- `DuringActive`, `.Branch`, `.OnFault<TException>` (`ctx.Fault`), `WhenAll<…>` (`IJoinableSagaState`), `.Ask` / `ctx.AskAsync`, `RemoveWhenFinal()`.
- Correlation via `ICorrelatedEvent` or `CorrelateBy<TEvent>`; events arrive through `IEventDispatcher` or `IEventBus`.
- Optimistic concurrency with bounded retries (`SagaFluxOptions.MaxConcurrencyRetries`) and an optional per-instance lock (`ISagaLockProvider`).
- Compensating actions: string-keyed LIFO stack, `OnCompensate` / `Compensate()` / `[CompensatingEvent]`.
- Durable timeouts: `Schedule<T>` / `Unschedule<T>`, polling hosted service on `TimeProvider`.
- Transactional outbox seam: `ISagaOutbox`, `UseOutbox<T>()`, outbox dispatcher.
- Observability: `ISagaTransitionObserver`, `EventFlux.Saga` activity source and optional dispatcher metrics.
- `[SagaEventFormerName]` keeps renamed event types resolving from stored names.

### EventFlux.Saga.Testing
- Given/When/Then harness with framework-independent assertions for state, published events, compensations,
  timeouts, faults, removal and requests; `Answer<TRequest, TResponse>` for `Ask`.

### EventFlux.Saga.EfCore
- Saga persistence, durable timeouts and a transactional outbox (`UseEfCore`, `UseEfCoreTimeouts`, `UseEfCoreOutbox`)
  on EfCore.RepositoryPack 1.3.0.

### EventFlux.Saga.RabbitFlow
- RabbitMQ transport on EventFlux.RabbitFlow 1.3.2 (`UseRabbitFlow`).

### EventFlux.Saga.RedisFlow
- Redis Streams transport on EventFlux.RedisFlow 1.1.1 (`UseRedisFlow`) and a Redis instance lock (`UseRedisDistributedLock`).

### EventFlux.Saga.Generators
- `[SagaStateSet]` enum to reflection-free state definitions, with diagnostics SAGA001–SAGA007.

### EventFlux.Saga.Inspector / EventFlux.Saga.Inspector.EfCore
- Transition timeline with HTML and JSON endpoints (`MapSagaInspector`) and a durable EF Core store (`UseEfCoreTimeline`).

### Packaging
- Source Link and `.snupkg` symbols for every library package; the project icon in every package.
