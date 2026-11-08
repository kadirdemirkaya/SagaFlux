# Changelog

All packages of the repository share one version. The release number and the NuGet publish are chosen by the
repository owner.

## Unreleased

First release candidate of the EventFlux.Saga package family. Every package targets .NET 8 and EventFlux 2.1.1.

### EventFlux.Saga
- Clarify existing post-persistence error boundaries: exceptions need not mean rollback; failed final removal
  without an outbox is not guaranteed to recover on redelivery, and post-send outbox completion errors can
  propagate while pending messages can be sent again. No runtime behavior or recovery policy changes.
- Fix `InMemorySagaTimeoutStore` to reject pre-cancelled calls to schedule, cancel, cancel-all, get-due and
  complete with `OperationCanceledException`, preserving pending timeouts and count; keep schedule's null validation first.
- Fix `InMemorySagaRepository<TState>.SaveAsync` to preserve the caller's `Version` and stored data when JSON
  serialization fails, propagate the original exception, and allow retrying the same object after fixing serialization.
- State machine DSL: `During` / `When` / `Then` / `ThenAsync` / `Publish` / `TransitionTo` / `Finalize`, with
  `SagaState` values instead of string literals.
- Correlation through `ICorrelatedEvent` or `CorrelateBy<TEvent>(...)`, optimistic concurrency with bounded
  retries (`SagaFluxOptions.MaxConcurrencyRetries`).
- Events reach the sagas whether they are published through `IEventDispatcher` or `IEventBus`; one dispatch runs each saga once (broker transports deliver at least once, see README "Delivery and duplicates").
- Compensating actions: string-keyed, append-only stack unwound in LIFO order; `[CompensatingEvent]`.
- Timeouts: `Schedule<T>` / `Unschedule<T>`, in-memory store and polling hosted service driven by `TimeProvider`.
- `DuringActive(...)`: transitions valid in every started, non-final state.
- `.Branch(condition, then, otherwise)`: conditional step chains inside a transition.
- `.OnFault<TException>(...)` and `SagaContext.Fault`: handle a failing step inside the saga; the failed steps'
  effects are discarded.
- `WhenAll<TA, TB>()` / `WhenAll<TA, TB, TC>()` with `IJoinableSagaState`: a transition that runs once several
  events have arrived, in any order; EF Core stores the pending events as a JSON column.
- `.Ask(request, onResponse)` and `SagaContext.AskAsync(...)`: send an EventFlux request from a transition and use
  the response (`ISagaRequester`); `SagaConfigurationException` for setup errors, never handled by `OnFault`.
- Transactional outbox: `ISagaOutbox`, `UseOutbox<T>()`, `ISagaOutboxDispatcher` and a polling hosted service
  (`OutboxPollingInterval`, `OutboxDispatchDelay`, `OutboxBatchSize`); transport send errors are logged for
  recovery except caller cancellation. Post-save cleanup and outbox completion errors can still propagate.
- `RemoveWhenFinal()` and `ISagaRepository.RemoveAsync` (default: not supported; in-memory and EF Core implement it).
- Transition observers (`ISagaTransitionObserver`); records carry the handled `Fault` and whether the instance was
  `Removed`.
- Tracing through the `EventFlux.Saga` `ActivitySource` (`SagaFluxDiagnostics`): one activity per saga and
  event, with transition tags; works with OpenTelemetry without extra packages.
- `[SagaEventFormerName("...")]` and `SagaEventTypeNames`: renamed or moved event types keep resolving from
  stored names (for example pending EF Core timeouts).
- Optional per-instance lock (`ISagaLockProvider`, `UseLockProvider<T>()`); published events are sent after the
  lock is released.

### EventFlux.Saga.Testing
- Given/When/Then harness with framework-independent assertions for state, published events, compensations,
  timeouts, handled faults (`HaveHandledFault<T>()`) and removal (`BeRemoved()`).
- `Answer<TRequest, TResponse>(...)` answers `Ask` requests without handlers; `HaveAsked<T>()` / `NotHaveAsked<T>()`.

### EventFlux.Saga.EfCore
- Fix durable timeout reads to continue past unresolvable event types while retaining those rows;
  preserve the valid-timeout batch limit, due-time order and exclusion of future timeouts.
- Fix outbox recovery to read past rows with unresolvable event types without changing or deleting them;
  preserve the valid-message batch limit, staging-time order and dispatch-delay filter.
- Saga persistence and a durable timeout store on EfCore.RepositoryPack 1.3.0, registered with the package's own
  scoped `AddEfCoreRepository`.
- `UseEfCoreOutbox<TDbContext>()` / `ConfigureSagaOutbox()`: outbox rows commit in the same `SaveChanges` as the
  instance.

### EventFlux.Saga.RabbitFlow
- Depend on EventFlux.RabbitFlow 1.3.2, a package-metadata update with the same public API, behavior and
  EventFlux 2.1.1 dependency as 1.3.1.
- RabbitMQ transport on EventFlux.RabbitFlow 1.3.2: publishing with cancellation, subscriptions for every saga
  event; failing saga steps are requeued or dead-lettered by RabbitFlow instead of lost.

### EventFlux.Saga.RedisFlow
- Depend on EventFlux.RedisFlow 1.1.1, a package-metadata update with the same public API, behavior and
  EventFlux 2.1.1 dependency as 1.1.0.
- Redis Streams transport on EventFlux.RedisFlow 1.1.1 using its typed `PublishAsync<TEvent>`; failing steps stay
  pending, are retried and dead-lettered by RedisFlow.
- `UseRedisDistributedLock()` on DistributedLock.Redis (Medallion.Threading).

### EventFlux.Saga.Generators
- Format generated ordinal literals independently of the build culture, fixing invalid C# for negative
  values when the culture uses a non-ASCII minus sign; preserve persisted names and ordinal mappings.
- Report SAGA005 and skip generation for file-local enums and enums inside file-local containing types,
  avoiding inaccessible references in generated files without suppressing healthy same-named siblings.
- Report SAGA004 for aliased numeric values, SAGA005 for inaccessible enums/containing types,
  SAGA006 for generic containing types and SAGA007 for colliding namespace-level set names; skip
  affected sets instead of emitting compiler errors or crashing. Preserve accessible nested sets.
- Handle the full `ulong` range without overflowing; values outside `int` retain declaration-index ordinals.
- `[SagaStateSet]` enum to reflection-free state definitions; SAGA001 (removed deployed state), SAGA002 (new
  state), SAGA003 (name clash).

### EventFlux.Saga.Inspector / EventFlux.Saga.Inspector.EfCore
- Transition timeline with HTML and JSON endpoints (`MapSagaInspector`), bounded in-memory store.
- Durable EF Core timeline store (`UseEfCoreTimeline<TDbContext>()`) with `PruneAsync(olderThan)`.

### Packaging
- Source Link and `.snupkg` symbol packages for every library package.
