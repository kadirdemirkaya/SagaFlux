# Persistence and outbox

Where saga instances, timeouts and published events are stored. Examples refer to `OrderSaga`, `OrderState` and the events from the quick start in the [README](../README.md).

## In-memory persistence

`InMemorySagaRepository<TState>` stores isolated JSON copies in the current process; state is lost on shutdown.
TState must support serialization and deserialization with System.Text.Json.
If JSON serialization fails during `SaveAsync`, the original exception propagates, the caller's `Version`
stays at its pre-save value, and any previously stored data and version remain unchanged. After fixing the
serialization problem, saving the same object again increments its version once on success.

## Persistence (EF Core)

```bash
dotnet add package EventFlux.Saga.EfCore
```

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.ConfigureSagaState<OrderState>().ToTable("OrderSagas");
    modelBuilder.ConfigureSagaTimeouts();
}
```

```csharp
services.AddDbContext<AppDbContext>(options => /* your provider */);
services.AddSagaFlux(typeof(Program).Assembly)
        .UseEfCore<OrderState, AppDbContext>()
        .UseEfCoreTimeouts<AppDbContext>();
```

- `CorrelationId` is the key and `Version` is an optimistic concurrency token.
- A compensation stack is stored as a JSON text column.
- `UseEfCoreTimeouts` keeps pending timeouts across restarts.
  - The due time is stored as UTC ticks.
  - Rows with unresolvable event types are logged and retained, without blocking later valid due timeouts.
    `TimeoutBatchSize` limits valid timeouts per poll, in due-time order; future timeouts are not delivered.
  - The event is stored as JSON, with its type name recorded without a version.
  - If you rename or move a timeout event type while timeouts are pending, keep the old name resolving with
    `[SagaEventFormerName("Old.Namespace.OldName, Old.Assembly")]` on the new type.
- The repository is scoped. Dispatch events inside a DI scope (for example an HTTP request), or enable
  EventFlux's `CreateScopePerEvent`.

## Transactional outbox (EF Core)

Without an outbox, published events are sent right after the instance is saved. A crash between the save and
the send loses them. The outbox closes that gap:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.ConfigureSagaState<OrderState>().ToTable("OrderSagas");
    modelBuilder.ConfigureSagaOutbox();
}

services.AddSagaFlux(typeof(Program).Assembly)
        .UseEfCore<OrderState, AppDbContext>()
        .UseEfCoreOutbox<AppDbContext>();
```

- **Staging:** published events are written to the `SagaOutbox` table in the same `SaveChanges` as the instance,
  so both are committed, or neither.
- **Normal path:** after the save and post-save cleanup/notification, the events are sent and their rows deleted.
- **Recovery:** transport send failures are logged and left to the outbox dispatcher, except cancellation when
  the caller's token is cancelled, which propagates. This does not suppress post-save cleanup or outbox
  completion errors. A hosted service sends the rows left behind:
  - It polls every `OutboxPollingInterval` (default 5 s).
  - It only picks rows older than `OutboxDispatchDelay` (default 30 s).
  - Rows with unresolvable event types are logged and retained, without blocking later valid messages.
    `OutboxBatchSize` limits the valid messages returned per poll, in staging-time order.
  - Delivery is at least once.
- **Scope:** use it for sagas stored with `UseEfCore` on the same `DbContext`. The outbox commits with that
  repository.
- **Custom stores:** implement `ISagaOutbox` and register it with `UseOutbox<T>()`.

### Errors after persistence and redelivery

**An exception from a saga invocation does not mean rollback.** State may already have been committed
before a later operation fails. Saving state, removing a final instance, cleaning up timeouts, notifying
observers and sending events are not one atomic operation.

- **Final removal without an outbox:** `RemoveWhenFinal()` saves `Final` before attempting removal.
  A removal error propagates before timeout cleanup, observer notification or outgoing sends; even a
  `SagaConcurrencyException` at removal is outside the save retry controlled by `MaxConcurrencyRetries`.
  In the characterized Active-to-Final case, a failure before deletion leaves the saved `Final` instance
  and its pending timeout intact, and the intended outgoing event is not sent. Once the removal failure
  is cleared, redelivering that same event is ignored because this saga has no matching transition in
  `Final`: it neither retries removal/timeout cleanup nor recovers the outgoing event. Redelivery is
  therefore not a guaranteed recovery mechanism. This is not evidence of saga-row loss or broker loss;
  other storage failures can have uncertain outcomes. Separately, an EF Core stale tracked delete can
  raise a concurrency exception while preserving a newer `Final` row; that repository-level case does
  not establish a full runner or broker delivery outcome.
- **Outbox completion after a successful send:** `ISagaOutbox.CompleteAsync` runs outside the transport
  send-error catch. Its exception can reach the caller after both the state commit and a successful
  send. If the outbox row remains pending, `ISagaOutboxDispatcher` can send the same stored message again
  after `OutboxDispatchDelay` and then complete it. Duplicate sends in this window are consistent with
  at-least-once delivery, not by themselves a bug or an exactly-once guarantee. Outbox dispatch recovers
  pending outgoing messages, not final-instance cleanup or skipped observer notifications.

Make receivers idempotent using application-level event identity, and inspect persisted state and pending
work before deciding how to reconcile a failed invocation; do not blindly replay business actions or
assume a retry repairs cleanup. These are existing behavior boundaries, not a new error-swallowing,
retry or opt-in cleanup policy. No automatic final-instance cleanup recovery is provided here.

See also: [Delivery and duplicates](correlation-and-delivery.md#delivery-and-duplicates), [Retention](observability.md#retention), [Extension points and options](extension-points-and-stability.md#extension-points-and-options).

---

[Documentation index](README.md)
