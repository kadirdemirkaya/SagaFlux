# Persistence and outbox

Where instances, timeouts and published events are stored. Examples use the quick start from the [README](../README.md).

## In-memory persistence

`InMemorySagaRepository<TState>` (the default) keeps isolated System.Text.Json copies in the process; state is
lost on shutdown. If serialization fails in `SaveAsync`, the exception propagates and both the caller's `Version`
and the stored data stay unchanged, so the same object can be saved again after the fix.

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

services.AddDbContext<AppDbContext>(options => /* your provider */);
services.AddSagaFlux(typeof(Program).Assembly)
        .UseEfCore<OrderState, AppDbContext>()
        .UseEfCoreTimeouts<AppDbContext>();
```

- `CorrelationId` is the key, `Version` the optimistic concurrency token; the compensation stack is a JSON column.
- `ConfigureSagaState<OrderState>("OrderSagas")` takes the table name directly.
- The repository is scoped: dispatch inside a DI scope (e.g. an HTTP request) or enable EventFlux's `CreateScopePerEvent`.
- `UseEfCoreTimeouts` keeps pending timeouts across restarts. Rows whose event type cannot be resolved are
  logged and kept without blocking valid ones; `TimeoutBatchSize` limits valid timeouts per poll.
- Renaming or moving a timeout event type while timeouts are pending: add
  `[SagaEventFormerName("Old.Namespace.OldName, Old.Assembly")]` to the new type.

## Transactional outbox (EF Core)

Without an outbox, events are sent right after the save, and a crash in between loses them. With it:

```csharp
modelBuilder.ConfigureSagaOutbox();
services.AddSagaFlux(typeof(Program).Assembly).UseEfCore<OrderState, AppDbContext>().UseEfCoreOutbox<AppDbContext>();
```

- Events are written to `SagaOutbox` in the same `SaveChanges` as the instance (same `DbContext`), then sent
  and deleted. Send failures are logged and left to the dispatcher (caller cancellation still propagates).
- A hosted service resends rows older than `OutboxDispatchDelay` (30 s) every `OutboxPollingInterval` (5 s), up to
  `OutboxBatchSize` valid rows; unresolvable rows are logged and kept. Delivery is at least once.
- Custom store: implement `ISagaOutbox` and register it with `UseOutbox<T>()`.

## Errors after persistence and redelivery

**An exception from a saga invocation does not mean rollback.** Saving state, removing a final instance,
cleaning up timeouts, notifying observers and sending events are not one atomic operation.

- **Final removal without an outbox:** `RemoveWhenFinal()` saves `Final` first. A removal error (including
  `SagaConcurrencyException`, not retried by `MaxConcurrencyRetries`) propagates before timeout cleanup, observer
  notification and sends: the `Final` instance and its timeouts stay and the event is not sent. Redelivery does
  not repair this, because `Final` has no matching transition.
- **Outbox completion after a successful send:** an `ISagaOutbox.CompleteAsync` error reaches the caller after
  the commit and the send. The row stays pending and may be sent again (at least once). The outbox recovers
  pending messages only, not cleanup or observer notifications.

Make receivers idempotent with an application-level event id, and inspect persisted state and pending work
before reconciling a failed invocation instead of blindly replaying it.

See also: [Delivery and duplicates](correlation-and-delivery.md#delivery-and-duplicates), [Retention](observability.md#retention).
