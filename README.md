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
[Delivery and duplicates](#delivery-and-duplicates).

## Correlating events

Every event a saga reacts to must lead to one instance id:

- **`ICorrelatedEvent`**: an `IEventRequest` with a `Guid CorrelationId` property. Implement it on events you
  design for the saga (`PaymentReceived` above); nothing else is needed.
- **Any other `IEventRequest`**: declare where the id is with `CorrelateBy<TEvent>(e => e.OrderId)` in the saga
  constructor (`OrderPlaced` above). Use it for events that already exist in your domain and have their own id
  property. A selector declared with `CorrelateBy` wins over `ICorrelatedEvent`.
- `AddSaga` validates this at startup and throws if an event cannot be correlated.

## Delivery and duplicates

- **In-process** (the default), each dispatch applies the event to each matching saga once.
- **RabbitFlow and RedisFlow deliver at least once.** A message can arrive again after a failure or a restart.
- **The version check does not detect a repeated event.** It only stops two concurrent writers from
  overwriting each other.
- **With the transactional outbox, events a transition publishes are sent at least once**, even across a crash.
- **Most duplicates are harmless.** A transition that moves the instance to another state ignores a second copy,
  because the instance is no longer in the state that accepts that event.
- **Some transitions can run twice.** Transitions that stay in their state, such as a counter in
  `DuringActive(...)`, run again for each copy. Make their actions idempotent, for example by recording the
  last processed event id in the state data.
- **Several sagas can react to the same event type.** Each has its own instance, repository and lock. They run
  one after another in registration order. If one throws, the ones after it do not run for that delivery;
  the transport retries the whole event.

## What it is, what it is not

> SagaFlux, ağır bir broker topolojisi ve ticari lisans gerektirmeden EventFlux ekosisteminde saga/process-manager pattern'i uygulamanı sağlayan, önce in-process sonra opsiyonel dağıtık çalışan, test edilebilirliği ve telafi (compensation) mantığını birinci sınıf vatandaş yapan hafif bir kütüphanedir.

**It is:**
- a state machine DSL (`During` / `When` / `Then` / `Publish` / `TransitionTo`)
- correlation and optimistic concurrency
- compensating actions
- a Given/When/Then test harness

**It is not:**
- a broker client or wire protocol of its own: publishing always delegates to EventFlux (in-process) or to the
  RabbitFlow / RedisFlow message format
- a routing-slip engine
- a visual designer

The saga definition never references a transport. Publishing goes through the small `ISagaTransport` seam, so
switching to a broker only changes the registration.

## Packages

All packages are pre-release (**0.1.0**) and share one version. They move to 1.0.0 together once the
public API has been reviewed and frozen.

| Package | Purpose | Version |
|---|---|---|
| `EventFlux.Saga` | Core: `Saga<TState>`, state machine DSL, in-process execution | 0.1.0 |
| `EventFlux.Saga.Testing` | Given/When/Then test harness | 0.1.0 |
| `EventFlux.Saga.EfCore` | Persistence on top of `EfCore.RepositoryPack` | 0.1.0 |
| `EventFlux.Saga.RabbitFlow` | RabbitMQ transport adapter on `EventFlux.RabbitFlow` (>= 1.3.2) | 0.1.0 |
| `EventFlux.Saga.RedisFlow` | Redis Streams transport adapter on `EventFlux.RedisFlow` (>= 1.1.1), optional Redis instance lock | 0.1.0 |
| `EventFlux.Saga.Generators` | Compile-time state definitions from a `[SagaStateSet]` enum | 0.1.0 |
| `EventFlux.Saga.Inspector` | Transition timeline with JSON and HTML endpoints | 0.1.0 |
| `EventFlux.Saga.Inspector.EfCore` | Durable EF Core timeline store for the inspector | 0.1.0 |

All packages target .NET 8 and EventFlux 2.1.1. .NET 8 is the baseline because it is the current LTS release
that EventFlux 2.x, RabbitFlow and RedisFlow support; other target frameworks can be added on request.
Packages ship with Source Link and `.snupkg` symbols.

## More state machine building blocks

```csharp
public PaymentSaga()
{
    RemoveWhenFinal();                                   // finished instances leave the repository

    During(SagaStates.Initial,
        When<PaymentRequested>()
            .Then(ctx => ctx.State.Amount = ctx.Event.Amount)
            .Branch(ctx => ctx.State.Amount > 1000m,     // conditional step chains
                then => then.Publish(ctx => new RequestApproval(ctx.CorrelationId)).TransitionTo(AwaitingApproval),
                otherwise => otherwise.Then(Authorize).TransitionTo(Charging))
            .OnFault<CardDeclinedException>(fault => fault // handle a failing step inside the saga
                .Publish(ctx => new PaymentDeclined(ctx.CorrelationId, ctx.Fault!.Message))
                .TransitionTo(Failed)));

    DuringActive(                                        // valid in every started, non-final state
        When<PaymentCancelled>().Compensate().Finalize());
}
```

- `DuringActive(...)` transitions apply in any state of a started instance except `Final`. Transitions declared
  with `During` for the current state are tried first, and active transitions never create an instance.
- `Branch(condition, then, otherwise)` accepts every step, including `TransitionTo`, `OnCompensate`,
  `Compensate` and nested branches.
- `OnFault<TException>(...)` runs when a step of the transition throws that exception type.
  - What the failed steps published, scheduled or declared is discarded.
  - Changes they made to the state data are kept.
  - The handled exception is available as `ctx.Fault`, and appears on the inspector timeline.
  - Unhandled exceptions still fail the message, so the transport retries it.
- `RemoveWhenFinal()` deletes the instance from its repository once it is final. The in-memory and EF Core
  repositories support it, and the timeline keeps the history. A later event matching an `Initial` transition
  starts a new instance.
- The harness covers them with `BeRemoved()`, `HaveHandledFault<T>()` and `NotHaveHandledFault()`.

## Waiting for several events

```csharp
public sealed class ShipmentState : IJoinableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = "";
    public int Version { get; set; }
    public List<string> JoinedEvents { get; set; } = new();
}

During(SagaStates.Initial,
    WhenAll<OrderPaid, OrderPacked>()                    // both, in any order
        .Publish(ctx => new ShipParcel(ctx.CorrelationId))
        .TransitionTo(ReadyToDispatch));
```

- **Before it completes:** each awaited event is recorded in `JoinedEvents` and the instance stays in its
  state. The first event creates the instance.
- **When it completes:** the steps run once all events have arrived. `ctx.Event` is the event that completed
  the join. A repeated event counts once, and the join starts over after it has run.
- **Arity:** `WhenAll` takes two or three event types.
- **Persistence:** the EF Core package stores `JoinedEvents` as a JSON text column (`ConfigureSagaState`).

## Asking for a decision

A transition can send an EventFlux request and use the response before it decides where to go:

```csharp
public sealed record CheckCredit(Guid ApplicantId, decimal Amount) : IEventRequest<CreditScore>;
public sealed record CreditScore(int Value) : IEventResponse;

During(SagaStates.Initial,
    When<LoanApplied>()
        .Ask(ctx => new CheckCredit(ctx.CorrelationId, ctx.Event.Amount), (ctx, score) => ctx.State.Score = score.Value)
        .Branch(ctx => ctx.State.Score >= 600,
            then => then.TransitionTo(Approved),
            otherwise => otherwise.TransitionTo(Rejected)));
```

- The request goes to its `IEventHandler<CheckCredit, CreditScore>` through `IEventDispatcher`.
  `ctx.AskAsync(request)` does the same inside `ThenAsync`.
- It runs while the instance is processed (and locked), so its handler must not wait for this same instance.
- A handler that throws, or returns no response, fails the step. `OnFault<TException>(...)` can handle that.
- **Talking to another service over a broker:** publish a command and handle the reply with a normal
  `When<TReply>()`, plus a `Schedule(...)` timeout if the reply may never come.
- **In tests:** answer requests without any handler, using
  `SagaTestHarness...Answer<CheckCredit, CreditScore>(c => new CreditScore(720))`, and assert them with
  `HaveAsked<CheckCredit>()`. A request without an answer fails with `SagaConfigurationException`. `OnFault`
  never handles that exception, so a setup mistake cannot pass as a business fault.

## Compensating actions

Each completed step records a **stable string key**. Unwinding runs the pending keys in reverse (LIFO)
order. Entries are marked as compensated, never deleted.

```csharp
public sealed class TravelState : ICompensableSagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = "";
    public int Version { get; set; }
    public List<CompensationEntry> Compensations { get; set; } = new();
}

public TravelSaga()
{
    During(AwaitingFlight,
        When<FlightBooked>()
            .OnCompensate("CancelFlight", ctx => ctx.Publish(new CancelFlight(ctx.CorrelationId)))
            .TransitionTo(AwaitingHotel));

    During(AwaitingCar,
        When<CarFailed>().Compensate().TransitionTo(Cancelled));
}

[CompensatingEvent]
public sealed record TripCancelled(Guid CorrelationId) : ICorrelatedEvent;
```

- A `[CompensatingEvent]` unwinds an existing instance in any state.
- Only the key is persisted, never the delegate, so keep keys constant across deployments.
- An unknown key throws `SagaCompensationException`.

## Timeouts

```csharp
During(SagaStates.Initial,
    When<ReservationRequested>()
        .Schedule(TimeSpan.FromMinutes(15), ctx => new ReservationExpired(ctx.CorrelationId))
        .TransitionTo(AwaitingPayment));

During(AwaitingPayment,
    When<ReservationPaid>().Unschedule<ReservationExpired>().TransitionTo(Confirmed),
    When<ReservationExpired>().Publish(ctx => new ReleaseSeat(ctx.CorrelationId)).TransitionTo(Expired));
```

- The timeout event comes back to the saga through an ordinary `When<T>()` transition.
- Scheduling the same type again replaces the pending timeout.
- Reaching `Final` cancels all pending timeouts of the instance.
- A hosted service delivers due timeouts (`SagaFluxOptions.TimeoutPollingInterval`, default 1 second). It is
  registered automatically when a saga declares `Schedule`.
- Without a host, call `ISagaTimeoutDispatcher.DispatchDueAsync()` yourself.
- Delivery is at-least-once, and a failed delivery is retried after `TimeoutRetryDelay`.
- Time comes from `TimeProvider`, so tests can drive it.
- With valid arguments, `InMemorySagaTimeoutStore` rejects calls to `ScheduleAsync`, `CancelAsync`,
  `CancelAllAsync`, `GetDueAsync` and `CompleteAsync` whose token is already cancelled before the call,
  throwing `OperationCanceledException` with that token and leaving pending timeouts and `Count` unchanged.
  `ScheduleAsync` checks for a null timeout first. Cancellation during an operation or while waiting for
  the store's lock has no additional guarantee.

## Generated states

```bash
dotnet add package EventFlux.Saga.Generators
```

```csharp
[SagaStateSet]
public enum OrderStates { Initial, AwaitingPayment, Shipped, Final }

During(OrderStatesSet.Initial,
    When<OrderPlaced>().TransitionTo(OrderStatesSet.AwaitingPayment));
```

- The compiler generates `OrderStatesSet` with one `SagaState` per member, plus `All`, `From(enum)` and
  `TryParse(name)`. The generated code uses no reflection.
- `Initial` and `Final` map to the built-in states.
- Top-level public/internal enums (including the global namespace) and accessible nested enums in
  non-generic types keep the namespace-level `{EnumName}Set` name. A nested enum and every containing
  type must be accessible from that namespace (`public`, `internal` or `protected internal`) and must not
  be file-local. File-local enums and enums inside file-local types produce **SAGA005** with no set,
  because the separate generated file cannot access them.
- Invalid inputs produce an error and no set for the affected enum, rather than invalid generated code:
  **SAGA004** for aliased numeric values, **SAGA005** for an inaccessible enum or containing type,
  **SAGA006** for a generic containing type, and **SAGA007** for multiple eligible enums that would
  generate the same set name in one namespace (reported on each conflicting enum). Use distinct numeric
  values and enum names, or move the enum to an accessible, non-generic type or another namespace.
- Ordinal literals use culture-independent C# formatting, including ASCII minus signs for negative values;
  persisted state names and ordinal mappings are unchanged.
- Values outside the `int` range, including large `ulong` values, use the member's zero-based declaration
  index as the ordinal; `From(enum)` still matches the declared numeric value.
- Commit an `OrderStates.sagastates` file listing the deployed states (one per line) and add it as
  `<AdditionalFiles Include="OrderStates.sagastates" />`.
  - Removing a listed state produces warning **SAGA001**, because stored instances may still be in it.
  - A state that is not listed yet produces **SAGA002** (info).

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
- It uses [DistributedLock.Redis](https://github.com/madelson/DistributedLock) on the RedisFlow connection.
  SagaFlux does not implement a lock of its own.
- The lock covers loading, executing and saving an instance. Published events are sent after it is released.
- Any other `ISagaLockProvider` can be plugged in with `UseLockProvider<T>()`.

## Inspector

```bash
dotnet add package EventFlux.Saga.Inspector
```

```csharp
builder.Services.AddSagaFlux(typeof(Program).Assembly).UseInspector();

app.MapSagaInspector("/saga-inspector").RequireAuthorization("ops");
```

Every handled event is recorded: saga, instance, event, from/to state, published events, compensations and
timeouts.

| Endpoint | Returns |
|---|---|
| `GET /saga-inspector` | An HTML page |
| `GET /saga-inspector/instances/{correlationId}` | The instance timeline as JSON |
| `GET /saga-inspector/recent?take=50` | The latest records as JSON |

- The timeline is in-memory and bounded (`MaxEntries`, default 10,000).
- To keep it across restarts and share it between nodes, add `EventFlux.Saga.Inspector.EfCore`, map the table
  with `modelBuilder.ConfigureSagaTimeline()` and call `.UseEfCoreTimeline<AppDbContext>()`. Rows are kept until
  you delete them with `EfCoreSagaTimelineStore.PruneAsync(olderThan)` from a scheduled job of your own.
- Any `ISagaTransitionObserver` you register receives the same records.
- Protect the endpoints outside development, because they expose business data.

## Tracing

SagaFlux reports every event applied to a saga instance to the `System.Diagnostics.ActivitySource` named
`EventFlux.Saga` (`SagaFluxDiagnostics.ActivitySourceName`). No extra package is needed. With OpenTelemetry:

```csharp
builder.Services.AddOpenTelemetry().WithTracing(t => t.AddSource(SagaFluxDiagnostics.ActivitySourceName));
```

- **Activity:** one per saga and event, named `"{Saga} {Event}"`.
- **Tags:** saga, event, correlation id, handled, from/to state, version, attempts, handled fault and removal.
  The names are listed as `SagaFluxDiagnostics.Tag*`.
- **Failures:** a failing event sets the activity status to `Error`.
- **Cost:** nothing is recorded when no listener is attached.

## Retention

- **Compensation stack.** It is stored in the instance row, one entry per recorded step. Entries are marked
  compensated, never deleted, so the stack grows only with the steps of that instance. It is deleted together
  with the instance when the saga calls `RemoveWhenFinal()`.
- **Instances.** Final instances stay in the repository unless the saga calls `RemoveWhenFinal()`.
- **Inspector timeline.** The in-memory store keeps the last `MaxEntries` records. The EF Core store keeps
  records until `PruneAsync(olderThan)` deletes them.

## Testing a saga (no database, no broker, no DI)

```bash
dotnet add package EventFlux.Saga.Testing
```

```csharp
var result = SagaTestHarness.For<OrderSaga, OrderState>()
    .Given(OrderSaga.AwaitingPayment)
    .When(new PaymentReceived(orderId))
    .Then();

result.Should().BeFinal()
      .And.HavePublished<ShipOrder>(s => s.OrderId == orderId);

SagaTestHarness.For<TravelSaga, TravelState>()
    .Given(TravelSaga.AwaitingCar)
    .WithCompensations("CancelFlight", "CancelHotel")
    .When(new CarFailed(tripId))
    .Then()
    .Should().HaveCompensatedInOrder("CancelHotel", "CancelFlight");

SagaTestHarness.For<ReservationSaga, ReservationState>()
    .When(new ReservationRequested(id))
    .Then()
    .Should().HaveScheduled<ReservationExpired>(TimeSpan.FromMinutes(15));
```

Failed assertions throw `SagaAssertionException`, so the harness works with any test framework.

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
