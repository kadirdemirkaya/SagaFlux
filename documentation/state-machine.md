# State machine

The state machine DSL beyond the quick start: branches, faults, joins, requests, compensation, timeouts and generated states.

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

See also: [Testing](testing.md), [Persistence and outbox](persistence.md).

---

[Documentation index](README.md)
