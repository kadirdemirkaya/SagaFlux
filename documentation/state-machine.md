# State machine

Building blocks beyond the quick start.

## Branches, faults, active states

```csharp
RemoveWhenFinal();

During(SagaStates.Initial,
    When<PaymentRequested>()
        .Branch(ctx => ctx.Event.Amount > 1000m,
            then => then.Publish(ctx => new RequestApproval(ctx.CorrelationId)).TransitionTo(AwaitingApproval),
            otherwise => otherwise.Then(Authorize).TransitionTo(Charging))
        .OnFault<CardDeclinedException>(fault => fault
            .Publish(ctx => new PaymentDeclined(ctx.CorrelationId, ctx.Fault!.Message))
            .TransitionTo(Failed)));

DuringActive(When<PaymentCancelled>().Compensate().Finalize());
```

- `DuringActive(...)`: every started, non-final state. `During` transitions win; it never creates an instance.
- `Branch(condition, then, otherwise)`: accepts every step, including nested branches.
- `OnFault<TException>(...)`: runs when a step throws that exception (`ctx.Fault`). Events, timeouts and
  compensations of the failed steps are discarded; state changes are kept. Unhandled exceptions fail the message.
- `RemoveWhenFinal()`: deletes the instance once final (in-memory and EF Core repositories); a later `Initial`
  event starts a new one.

## Waiting for several events

```csharp
During(SagaStates.Initial,
    WhenAll<OrderPaid, OrderPacked>().Publish(ctx => new ShipParcel(ctx.CorrelationId)).TransitionTo(ReadyToDispatch));
```

The state implements `IJoinableSagaState` (adds `List<string> JoinedEvents`). Two or three event types in any
order; the first creates the instance. The steps run once all arrived (`ctx.Event` is the last). A repeated
event counts once; the join starts over after it ran.

## Asking for a decision

```csharp
public sealed record CheckCredit(Guid ApplicantId, decimal Amount) : IEventRequest<CreditScore>;
public sealed record CreditScore(int Value) : IEventResponse;

During(SagaStates.Initial,
    When<LoanApplied>()
        .Ask(ctx => new CheckCredit(ctx.CorrelationId, ctx.Event.Amount), (ctx, score) => ctx.State.Score = score.Value)
        .TransitionTo(Scored));
```

- Sent through `IEventDispatcher` to its `IEventHandler<,>`; `ctx.AskAsync(request)` does the same in `ThenAsync`.
- It runs while the instance is locked, so the handler must not wait for the same instance. A throwing handler
  or a missing response fails the step. Across services, publish a command, handle the reply and `Schedule` a timeout.
- In tests, `Answer<TRequest, TResponse>(...)` replaces the handler; an unanswered request throws
  `SagaConfigurationException`, which `OnFault` never handles.

## Compensating actions

```csharp
During(AwaitingFlight,
    When<FlightBooked>().OnCompensate("CancelFlight", ctx => ctx.Publish(new CancelFlight(ctx.CorrelationId))).TransitionTo(AwaitingCar));
During(AwaitingCar,
    When<CarFailed>().Compensate().TransitionTo(Cancelled));
```

- The state implements `ICompensableSagaState` (adds `List<CompensationEntry> Compensations`). `Compensate()`
  runs pending keys in reverse (LIFO) order. An event marked `[CompensatingEvent]` unwinds an existing instance
  in any state.
- Only the key is persisted: keep keys constant across deployments. An unknown key throws `SagaCompensationException`.

## Timeouts

```csharp
During(SagaStates.Initial,
    When<ReservationRequested>().Schedule(TimeSpan.FromMinutes(15), ctx => new ReservationExpired(ctx.CorrelationId)).TransitionTo(AwaitingPayment));
During(AwaitingPayment,
    When<ReservationPaid>().Unschedule<ReservationExpired>().TransitionTo(Confirmed),
    When<ReservationExpired>().TransitionTo(Expired));
```

- The timeout arrives as a normal event. Scheduling the same type again replaces it; `Final` cancels all.
- A hosted service delivers due timeouts every `TimeoutPollingInterval` (1 s); without a host call
  `ISagaTimeoutDispatcher.DispatchDueAsync()`. At least once; failures retry after `TimeoutRetryDelay`.
- Time comes from `TimeProvider`. `InMemorySagaTimeoutStore` throws `OperationCanceledException` for an
  already-cancelled token and changes nothing.

## Generated states

```csharp
[SagaStateSet] // package EventFlux.Saga.Generators
public enum OrderStates { Initial, AwaitingPayment, Shipped, Final }

During(OrderStatesSet.Initial, When<OrderPlaced>().TransitionTo(OrderStatesSet.AwaitingPayment));
```

- Generates `OrderStatesSet` with one `SagaState` per member, plus `All`, `From(enum)` and `TryParse(name)`;
  `Initial` / `Final` map to the built-in states. Values outside `int` use the declaration index as ordinal.
- The enum must be accessible from its namespace, not file-local and not inside a generic type.
- Errors: **SAGA003** member collides with a generated one (`All`), **SAGA004** aliased values, **SAGA005**
  inaccessible or file-local enum, **SAGA006** generic containing type, **SAGA007** duplicate set name.
- List deployed states in `OrderStates.sagastates` (`<AdditionalFiles Include="OrderStates.sagastates" />`):
  removing a listed state warns **SAGA001**, an unlisted one reports **SAGA002** (info).
