# Correlation and delivery

How events find their saga instance, and what happens when a message is delivered more than once. Examples refer to `OrderSaga`, `OrderState` and the events from the quick start in the [README](../README.md).

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

See also: [Transports and distributed lock](transports.md), [Errors after persistence and redelivery](persistence.md#errors-after-persistence-and-redelivery).

---

[Documentation index](README.md)
