# Correlation and delivery

How events find their saga instance, and what a repeated delivery does. Examples use the quick start from the [README](../README.md).

## Correlating events

Every event a saga reacts to must lead to one instance id:

- **`ICorrelatedEvent`**: an `IEventRequest` with a `Guid CorrelationId`. Nothing else is needed.
- **Any other `IEventRequest`**: declare the id with `CorrelateBy<TEvent>(e => e.OrderId)` in the saga
  constructor. `CorrelateBy` wins over `ICorrelatedEvent`.
- `AddSaga` throws at startup if an event cannot be correlated.

## Delivery and duplicates

- **In-process** (default): one dispatch, through `IEventDispatcher` or `IEventBus`, runs each matching saga once.
- **RabbitFlow, RedisFlow and the outbox deliver at least once.** A message can arrive again after a failure or restart.
- **The version check does not detect a repeated event**; it only stops two concurrent writers.
- A transition that moves to another state ignores a second copy. A transition that stays in its state (for
  example a counter in `DuringActive`) runs again, so make it idempotent, e.g. by storing the last event id.
- Several sagas reacting to one event type run one after another in registration order, each with its own
  instance, repository and lock. If one throws, the rest do not run and the transport retries the whole event.

See also: [Transports](transports.md), [Errors after persistence](persistence.md#errors-after-persistence-and-redelivery).
