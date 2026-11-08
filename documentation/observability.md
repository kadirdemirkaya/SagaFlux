# Observability and retention

Seeing what sagas do: the inspector timeline, tracing, metrics, and how long recorded data is kept.

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
`EventFlux.Saga` (`SagaFluxDiagnostics.ActivitySourceName`). No extra package is needed: any `ActivitySource`
listener or tracing SDK that subscribes to that source name receives the activities. With the built-in listener:

```csharp
using var listener = new ActivityListener
{
    ShouldListenTo = source => source.Name == SagaFluxDiagnostics.ActivitySourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
    ActivityStopped = activity => Console.WriteLine($"{activity.DisplayName} {activity.Duration}")
};
ActivitySource.AddActivityListener(listener);
```

- **Activity:** one per saga and event, named `"{Saga} {Event}"`.
- **Tags:** saga, event, correlation id, handled, from/to state, version, attempts, handled fault and removal.
  The names are listed as `SagaFluxDiagnostics.Tag*`.
- **Failures:** a failing event sets the activity status to `Error`.
- **Cost:** nothing is recorded when no listener is attached.

### Metrics (optional)

SagaFlux also reports a few operational counters through `System.Diagnostics.Metrics`. They are off until your
application subscribes to the meter names below (with its own `MeterListener` or metrics pipeline); there is no
option to turn on and no extra package.

| Meter | Counter | Unit | Tags |
|---|---|---|---|
| `EventFlux.Saga` | `sagaflux.dispatcher.dispatch.operations` | `{operation}` | `kind`: `outbox` / `timeout`, `outcome`: `success` / `failure` / `cancelled` |
| `EventFlux.Saga` | `sagaflux.dispatcher.completion.operations` | `{operation}` | same as above |
| `EventFlux.Saga.EfCore` | `sagaflux.store.unresolved_row.observations` | `{observation}` | `kind`: `outbox` / `timeout` |

```csharp
using var listener = new MeterListener();
listener.InstrumentPublished = (instrument, l) =>
{
    if (instrument.Meter.Name is "EventFlux.Saga" or "EventFlux.Saga.EfCore") l.EnableMeasurementEvents(instrument);
};
listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Console.WriteLine($"{instrument.Name} +{value}"));
listener.Start();
```

- **Dispatch / completion:** one count per send (outbox) or route (timeout) attempted by the batch dispatchers,
  and one per store completion call they make. `cancelled` means the call threw `OperationCanceledException`
  while the dispatcher's token was cancelled; any other exception is `failure`. A completion `success` means the
  store call returned, not that a row was deleted or a saga finished.
- **Unresolved rows:** one count each time the EF Core outbox or timeout read skips a stored row whose event type
  cannot be resolved or whose payload is not an event. The row stays stored, so every later read counts it again.
- **What they are not:** attempt and encounter totals while a listener is attached, not unique messages, a
  backlog size, delivery guarantees or business outcomes. Inline sends made while a saga handles an event are not
  counted. No identifier, payload, type name or error text is ever attached as a tag.
- **Listeners:** keep callbacks fast and non-blocking. An exception thrown by a callback is swallowed and never
  changes a dispatch, completion, retry or read result. Metrics and tracing are independent of each other.

## Retention

- **Compensation stack.** It is stored in the instance row, one entry per recorded step. Entries are marked
  compensated, never deleted, so the stack grows only with the steps of that instance. It is deleted together
  with the instance when the saga calls `RemoveWhenFinal()`.
- **Instances.** Final instances stay in the repository unless the saga calls `RemoveWhenFinal()`.
- **Inspector timeline.** The in-memory store keeps the last `MaxEntries` records. The EF Core store keeps
  records until `PruneAsync(olderThan)` deletes them.

See also: [Persistence and outbox](persistence.md), [Compensating actions](state-machine.md#compensating-actions).

---

[Documentation index](README.md)
