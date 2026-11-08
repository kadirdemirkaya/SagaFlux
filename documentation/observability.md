# Observability and retention

The inspector timeline, tracing, metrics, and how long data is kept.

## Inspector

```bash
dotnet add package EventFlux.Saga.Inspector
```

```csharp
builder.Services.AddSagaFlux(typeof(Program).Assembly).UseInspector();
app.MapSagaInspector("/saga-inspector").RequireAuthorization("ops");
```

| Endpoint | Returns |
|---|---|
| `GET /saga-inspector` | HTML page |
| `GET /saga-inspector/instances/{correlationId}` | Instance timeline (JSON) |
| `GET /saga-inspector/recent?take=50` | Latest records (JSON) |

- Each handled event is recorded (saga, instance, event, from/to state, published events, compensations,
  timeouts); registered `ISagaTransitionObserver`s receive the same records.
- The default store is in-memory and bounded (`MaxEntries`, default 10,000). For a durable, shared timeline add
  `EventFlux.Saga.Inspector.EfCore`, call `modelBuilder.ConfigureSagaTimeline()` and `.UseEfCoreTimeline<AppDbContext>()`.
- Protect the endpoints outside development; they expose business data.

## Tracing

Each event applied to an instance creates an activity on the `ActivitySource` `EventFlux.Saga`
(`SagaFluxDiagnostics.ActivitySourceName`), named `"{Saga} {Event}"` with `SagaFluxDiagnostics.Tag*` tags.
A failing event sets status `Error`. Nothing is recorded without a listener.

## Metrics (optional)

Counters on `System.Diagnostics.Metrics`, off until you listen to the meter:

| Meter | Counter | Tags |
|---|---|---|
| `EventFlux.Saga` | `sagaflux.dispatcher.dispatch.operations` | `kind`: `outbox`/`timeout`, `outcome`: `success`/`failure`/`cancelled` |
| `EventFlux.Saga` | `sagaflux.dispatcher.completion.operations` | same |
| `EventFlux.Saga.EfCore` | `sagaflux.store.unresolved_row.observations` | `kind`: `outbox`/`timeout` |

- They count attempts of the batch dispatchers (each send/route and each store completion call) and each EF Core
  read that skips an unusable row; not unique messages, backlog or business outcomes. A completion `success`
  only means the store call returned; `cancelled` means the dispatcher's own token was cancelled. Inline sends
  while a saga handles an event are not counted.
- No ids, payloads, type names or error text are tagged. A throwing listener callback is swallowed.

## Retention

- **Instances:** final instances stay unless the saga calls `RemoveWhenFinal()`.
- **Compensation stack:** kept in the instance row (entries marked, never deleted) until the instance is removed.
- **Timeline:** in-memory keeps the last `MaxEntries`; EF Core keeps rows until `EfCoreSagaTimelineStore.PruneAsync(olderThan)`.
