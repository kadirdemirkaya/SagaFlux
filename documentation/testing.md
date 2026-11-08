# Testing

Unit-test a saga with no database, no broker and no DI. Examples use the [README](../README.md) quick start and the [state machine](state-machine.md) page.

```bash
dotnet add package EventFlux.Saga.Testing
```

```csharp
SagaTestHarness.For<OrderSaga, OrderState>()
    .Given(OrderSaga.AwaitingPayment)
    .When(new PaymentReceived(orderId))
    .Then()
    .Should().BeFinal()
    .And.HavePublished<ShipOrder>(s => s.OrderId == orderId);

SagaTestHarness.For<TravelSaga, TravelState>()
    .Given(TravelSaga.AwaitingCar)
    .WithCompensations("CancelFlight", "CancelHotel")
    .When(new CarFailed(tripId))
    .Then()
    .Should().HaveCompensatedInOrder("CancelHotel", "CancelFlight");
```

- Other assertions: `HaveScheduled<T>(delay)`, `BeRemoved()`, `HaveHandledFault<T>()`, `NotHaveHandledFault()`,
  `HaveAsked<T>()` / `NotHaveAsked<T>()`.
- `.Answer<TRequest, TResponse>(...)` answers `Ask` requests without a handler.
- Failed assertions throw `SagaAssertionException`, so any test framework works.
