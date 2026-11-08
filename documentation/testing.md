# Testing

Unit testing sagas with the Given/When/Then harness. Examples refer to `OrderSaga`, `OrderState` and the events from the quick start in the [README](../README.md).

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

See also: [State machine](state-machine.md) (`TravelSaga` in [Compensating actions](state-machine.md#compensating-actions), `ReservationSaga` in [Timeouts](state-machine.md#timeouts), `Answer`/`HaveAsked` in [Asking for a decision](state-machine.md#asking-for-a-decision)).

---

[Documentation index](README.md)
