namespace EventFlux.Saga.Testing;

/// <summary>
/// Entry point of the Given/When/Then saga test API.
/// </summary>
/// <remarks>
/// Scenarios run the saga's pure state machine only: no repository, no transport, no DI container.
/// <code>
/// var result = SagaTestHarness.For&lt;OrderSaga, OrderState&gt;()
///     .Given(OrderSaga.AwaitingPayment)
///     .When(new PaymentFailed(orderId))
///     .Then();
///
/// result.Should().TransitionTo(OrderSaga.Cancelled)
///       .And.HavePublished&lt;NotifyCustomer&gt;();
/// </code>
/// </remarks>
public static class SagaTestHarness
{
    /// <summary>
    /// Starts a scenario for a new instance of <typeparamref name="TSaga"/>.
    /// </summary>
    /// <typeparam name="TSaga">The saga type.</typeparam>
    /// <typeparam name="TState">The saga state type.</typeparam>
    /// <returns>A scenario with no existing saga instance.</returns>
    public static SagaScenario<TState> For<TSaga, TState>()
        where TSaga : Saga<TState>, new()
        where TState : class, ISagaState, new()
        => For<TState>(new TSaga());

    /// <summary>
    /// Starts a scenario for the given saga definition.
    /// </summary>
    /// <typeparam name="TState">The saga state type.</typeparam>
    /// <param name="saga">The saga definition.</param>
    /// <returns>A scenario with no existing saga instance.</returns>
    public static SagaScenario<TState> For<TState>(Saga<TState> saga)
        where TState : class, ISagaState, new()
    {
        ArgumentNullException.ThrowIfNull(saga);
        saga.Validate();
        return new SagaScenario<TState>(saga);
    }
}
