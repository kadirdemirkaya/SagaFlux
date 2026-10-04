namespace EventFlux.Saga;

/// <summary>
/// Delivers due saga timeouts to the sagas.
/// </summary>
public interface ISagaTimeoutDispatcher
{
    /// <summary>
    /// Delivers the timeouts due now (up to <see cref="SagaFluxOptions.TimeoutBatchSize"/>), each in its own DI scope.
    /// </summary>
    /// <remarks>
    /// A delivered timeout is removed from the store. A timeout whose delivery throws stays pending and is retried
    /// after <see cref="SagaFluxOptions.TimeoutRetryDelay"/>. Delivery is at-least-once: a transition handling a
    /// timeout event should tolerate a repeat (it usually does, because the instance has left the waiting state).
    /// </remarks>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The number of timeouts delivered successfully.</returns>
    Task<int> DispatchDueAsync(CancellationToken ct = default);
}
