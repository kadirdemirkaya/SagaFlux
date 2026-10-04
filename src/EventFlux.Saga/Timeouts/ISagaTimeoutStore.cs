namespace EventFlux.Saga;

/// <summary>
/// Stores pending saga timeouts.
/// </summary>
public interface ISagaTimeoutStore
{
    /// <summary>
    /// Stores a timeout, replacing any pending timeout with the same <see cref="SagaTimeout.CorrelationId"/> and
    /// <see cref="SagaTimeout.Key"/>.
    /// </summary>
    /// <param name="timeout">The timeout.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct);

    /// <summary>
    /// Removes the pending timeout with the given key, if any.
    /// </summary>
    /// <param name="correlationId">The saga instance.</param>
    /// <param name="key">The timeout key.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task CancelAsync(Guid correlationId, string key, CancellationToken ct);

    /// <summary>
    /// Removes every pending timeout of a saga instance.
    /// </summary>
    /// <param name="correlationId">The saga instance.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task CancelAllAsync(Guid correlationId, CancellationToken ct);

    /// <summary>
    /// Returns up to <paramref name="maxCount"/> timeouts due at <paramref name="now"/>, earliest first.
    /// </summary>
    /// <param name="now">The current time.</param>
    /// <param name="maxCount">The maximum number of timeouts to return.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The due timeouts.</returns>
    Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct);

    /// <summary>
    /// Removes a delivered timeout. Does nothing when the scheduling was already replaced or cancelled.
    /// </summary>
    /// <param name="timeoutId">The <see cref="SagaTimeout.Id"/> of the delivered timeout.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task CompleteAsync(Guid timeoutId, CancellationToken ct);
}
