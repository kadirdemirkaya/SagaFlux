namespace EventFlux.Saga.Inspector;

/// <summary>
/// Stores saga transition records for the inspector.
/// </summary>
public interface ISagaTimelineStore
{
    /// <summary>
    /// Appends a record.
    /// </summary>
    /// <param name="record">The record.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task AppendAsync(SagaTransitionRecord record, CancellationToken ct);

    /// <summary>
    /// Returns the timeline of one saga instance, oldest first.
    /// </summary>
    /// <param name="correlationId">The saga instance.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The records of the instance.</returns>
    Task<IReadOnlyList<SagaTransitionRecord>> GetTimelineAsync(Guid correlationId, CancellationToken ct);

    /// <summary>
    /// Returns the most recent records across all instances, newest first.
    /// </summary>
    /// <param name="take">The maximum number of records.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The records.</returns>
    Task<IReadOnlyList<SagaTransitionRecord>> GetRecentAsync(int take, CancellationToken ct);
}
