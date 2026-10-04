namespace EventFlux.Saga;

/// <summary>
/// Loads and stores saga instances.
/// </summary>
/// <typeparam name="TState">The saga state type.</typeparam>
public interface ISagaRepository<TState> where TState : class, ISagaState
{
    /// <summary>
    /// Finds the saga instance with the given correlation id.
    /// </summary>
    /// <param name="correlationId">The correlation id of the instance.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The instance, or <c>null</c> when none exists.</returns>
    Task<TState?> FindAsync(Guid correlationId, CancellationToken ct);

    /// <summary>
    /// Inserts or updates a saga instance using optimistic concurrency.
    /// </summary>
    /// <remarks>
    /// The stored version must equal <see cref="ISagaState.Version"/> of <paramref name="state"/> (0 for a new
    /// instance). On success the repository increments <see cref="ISagaState.Version"/>.
    /// </remarks>
    /// <param name="state">The instance to save.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the save operation.</returns>
    /// <exception cref="SagaConcurrencyException">Thrown when the instance was changed by someone else since it was loaded.</exception>
    Task SaveAsync(TState state, CancellationToken ct);

    /// <summary>
    /// Removes an instance, used by sagas that call <c>RemoveWhenFinal()</c>. Removing a missing instance does nothing.
    /// </summary>
    /// <remarks>The default implementation throws <see cref="NotSupportedException"/>.</remarks>
    /// <param name="correlationId">The instance correlation id.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    /// <exception cref="NotSupportedException">Thrown when the repository cannot remove instances.</exception>
    Task RemoveAsync(Guid correlationId, CancellationToken ct)
        => throw new NotSupportedException(
            $"{GetType().Name} cannot remove saga instances. Implement {nameof(RemoveAsync)} or do not call RemoveWhenFinal() in the saga.");
}
