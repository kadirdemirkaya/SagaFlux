namespace EventFlux.Saga;

/// <summary>
/// Thrown by an <see cref="ISagaRepository{TState}"/> when a saga instance was modified concurrently.
/// </summary>
/// <param name="correlationId">The correlation id of the conflicting instance.</param>
/// <param name="expectedVersion">The version the caller loaded.</param>
public sealed class SagaConcurrencyException(Guid correlationId, int expectedVersion)
    : Exception($"Saga instance '{correlationId}' was modified concurrently (expected version {expectedVersion}).")
{
    /// <summary>
    /// Gets the correlation id of the conflicting instance.
    /// </summary>
    public Guid CorrelationId { get; } = correlationId;

    /// <summary>
    /// Gets the version the caller loaded.
    /// </summary>
    public int ExpectedVersion { get; } = expectedVersion;
}
