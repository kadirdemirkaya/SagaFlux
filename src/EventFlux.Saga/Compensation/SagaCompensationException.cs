namespace EventFlux.Saga;

/// <summary>
/// Thrown when a persisted compensation key has no registered action, typically because the key was renamed or
/// removed in a newer deployment.
/// </summary>
/// <param name="correlationId">The saga instance.</param>
/// <param name="key">The unknown compensation key.</param>
public sealed class SagaCompensationException(Guid correlationId, string key)
    : Exception($"Saga instance '{correlationId}' has no compensation action registered for key '{key}'. Compensation keys must stay stable across deployments.")
{
    /// <summary>
    /// Gets the saga instance.
    /// </summary>
    public Guid CorrelationId { get; } = correlationId;

    /// <summary>
    /// Gets the unknown compensation key.
    /// </summary>
    public string Key { get; } = key;
}
