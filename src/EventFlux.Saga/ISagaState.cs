namespace EventFlux.Saga;

/// <summary>
/// The persisted data of a single saga instance.
/// </summary>
/// <remarks>
/// Implement this on your own state class and add the business data the saga needs. The class must have a public
/// parameterless constructor so that SagaFlux can create new instances.
/// </remarks>
public interface ISagaState
{
    /// <summary>
    /// Gets or sets the identifier that correlates events with this saga instance.
    /// </summary>
    Guid CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the <see cref="SagaState.Name"/> of the state the instance is currently in.
    /// </summary>
    string CurrentState { get; set; }

    /// <summary>
    /// Gets or sets the version used for optimistic concurrency. Managed by the <see cref="ISagaRepository{TState}"/>.
    /// </summary>
    int Version { get; set; }
}
