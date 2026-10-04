namespace EventFlux.Saga;

/// <summary>
/// Lists the EventFlux notification types the registered sagas react to.
/// </summary>
/// <remarks>
/// Transport packages use it to subscribe to exactly the events that must reach a saga.
/// </remarks>
public interface ISagaEventCatalog
{
    /// <summary>
    /// Gets the notification types handled by at least one registered saga.
    /// </summary>
    IReadOnlyCollection<Type> EventTypes { get; }
}
