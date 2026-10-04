namespace EventFlux.Saga;

/// <summary>
/// A saga state that can wait for several events with <c>WhenAll&lt;...&gt;()</c>: it records which of them have
/// already arrived.
/// </summary>
/// <remarks>
/// Entries have the form <c>"{join}/{event type name}"</c> and are removed when their join completes. Persist the list
/// with the instance (the EF Core package stores it as a JSON text column).
/// </remarks>
public interface IJoinableSagaState : ISagaState
{
    /// <summary>
    /// Gets or sets the events received by joins that have not completed yet.
    /// </summary>
    List<string> JoinedEvents { get; set; }
}
