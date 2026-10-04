namespace EventFlux.Saga;

/// <summary>
/// A saga state that persists a compensation stack. Required for sagas that use <c>OnCompensate</c>.
/// </summary>
public interface ICompensableSagaState : ISagaState
{
    /// <summary>
    /// Gets or sets the append-only compensation stack, oldest entry first.
    /// </summary>
    List<CompensationEntry> Compensations { get; set; }
}
