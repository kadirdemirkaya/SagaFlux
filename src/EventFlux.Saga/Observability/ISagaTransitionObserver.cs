namespace EventFlux.Saga;

/// <summary>
/// Receives a <see cref="SagaTransitionRecord"/> after every handled event, once the instance has been saved and
/// before its events are published. Register implementations as <see cref="ISagaTransitionObserver"/> services.
/// </summary>
/// <remarks>
/// Observers must not influence the saga: an exception thrown by an observer is logged and swallowed.
/// </remarks>
public interface ISagaTransitionObserver
{
    /// <summary>
    /// Handles a transition record.
    /// </summary>
    /// <param name="record">The record.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct);
}
