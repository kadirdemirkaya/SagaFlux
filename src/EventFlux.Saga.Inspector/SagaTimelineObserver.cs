namespace EventFlux.Saga.Inspector;

internal sealed class SagaTimelineObserver(ISagaTimelineStore store) : ISagaTransitionObserver
{
    public Task OnTransitionAsync(SagaTransitionRecord record, CancellationToken ct) => store.AppendAsync(record, ct);
}
