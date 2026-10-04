namespace EventFlux.Saga.Internal;

internal sealed class SagaEffects(ISagaRequester? requester = null)
{
    public ISagaRequester? Requester { get; } = requester;

    public List<object> Asked { get; } = new();

    public List<SagaPublishedEvent> Published { get; } = new();

    public List<SagaScheduledTimeout> Scheduled { get; } = new();

    public List<string> Unscheduled { get; } = new();

    public void Schedule(SagaScheduledTimeout timeout)
    {
        Unscheduled.Remove(timeout.Key);
        Scheduled.RemoveAll(s => s.Key == timeout.Key);
        Scheduled.Add(timeout);
    }

    public void Unschedule(string key)
    {
        Scheduled.RemoveAll(s => s.Key == key);
        if (!Unscheduled.Contains(key))
            Unscheduled.Add(key);
    }

    public (int Published, SagaScheduledTimeout[] Scheduled, string[] Unscheduled) Snapshot()
        => (Published.Count, Scheduled.ToArray(), Unscheduled.ToArray());

    public void Restore((int Published, SagaScheduledTimeout[] Scheduled, string[] Unscheduled) snapshot)
    {
        Published.RemoveRange(snapshot.Published, Published.Count - snapshot.Published);
        Scheduled.Clear();
        Scheduled.AddRange(snapshot.Scheduled);
        Unscheduled.Clear();
        Unscheduled.AddRange(snapshot.Unscheduled);
    }
}
