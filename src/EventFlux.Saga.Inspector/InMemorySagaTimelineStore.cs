namespace EventFlux.Saga.Inspector;

/// <summary>
/// A bounded, process-local <see cref="ISagaTimelineStore"/>. It keeps the last
/// <see cref="SagaInspectorOptions.MaxEntries"/> records and loses them when the process stops.
/// </summary>
/// <param name="options">The inspector options.</param>
public sealed class InMemorySagaTimelineStore(SagaInspectorOptions options) : ISagaTimelineStore
{
    private readonly object _gate = new();
    private readonly LinkedList<SagaTransitionRecord> _records = new();

    /// <summary>
    /// Gets the number of stored records.
    /// </summary>
    public int Count
    {
        get { lock (_gate) return _records.Count; }
    }

    /// <inheritdoc />
    public Task AppendAsync(SagaTransitionRecord record, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(record);
        lock (_gate)
        {
            _records.AddLast(record);
            while (_records.Count > options.MaxEntries)
                _records.RemoveFirst();
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<SagaTransitionRecord>> GetTimelineAsync(Guid correlationId, CancellationToken ct)
    {
        lock (_gate)
        {
            IReadOnlyList<SagaTransitionRecord> timeline = _records.Where(r => r.CorrelationId == correlationId).ToList();
            return Task.FromResult(timeline);
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<SagaTransitionRecord>> GetRecentAsync(int take, CancellationToken ct)
    {
        lock (_gate)
        {
            IReadOnlyList<SagaTransitionRecord> recent = _records.Reverse().Take(Math.Max(0, take)).ToList();
            return Task.FromResult(recent);
        }
    }
}
