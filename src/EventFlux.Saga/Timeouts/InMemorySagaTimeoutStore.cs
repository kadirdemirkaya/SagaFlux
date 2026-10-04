namespace EventFlux.Saga.Timeouts;

/// <summary>
/// A process-local <see cref="ISagaTimeoutStore"/>. Pending timeouts are lost when the process stops.
/// </summary>
public sealed class InMemorySagaTimeoutStore : ISagaTimeoutStore
{
    private readonly object _gate = new();
    private readonly Dictionary<(Guid CorrelationId, string Key), SagaTimeout> _timeouts = new();

    /// <summary>
    /// Gets the number of pending timeouts.
    /// </summary>
    public int Count
    {
        get { lock (_gate) return _timeouts.Count; }
    }

    /// <inheritdoc />
    public Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(timeout);
        lock (_gate) _timeouts[(timeout.CorrelationId, timeout.Key)] = timeout;
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CancelAsync(Guid correlationId, string key, CancellationToken ct)
    {
        lock (_gate) _timeouts.Remove((correlationId, key));
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task CancelAllAsync(Guid correlationId, CancellationToken ct)
    {
        lock (_gate)
        {
            foreach (var key in _timeouts.Keys.Where(k => k.CorrelationId == correlationId).ToList())
                _timeouts.Remove(key);
        }
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
    {
        lock (_gate)
        {
            IReadOnlyList<SagaTimeout> due = _timeouts.Values
                .Where(t => t.DueAt <= now)
                .OrderBy(t => t.DueAt)
                .Take(maxCount)
                .ToList();
            return Task.FromResult(due);
        }
    }

    /// <inheritdoc />
    public Task CompleteAsync(Guid timeoutId, CancellationToken ct)
    {
        lock (_gate)
        {
            var match = _timeouts.FirstOrDefault(p => p.Value.Id == timeoutId);
            if (match.Value is not null)
                _timeouts.Remove(match.Key);
        }
        return Task.CompletedTask;
    }
}
