using System.Collections.Concurrent;
using System.Text.Json;

namespace EventFlux.Saga.Repositories;

/// <summary>
/// A process-local <see cref="ISagaRepository{TState}"/> for tests, demos and single-process apps that do not need
/// durability. Instances are lost when the process stops.
/// </summary>
/// <remarks>
/// Stored instances are isolated from callers by a JSON round-trip, so <typeparamref name="TState"/> must be
/// serializable with <see cref="JsonSerializer"/> (public getters and setters). Optimistic concurrency is enforced
/// through <see cref="ISagaState.Version"/>.
/// </remarks>
/// <typeparam name="TState">The saga state type.</typeparam>
public sealed class InMemorySagaRepository<TState> : ISagaRepository<TState> where TState : class, ISagaState
{
    private readonly ConcurrentDictionary<Guid, string> _store = new();

    /// <summary>
    /// Gets the number of stored instances.
    /// </summary>
    public int Count => _store.Count;

    /// <inheritdoc />
    public Task<TState?> FindAsync(Guid correlationId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(_store.TryGetValue(correlationId, out var json) ? Deserialize(json) : null);
    }

    /// <inheritdoc />
    public Task SaveAsync(TState state, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(state);
        ct.ThrowIfCancellationRequested();

        var expected = state.Version;
        state.Version = expected + 1;
        string json;
        try
        {
            json = JsonSerializer.Serialize(state);
        }
        catch
        {
            state.Version = expected;
            throw;
        }

        bool saved;
        if (expected == 0)
        {
            saved = _store.TryAdd(state.CorrelationId, json);
        }
        else
        {
            saved = _store.TryGetValue(state.CorrelationId, out var current)
                    && Deserialize(current)!.Version == expected
                    && _store.TryUpdate(state.CorrelationId, json, current);
        }

        if (!saved)
        {
            state.Version = expected;
            throw new SagaConcurrencyException(state.CorrelationId, expected);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task RemoveAsync(Guid correlationId, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        _store.TryRemove(correlationId, out _);
        return Task.CompletedTask;
    }

    private static TState? Deserialize(string json) => JsonSerializer.Deserialize<TState>(json);
}
