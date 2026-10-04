namespace EventFlux.Saga.Concurrency;

/// <summary>
/// Serializes the processing of one saga instance across processes.
/// </summary>
/// <remarks>
/// <para>
/// Optimistic concurrency (<see cref="ISagaState.Version"/>) already keeps instances consistent; a lock only avoids
/// the wasted work of conflicting attempts when many events for the same instance arrive at once on several nodes.
/// The default <see cref="NoSagaLockProvider"/> takes no lock.
/// </para>
/// <para>
/// The lock covers loading, executing and saving the instance. It is released before the events the transition
/// published are sent, so a saga that reacts to its own events does not wait on itself.
/// </para>
/// <para>
/// Implementations wrap an existing distributed lock library (for example <c>EventFlux.Saga.RedisFlow</c> uses
/// DistributedLock.Redis); SagaFlux does not implement a lock of its own.
/// </para>
/// </remarks>
public interface ISagaLockProvider
{
    /// <summary>
    /// Acquires the lock of one saga instance, waiting until it is free.
    /// </summary>
    /// <param name="sagaName">The saga type name.</param>
    /// <param name="correlationId">The instance correlation id.</param>
    /// <param name="ct">A token that cancels waiting.</param>
    /// <returns>A handle that releases the lock when disposed.</returns>
    ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct);
}

/// <summary>
/// <see cref="ISagaLockProvider"/> that takes no lock; instances rely on optimistic concurrency only. The default.
/// </summary>
public sealed class NoSagaLockProvider : ISagaLockProvider
{
    private static readonly IAsyncDisposable Released = new NoLock();

    /// <inheritdoc />
    public ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct)
        => ValueTask.FromResult(Released);

    private sealed class NoLock : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
