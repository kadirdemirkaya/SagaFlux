namespace EventFlux.Saga.RedisFlow;

/// <summary>
/// Options of the Redis saga instance lock.
/// </summary>
public sealed class RedisSagaLockOptions
{
    /// <summary>
    /// Gets or sets the prefix of the Redis keys the locks use. The key is <c>{prefix}{saga}:{correlation id}</c>.
    /// Default: <c>sagaflux:lock:</c>.
    /// </summary>
    public string KeyPrefix { get; set; } = "sagaflux:lock:";

    /// <summary>
    /// Gets or sets how long to wait for a busy instance before failing with <see cref="TimeoutException"/>, which
    /// leaves the message to the broker's retry. <c>null</c> waits indefinitely. Default: 30 seconds.
    /// </summary>
    public TimeSpan? AcquireTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the lifetime of a lock whose holder stopped renewing it (for example a crashed node). Held locks
    /// are renewed automatically. <c>null</c> keeps the distributed lock dependency's default (30 seconds).
    /// </summary>
    public TimeSpan? Expiry { get; set; }
}
