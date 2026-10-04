using EventFlux.Saga.Concurrency;
using Medallion.Threading.Redis;
using StackExchange.Redis;

namespace EventFlux.Saga.RedisFlow;

/// <summary>
/// <see cref="ISagaLockProvider"/> backed by DistributedLock.Redis (Medallion.Threading), on the Redis connection of
/// EventFlux.RedisFlow. Only one node at a time processes an event of a given saga instance.
/// </summary>
/// <param name="redis">The Redis connection.</param>
/// <param name="options">The lock options.</param>
public sealed class RedisSagaLockProvider(IConnectionMultiplexer redis, RedisSagaLockOptions options) : ISagaLockProvider
{
    private readonly Lazy<RedisDistributedSynchronizationProvider> _provider = new(() =>
        new RedisDistributedSynchronizationProvider(redis.GetDatabase(), builder =>
        {
            if (options.Expiry is { } expiry)
                builder.Expiry(expiry);
        }));

    /// <inheritdoc />
    public async ValueTask<IAsyncDisposable> AcquireAsync(string sagaName, Guid correlationId, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(sagaName);
        var key = $"{options.KeyPrefix}{sagaName}:{correlationId:N}";
        return await _provider.Value.CreateLock(key).AcquireAsync(options.AcquireTimeout, ct).ConfigureAwait(false);
    }
}
