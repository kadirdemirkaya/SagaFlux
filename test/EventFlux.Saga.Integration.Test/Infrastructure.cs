namespace EventFlux.Saga.Integration.Test;

/// <summary>
/// Runs only when <c>SAGAFLUX_RABBITMQ_HOST</c> names a reachable RabbitMQ broker (for example <c>localhost</c>).
/// </summary>
public sealed class RabbitMqFactAttribute : FactAttribute
{
    public const string Variable = "SAGAFLUX_RABBITMQ_HOST";

    public RabbitMqFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Host))
            Skip = $"Set {Variable} to run RabbitMQ integration tests.";
    }

    public static string? Host => Environment.GetEnvironmentVariable(Variable);
}

/// <summary>
/// Runs only when <c>SAGAFLUX_REDIS</c> holds a Redis connection string (for example <c>localhost:6379</c>).
/// </summary>
public sealed class RedisFactAttribute : FactAttribute
{
    public const string Variable = "SAGAFLUX_REDIS";

    public RedisFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            Skip = $"Set {Variable} to run Redis integration tests.";
    }

    public static string? ConnectionString => Environment.GetEnvironmentVariable(Variable);
}

internal static class Eventually
{
    public static async Task<T> WaitAsync<T>(Func<Task<T?>> probe, Func<T, bool> done, TimeSpan? timeout = null)
        where T : class
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));
        T? last = null;
        while (DateTime.UtcNow < deadline)
        {
            last = await probe();
            if (last is not null && done(last))
                return last;
            await Task.Delay(100);
        }
        throw new TimeoutException($"Condition not met in time; last value: {last?.ToString() ?? "<null>"}.");
    }
}
