namespace EventFlux.Saga;

/// <summary>
/// Runtime options of SagaFlux.
/// </summary>
public sealed class SagaFluxOptions
{
    private int _maxConcurrencyRetries = 3;
    private TimeSpan _timeoutPollingInterval = TimeSpan.FromSeconds(1);
    private int _timeoutBatchSize = 100;
    private TimeSpan _timeoutRetryDelay = TimeSpan.FromSeconds(30);
    private TimeSpan _outboxPollingInterval = TimeSpan.FromSeconds(5);
    private TimeSpan _outboxDispatchDelay = TimeSpan.FromSeconds(30);
    private int _outboxBatchSize = 100;

    /// <summary>
    /// Gets or sets how many times an event is applied when the instance keeps changing concurrently
    /// (<see cref="SagaConcurrencyException"/>). Default: 3.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int MaxConcurrencyRetries
    {
        get => _maxConcurrencyRetries;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _maxConcurrencyRetries = value;
        }
    }

    /// <summary>
    /// Gets or sets whether a hosted service delivers due timeouts periodically. Default: <c>true</c>.
    /// When <c>false</c>, call <see cref="ISagaTimeoutDispatcher.DispatchDueAsync"/> yourself.
    /// </summary>
    public bool EnableTimeoutPolling { get; set; } = true;

    /// <summary>
    /// Gets or sets how often the hosted service looks for due timeouts. Default: 1 second.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not positive.</exception>
    public TimeSpan TimeoutPollingInterval
    {
        get => _timeoutPollingInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            _timeoutPollingInterval = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum number of timeouts delivered per poll. Default: 100.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int TimeoutBatchSize
    {
        get => _timeoutBatchSize;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _timeoutBatchSize = value;
        }
    }

    /// <summary>
    /// Gets or sets how long a timeout whose delivery failed waits before it is retried. Default: 30 seconds.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    public TimeSpan TimeoutRetryDelay
    {
        get => _timeoutRetryDelay;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _timeoutRetryDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets whether a hosted service sends pending outbox messages periodically (only when an outbox is
    /// registered). Default: <c>true</c>. When <c>false</c>, call <see cref="ISagaOutboxDispatcher.DispatchPendingAsync"/> yourself.
    /// </summary>
    public bool EnableOutboxPolling { get; set; } = true;

    /// <summary>
    /// Gets or sets how often the hosted service looks for pending outbox messages. Default: 5 seconds.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is not positive.</exception>
    public TimeSpan OutboxPollingInterval
    {
        get => _outboxPollingInterval;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(value, TimeSpan.Zero);
            _outboxPollingInterval = value;
        }
    }

    /// <summary>
    /// Gets or sets how old a pending outbox message must be before the hosted service sends it. It leaves time for
    /// the runner to send its own messages right after saving. Default: 30 seconds.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    public TimeSpan OutboxDispatchDelay
    {
        get => _outboxDispatchDelay;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, TimeSpan.Zero);
            _outboxDispatchDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum number of outbox messages sent per poll. Default: 100.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int OutboxBatchSize
    {
        get => _outboxBatchSize;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _outboxBatchSize = value;
        }
    }
}
