namespace EventFlux.Saga.Inspector;

/// <summary>
/// Options of the saga inspector.
/// </summary>
public sealed class SagaInspectorOptions
{
    private int _maxEntries = 10_000;

    /// <summary>
    /// Gets or sets how many transition records the in-memory timeline keeps; the oldest are evicted first.
    /// Default: 10,000.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int MaxEntries
    {
        get => _maxEntries;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _maxEntries = value;
        }
    }
}
