namespace EventFlux.Saga;

/// <summary>
/// Factory and well-known values for <see cref="SagaState"/>.
/// </summary>
public static class SagaStates
{
    private static int _nextOrdinal = 1;

    /// <summary>
    /// The state of a saga instance that has just been created for its first event.
    /// Transitions declared with <c>During(SagaStates.Initial, ...)</c> are the ones allowed to start a new instance.
    /// </summary>
    public static readonly SagaState Initial = new(nameof(Initial), 0);

    /// <summary>
    /// The state of a saga instance that has completed. Events arriving for a final instance are ignored.
    /// </summary>
    public static readonly SagaState Final = new(nameof(Final), int.MaxValue);

    /// <summary>
    /// Defines a state with the given name and an automatically assigned ordinal.
    /// </summary>
    /// <remarks>
    /// Passing <c>nameof(Field)</c> keeps the persisted name tied to the field name, so renaming the field is a
    /// compile-time visible change. Defining a state named <c>Initial</c> or <c>Final</c> returns the matching
    /// well-known state.
    /// </remarks>
    /// <param name="name">The unique, persisted name of the state.</param>
    /// <returns>The defined state.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null, empty or whitespace.</exception>
    public static SagaState Define(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name == Initial.Name) return Initial;
        if (name == Final.Name) return Final;
        return new SagaState(name, Interlocked.Increment(ref _nextOrdinal));
    }

    /// <summary>
    /// Defines a state with the given name and an explicit ordinal.
    /// </summary>
    /// <param name="name">The unique, persisted name of the state.</param>
    /// <param name="ordinal">The declaration order of the state.</param>
    /// <returns>The defined state.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null, empty or whitespace.</exception>
    public static SagaState Define(string name, int ordinal)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return new SagaState(name, ordinal);
    }
}
