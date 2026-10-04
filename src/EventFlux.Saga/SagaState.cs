namespace EventFlux.Saga;

/// <summary>
/// A single state of a saga state machine.
/// </summary>
/// <remarks>
/// <para>
/// Only <see cref="Name"/> is persisted (in <see cref="ISagaState.CurrentState"/>), so two states are equal when their
/// names are equal; <see cref="Ordinal"/> is informational (declaration order, diagnostics, source generator output).
/// </para>
/// <para>
/// Declare states as <c>static readonly</c> fields of the saga class through <see cref="SagaStates.Define(string)"/>
/// instead of writing string literals:
/// <code>
/// public static readonly SagaState AwaitingPayment = SagaStates.Define(nameof(AwaitingPayment));
/// </code>
/// </para>
/// </remarks>
/// <param name="Name">The persisted, unique name of the state.</param>
/// <param name="Ordinal">The declaration order of the state; not persisted.</param>
public readonly record struct SagaState(string Name, int Ordinal)
{
    /// <summary>
    /// Converts a state to its persisted name.
    /// </summary>
    /// <param name="state">The state to convert.</param>
    public static implicit operator string(SagaState state) => state.Name;

    /// <summary>
    /// Determines whether two states have the same <see cref="Name"/>.
    /// </summary>
    /// <param name="other">The state to compare with.</param>
    /// <returns><c>true</c> when the names are equal (ordinal, case-sensitive); otherwise <c>false</c>.</returns>
    public bool Equals(SagaState other) => string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <inheritdoc />
    public override int GetHashCode() => Name is null ? 0 : StringComparer.Ordinal.GetHashCode(Name);

    /// <inheritdoc />
    public override string ToString() => Name;
}
