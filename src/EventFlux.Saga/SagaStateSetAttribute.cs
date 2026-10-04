namespace EventFlux.Saga;

/// <summary>
/// Marks an enum whose members are the states of a saga. With the <c>EventFlux.Saga.Generators</c> package, the
/// compiler generates a static <c>{EnumName}Set</c> class holding one <see cref="SagaState"/> per member (reflection
/// free), so states are no longer written by hand.
/// </summary>
/// <remarks>
/// Members named <c>Initial</c> and <c>Final</c> map to <see cref="SagaStates.Initial"/> and
/// <see cref="SagaStates.Final"/>. Add a <c>{EnumName}.sagastates</c> file (one state name per line) as an
/// <c>AdditionalFiles</c> item to get a build warning when a state that may still be persisted is removed.
/// </remarks>
[AttributeUsage(AttributeTargets.Enum, Inherited = false, AllowMultiple = false)]
public sealed class SagaStateSetAttribute : Attribute
{
}
