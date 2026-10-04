namespace EventFlux.Saga;

/// <summary>
/// Marks an event that unwinds a saga: when it arrives for an existing, non-final instance, every pending
/// compensation runs in reverse order (LIFO), whether or not the current state declares a transition for it.
/// A matching transition, if any, runs first.
/// </summary>
/// <remarks>
/// Only sagas that can correlate the event (via <c>CorrelateBy</c> or <see cref="ICorrelatedEvent"/>) react to it.
/// EventFlux's dispatcher runs the saga pipeline only for events that have a handler: either declare a transition for
/// the event in the saga (which registers one automatically) or handle it somewhere in your application.
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = true, AllowMultiple = false)]
public sealed class CompensatingEventAttribute : Attribute
{
}
