using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// An EventFlux notification that carries the correlation id of the saga instance it belongs to.
/// </summary>
/// <remarks>
/// Implementing this interface removes the need for <c>CorrelateBy</c> in the saga definition.
/// </remarks>
public interface ICorrelatedEvent : IEventRequest
{
    /// <summary>
    /// Gets the correlation id of the saga instance the event belongs to.
    /// </summary>
    Guid CorrelationId { get; }
}
