using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// A pending timeout stored in an <see cref="ISagaTimeoutStore"/>.
/// </summary>
/// <param name="Id">The unique id of this scheduling.</param>
/// <param name="CorrelationId">The saga instance.</param>
/// <param name="Key">The timeout key, unique per instance.</param>
/// <param name="DueAt">When the timeout fires.</param>
/// <param name="Event">The event delivered to the saga when it fires.</param>
public sealed record SagaTimeout(Guid Id, Guid CorrelationId, string Key, DateTimeOffset DueAt, IEventRequest Event);
