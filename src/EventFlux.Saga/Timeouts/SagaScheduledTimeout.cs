using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// A timeout requested by a transition: <see cref="Event"/> is delivered back to the saga after <see cref="Delay"/>.
/// </summary>
/// <param name="Key">The timeout key; scheduling the same key again replaces the pending timeout.</param>
/// <param name="Delay">The delay after the instance has been saved.</param>
/// <param name="Event">The event delivered when the timeout fires.</param>
public sealed record SagaScheduledTimeout(string Key, TimeSpan Delay, IEventRequest Event);
