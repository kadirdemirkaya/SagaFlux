namespace EventFlux.Saga;

/// <summary>
/// Declares a name an event type was stored under before it was renamed or moved, so records persisted with the old
/// name (for example pending timeouts in <c>EventFlux.Saga.EfCore</c>) keep resolving to this type.
/// </summary>
/// <remarks>
/// Stored names have the form <c>"Namespace.Type, Assembly"</c> (no version). Repeat the attribute for every former
/// name, and keep it until no stored record uses that name any more.
/// </remarks>
/// <example><code>
/// [SagaEventFormerName("Shop.Orders.PaymentTimeout, Shop.Orders")]
/// public sealed record PaymentExpired(Guid CorrelationId) : ICorrelatedEvent;
/// </code></example>
/// <param name="name">The former stored name.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = true, Inherited = false)]
public sealed class SagaEventFormerNameAttribute(string name) : Attribute
{
    /// <summary>
    /// Gets the former stored name.
    /// </summary>
    public string Name { get; } = name;
}
