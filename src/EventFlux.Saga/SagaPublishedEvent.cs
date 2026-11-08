using System.ComponentModel;
using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// An event a saga published during a transition.
/// </summary>
public sealed class SagaPublishedEvent
{
    private readonly Func<ISagaTransport, CancellationToken, Task> _send;

    private SagaPublishedEvent(IEventRequest @event, Func<ISagaTransport, CancellationToken, Task> send)
    {
        Event = @event;
        _send = send;
    }

    /// <summary>
    /// Gets the published event.
    /// </summary>
    public IEventRequest Event { get; }

    internal static SagaPublishedEvent Create<TEvent>(TEvent @event) where TEvent : IEventRequest
        => new(@event, (transport, ct) => transport.PublishAsync(@event, ct));

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, Func<IEventRequest, SagaPublishedEvent>> Factories = new();

    /// <summary>
    /// Wraps an event whose type is only known at run time (for example one read back from an outbox).
    /// </summary>
    /// <param name="event">The event.</param>
    /// <returns>The published event.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public static SagaPublishedEvent FromEvent(IEventRequest @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        var factory = Factories.GetOrAdd(@event.GetType(), type =>
        {
            var create = typeof(SagaPublishedEvent)
                .GetMethod(nameof(Create), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
                .MakeGenericMethod(type);
            return e => (SagaPublishedEvent)create.Invoke(null, [e])!;
        });
        return factory(@event);
    }

    /// <summary>
    /// Sends the event through <paramref name="transport"/>, preserving its static type.
    /// </summary>
    /// <param name="transport">The transport.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the publish operation.</returns>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task SendAsync(ISagaTransport transport, CancellationToken ct) => _send(transport, ct);

    /// <inheritdoc />
    public override string ToString() => Event.GetType().Name;
}
