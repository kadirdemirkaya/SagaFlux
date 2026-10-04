using EventFlux.Abstractions;

namespace EventFlux.Saga;

/// <summary>
/// An event a saga transition published, kept in the outbox until the transport has sent it.
/// </summary>
/// <param name="Id">The message id.</param>
/// <param name="Event">The event.</param>
/// <param name="StagedAt">When the transition staged it.</param>
public sealed record SagaOutboxMessage(Guid Id, IEventRequest Event, DateTimeOffset StagedAt);

/// <summary>
/// Keeps the events a transition publishes in the same unit of work as the instance, so saving the instance and
/// recording what it must publish either both happen or neither does (transactional outbox).
/// </summary>
/// <remarks>
/// <para>
/// The runner stages the messages before the repository saves the instance; the repository's save must commit them
/// (for example the EF Core outbox and repository share one <c>DbContext</c>). After saving, the runner sends the
/// events and completes them. If sending fails, or the process stops first, <see cref="ISagaOutboxDispatcher"/>
/// sends the pending messages later. Delivery is at least once.
/// </para>
/// <para>Register one with <c>UseOutbox&lt;T&gt;()</c> or <c>UseEfCoreOutbox&lt;TDbContext&gt;()</c>; without one, events
/// are sent right after the save and a failure there fails the message.</para>
/// </remarks>
public interface ISagaOutbox
{
    /// <summary>
    /// Adds messages to the current unit of work; the next repository save commits them.
    /// </summary>
    /// <param name="messages">The messages.</param>
    void Stage(IReadOnlyList<SagaOutboxMessage> messages);

    /// <summary>
    /// Drops the messages staged since the last save (the save failed or is retried).
    /// </summary>
    void Discard();

    /// <summary>
    /// Marks messages as sent and removes them.
    /// </summary>
    /// <param name="ids">The message ids.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A task representing the operation.</returns>
    Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct);

    /// <summary>
    /// Returns messages staged before <paramref name="stagedBefore"/> that are still pending, oldest first.
    /// </summary>
    /// <param name="stagedBefore">Only messages staged before this moment.</param>
    /// <param name="maxCount">The maximum number of messages.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The pending messages.</returns>
    Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct);
}

/// <summary>
/// Sends the outbox messages that were saved but not sent (see <see cref="ISagaOutbox"/>).
/// </summary>
public interface ISagaOutboxDispatcher
{
    /// <summary>
    /// Sends one batch of pending messages older than <see cref="SagaFluxOptions.OutboxDispatchDelay"/>.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The number of messages sent.</returns>
    Task<int> DispatchPendingAsync(CancellationToken ct = default);
}
