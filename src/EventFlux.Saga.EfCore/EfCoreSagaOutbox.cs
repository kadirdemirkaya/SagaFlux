using System.Text.Json;
using EfCore.Repository.Abstractions;
using EventFlux.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.EfCore;

/// <summary>
/// <see cref="ISagaOutbox"/> on top of the EfCore.RepositoryPack repositories of <see cref="SagaOutboxRecord"/>.
/// </summary>
/// <remarks>
/// Staged records are added to the scoped <c>DbContext</c> without saving; the <see cref="EfCoreSagaRepository{TState}"/>
/// save of the same scope commits them with the instance. Use it with EF Core saga repositories on the same context.
/// Events are stored as JSON with <see cref="JsonSerializer"/>; a record whose type can no longer be resolved is
/// skipped and logged, never deleted (see <see cref="SagaEventFormerNameAttribute"/>).
/// </remarks>
/// <param name="reader">The read repository of <see cref="SagaOutboxRecord"/>.</param>
/// <param name="writer">The write repository of <see cref="SagaOutboxRecord"/> (same <see cref="DbContext"/> as the saga repository).</param>
/// <param name="logger">The logger.</param>
public sealed class EfCoreSagaOutbox(
    IReadRepository<SagaOutboxRecord> reader,
    IWriteRepository<SagaOutboxRecord> writer,
    ILogger<EfCoreSagaOutbox> logger) : ISagaOutbox
{
    private readonly List<SagaOutboxRecord> _staged = new();

    /// <inheritdoc />
    public void Stage(IReadOnlyList<SagaOutboxMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        foreach (var message in messages)
        {
            var type = message.Event.GetType();
            var record = new SagaOutboxRecord
            {
                Id = message.Id,
                EventType = SagaEventTypeNames.GetName(type),
                Payload = JsonSerializer.Serialize(message.Event, type),
                StagedAtUtcTicks = message.StagedAt.UtcTicks,
            };
            writer.Add(record);
            _staged.Add(record);
        }
    }

    /// <inheritdoc />
    public void Discard()
    {
        foreach (var record in _staged)
        {
            var entry = writer.Table.Entry(record);
            if (entry.State == EntityState.Added)
                entry.State = EntityState.Detached;
        }
        _staged.Clear();
    }

    /// <inheritdoc />
    public async Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(ids);
        if (ids.Count == 0) return;

        foreach (var record in _staged.Where(r => ids.Contains(r.Id)).ToList())
        {
            var entry = writer.Table.Entry(record);
            if (entry.State == EntityState.Added)
                entry.State = EntityState.Detached;
            _staged.Remove(record);
        }

        var stored = await reader.GetListAsync(r => ids.Contains(r.Id), ct).ConfigureAwait(false);
        if (stored.Count == 0) return;
        writer.Remove(stored);
        await writer.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct)
    {
        var before = stagedBefore.UtcTicks;
        var query = reader.GetQueryable()
            .AsNoTracking()
            .Where(r => r.StagedAtUtcTicks < before);
        var recordCount = await query.CountAsync(ct).ConfigureAwait(false);
        var pending = new List<SagaOutboxMessage>();
        var offset = 0;
        while (offset < recordCount && pending.Count < maxCount)
        {
            var records = await query
                .OrderBy(r => r.StagedAtUtcTicks)
                .ThenBy(r => r.Id)
                .Skip(offset)
                .Take(Math.Min(maxCount - pending.Count, recordCount - offset))
                .ToListAsync(ct)
                .ConfigureAwait(false);
            if (records.Count == 0) break;
            offset += records.Count;

            foreach (var record in records)
            {
                var type = SagaEventTypeNames.Resolve(record.EventType);
                if (type is null || JsonSerializer.Deserialize(record.Payload, type) is not IEventRequest @event)
                {
                    SagaStoreMetrics.OutboxRowSkipped();
                    logger.LogWarning("Saga outbox message {Id} has an unresolvable event type '{EventType}' and is skipped.",
                        record.Id, record.EventType);
                    continue;
                }

                pending.Add(new SagaOutboxMessage(record.Id, @event, new DateTimeOffset(record.StagedAtUtcTicks, TimeSpan.Zero)));
            }
        }

        return pending;
    }
}
