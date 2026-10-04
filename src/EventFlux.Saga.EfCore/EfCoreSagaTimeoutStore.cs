using System.Text.Json;
using EfCore.Repository.Abstractions;
using EventFlux.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.EfCore;

/// <summary>
/// <see cref="ISagaTimeoutStore"/> on top of the EfCore.RepositoryPack read/write repositories of
/// <see cref="SagaTimeoutRecord"/>.
/// </summary>
/// <remarks>
/// Timeout events are stored as JSON with <see cref="JsonSerializer"/>, so they must round-trip through it. A record
/// whose event type can no longer be resolved is skipped and logged, never deleted; declare the old name with
/// <see cref="SagaEventFormerNameAttribute"/> on a renamed or moved event type to keep such records resolving.
/// </remarks>
/// <param name="reader">The EfCore.RepositoryPack read repository of <see cref="SagaTimeoutRecord"/>.</param>
/// <param name="writer">The EfCore.RepositoryPack write repository of <see cref="SagaTimeoutRecord"/> (same <see cref="DbContext"/>).</param>
/// <param name="logger">The logger.</param>
public sealed class EfCoreSagaTimeoutStore(
    IReadRepository<SagaTimeoutRecord> reader,
    IWriteRepository<SagaTimeoutRecord> writer,
    ILogger<EfCoreSagaTimeoutStore> logger) : ISagaTimeoutStore
{
    /// <inheritdoc />
    public async Task ScheduleAsync(SagaTimeout timeout, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(timeout);
        var existing = await reader.GetListAsync(r => r.CorrelationId == timeout.CorrelationId && r.Key == timeout.Key, ct).ConfigureAwait(false);
        writer.Remove(existing);

        var eventType = timeout.Event.GetType();
        writer.Add(new SagaTimeoutRecord
        {
            Id = timeout.Id,
            CorrelationId = timeout.CorrelationId,
            Key = timeout.Key,
            DueAtUtcTicks = timeout.DueAt.UtcTicks,
            EventType = SagaEventTypeNames.GetName(eventType),
            Payload = JsonSerializer.Serialize(timeout.Event, eventType)
        });
        await writer.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task CancelAsync(Guid correlationId, string key, CancellationToken ct)
        => await RemoveAsync(await reader.GetListAsync(r => r.CorrelationId == correlationId && r.Key == key, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task CancelAllAsync(Guid correlationId, CancellationToken ct)
        => await RemoveAsync(await reader.GetListAsync(r => r.CorrelationId == correlationId, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyList<SagaTimeout>> GetDueAsync(DateTimeOffset now, int maxCount, CancellationToken ct)
    {
        var nowTicks = now.UtcTicks;
        var records = await reader.GetQueryable()
            .AsNoTracking()
            .Where(r => r.DueAtUtcTicks <= nowTicks)
            .OrderBy(r => r.DueAtUtcTicks)
            .Take(maxCount)
            .ToListAsync(ct)
            .ConfigureAwait(false);

        var due = new List<SagaTimeout>(records.Count);
        foreach (var record in records)
        {
            var type = SagaEventTypeNames.Resolve(record.EventType);
            if (type is null || JsonSerializer.Deserialize(record.Payload, type) is not IEventRequest @event)
            {
                logger.LogWarning("Saga timeout {Id} for {CorrelationId} has an unresolvable event type '{EventType}' and is skipped.",
                    record.Id, record.CorrelationId, record.EventType);
                continue;
            }

            due.Add(new SagaTimeout(record.Id, record.CorrelationId, record.Key,
                new DateTimeOffset(record.DueAtUtcTicks, TimeSpan.Zero), @event));
        }

        return due;
    }

    /// <inheritdoc />
    public async Task CompleteAsync(Guid timeoutId, CancellationToken ct)
        => await RemoveAsync(await reader.GetListAsync(r => r.Id == timeoutId, ct).ConfigureAwait(false), ct).ConfigureAwait(false);

    private async Task RemoveAsync(List<SagaTimeoutRecord> records, CancellationToken ct)
    {
        if (records.Count == 0) return;
        writer.Remove(records);
        await writer.SaveChangesAsync(ct).ConfigureAwait(false);
    }
}
