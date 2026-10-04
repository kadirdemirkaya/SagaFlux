using System.Text.Json;
using EfCore.Repository.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Inspector.EfCore;

/// <summary>
/// <see cref="ISagaTimelineStore"/> on top of the EfCore.RepositoryPack repositories of <see cref="SagaTimelineEntry"/>.
/// </summary>
/// <remarks>
/// Each call runs in its own DI scope (its own <see cref="DbContext"/>): recording a transition is an observation that
/// never joins, nor breaks, the unit of work of the saga. Entries are kept until <see cref="PruneAsync"/> deletes
/// them; call it from a scheduled job of your own to bound the history.
/// </remarks>
/// <param name="scopes">Creates the scope of each call.</param>
public sealed class EfCoreSagaTimelineStore(IServiceScopeFactory scopes) : ISagaTimelineStore
{
    /// <inheritdoc />
    public async Task AppendAsync(SagaTransitionRecord record, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(record);
        await using var scope = scopes.CreateAsyncScope();
        var writer = scope.ServiceProvider.GetRequiredService<IWriteRepository<SagaTimelineEntry>>();
        writer.Add(ToEntry(record));
        await writer.SaveChangesAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<SagaTransitionRecord>> GetTimelineAsync(Guid correlationId, CancellationToken ct)
    {
        await using var scope = scopes.CreateAsyncScope();
        var entries = await Query(scope)
            .Where(e => e.CorrelationId == correlationId)
            .OrderBy(e => e.OccurredAtUtcTicks).ThenBy(e => e.Version)
            .ToListAsync(ct)
            .ConfigureAwait(false);
        return entries.Select(ToRecord).ToList();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<SagaTransitionRecord>> GetRecentAsync(int take, CancellationToken ct)
    {
        if (take <= 0) return [];
        await using var scope = scopes.CreateAsyncScope();
        var entries = await Query(scope)
            .OrderByDescending(e => e.OccurredAtUtcTicks).ThenByDescending(e => e.Version)
            .Take(take)
            .ToListAsync(ct)
            .ConfigureAwait(false);
        return entries.Select(ToRecord).ToList();
    }

    /// <summary>
    /// Deletes the entries recorded before <paramref name="olderThan"/>.
    /// </summary>
    /// <param name="olderThan">Entries that occurred before this moment are deleted.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The number of deleted entries.</returns>
    public async Task<int> PruneAsync(DateTimeOffset olderThan, CancellationToken ct)
    {
        var ticks = olderThan.UtcTicks;
        await using var scope = scopes.CreateAsyncScope();
        return await scope.ServiceProvider.GetRequiredService<IReadRepository<SagaTimelineEntry>>().GetQueryable()
            .Where(e => e.OccurredAtUtcTicks < ticks)
            .ExecuteDeleteAsync(ct)
            .ConfigureAwait(false);
    }

    private static IQueryable<SagaTimelineEntry> Query(AsyncServiceScope scope)
        => scope.ServiceProvider.GetRequiredService<IReadRepository<SagaTimelineEntry>>().GetQueryable().AsNoTracking();

    private static SagaTimelineEntry ToEntry(SagaTransitionRecord r) => new()
    {
        Id = Guid.NewGuid(),
        CorrelationId = r.CorrelationId,
        SagaType = r.SagaType,
        EventType = r.EventType,
        FromState = r.FromState,
        ToState = r.ToState,
        Created = r.Created,
        Version = r.Version,
        Published = JsonSerializer.Serialize(r.Published),
        Compensated = JsonSerializer.Serialize(r.Compensated),
        Scheduled = JsonSerializer.Serialize(r.Scheduled),
        Unscheduled = JsonSerializer.Serialize(r.Unscheduled),
        Fault = r.Fault,
        Removed = r.Removed,
        OccurredAtUtcTicks = r.OccurredAt.UtcTicks,
    };

    private static SagaTransitionRecord ToRecord(SagaTimelineEntry e) => new(
        e.CorrelationId,
        e.SagaType,
        e.EventType,
        e.FromState,
        e.ToState,
        e.Created,
        e.Version,
        Names(e.Published),
        Names(e.Compensated),
        Names(e.Scheduled),
        Names(e.Unscheduled),
        new DateTimeOffset(e.OccurredAtUtcTicks, TimeSpan.Zero))
    {
        Fault = e.Fault,
        Removed = e.Removed,
    };

    private static string[] Names(string json) => JsonSerializer.Deserialize<string[]>(json) ?? [];
}
