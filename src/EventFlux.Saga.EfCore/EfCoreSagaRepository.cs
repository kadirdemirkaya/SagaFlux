using EfCore.Repository.Abstractions;
using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.EfCore;

/// <summary>
/// <see cref="ISagaRepository{TState}"/> on top of the EfCore.RepositoryPack read/write repositories.
/// </summary>
/// <remarks>
/// Map <typeparamref name="TState"/> in your <see cref="DbContext"/> with
/// <see cref="SagaModelBuilderExtensions.ConfigureSagaState{TState}(ModelBuilder, int)"/> so that
/// <see cref="ISagaState.CorrelationId"/> is the key and <see cref="ISagaState.Version"/> is a concurrency token.
/// </remarks>
/// <typeparam name="TState">The saga state entity.</typeparam>
/// <param name="reader">The EfCore.RepositoryPack read repository of the state entity.</param>
/// <param name="writer">The EfCore.RepositoryPack write repository of the state entity (same <see cref="DbContext"/>).</param>
public sealed class EfCoreSagaRepository<TState>(IReadRepository<TState> reader, IWriteRepository<TState> writer)
    : ISagaRepository<TState>
    where TState : class, ISagaState, new()
{
    /// <inheritdoc />
    public Task<TState?> FindAsync(Guid correlationId, CancellationToken ct)
        => reader.GetAsync(s => s.CorrelationId == correlationId, asNoTracking: false, ct)!;

    /// <inheritdoc />
    public async Task SaveAsync(TState state, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(state);

        var expected = state.Version;
        state.Version = expected + 1;
        try
        {
            if (expected == 0)
                await InsertAsync(state, ct).ConfigureAwait(false);
            else
                await UpdateAsync(state, expected, ct).ConfigureAwait(false);
        }
        catch
        {
            state.Version = expected;
            throw;
        }
    }

    /// <inheritdoc />
    public async Task RemoveAsync(Guid correlationId, CancellationToken ct)
    {
        var existing = await reader.GetListAsync(s => s.CorrelationId == correlationId, ct).ConfigureAwait(false);
        if (existing.Count == 0) return;
        writer.Remove(existing);
        try
        {
            await writer.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateConcurrencyException)
        {
            writer.ClearChangeTracker();
            throw new SagaConcurrencyException(correlationId, existing[0].Version);
        }
    }

    private async Task InsertAsync(TState state, CancellationToken ct)
    {
        try
        {
            await writer.InsertAsync(state, ct).ConfigureAwait(false);
        }
        catch (DbUpdateException)
        {
            writer.ClearChangeTracker();
            if (await reader.AnyAsync(s => s.CorrelationId == state.CorrelationId, asNoTracking: true, ct).ConfigureAwait(false))
                throw new SagaConcurrencyException(state.CorrelationId, 0);
            throw;
        }
    }

    private async Task UpdateAsync(TState state, int expected, CancellationToken ct)
    {
        var entry = writer.Table.Entry(state);
        if (entry.State == EntityState.Detached)
            writer.Table.Attach(state);
        entry.State = EntityState.Modified;
        entry.Property(s => s.Version).OriginalValue = expected;

        try
        {
            await writer.SaveChangesAsync(ct).ConfigureAwait(false);
        }
        catch (DbUpdateConcurrencyException)
        {
            writer.ClearChangeTracker();
            throw new SagaConcurrencyException(state.CorrelationId, expected);
        }
    }
}
