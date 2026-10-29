using EfCore.Repository.Concretes;
using Microsoft.EntityFrameworkCore;

namespace EventFlux.Saga.EfCore.Test;

public sealed class FinalRemovalConcurrencyTests
{
    [Fact]
    public async Task Removing_a_tracked_stale_final_instance_preserves_the_newer_row()
    {
        using var database = new SqliteDatabase();
        var id = Guid.NewGuid();
        await using var first = database.CreateContext();
        var firstRepository = Repository(first);
        var state = new OrderState { CorrelationId = id, CurrentState = SagaStates.Final.Name, Amount = 7m };
        await firstRepository.SaveAsync(state, default);
        Assert.Equal(1, state.Version);
        Assert.Same(state, await firstRepository.FindAsync(id, default));
        Assert.Equal(EntityState.Unchanged, first.Entry(state).State);

        await using (var second = database.CreateContext())
        {
            var secondRepository = Repository(second);
            var newer = Assert.IsType<OrderState>(await secondRepository.FindAsync(id, default));
            newer.Amount = 9m;
            await secondRepository.SaveAsync(newer, default);
            Assert.Equal(2, newer.Version);
        }

        var error = await Assert.ThrowsAsync<SagaConcurrencyException>(() => firstRepository.RemoveAsync(id, default));

        Assert.Equal(id, error.CorrelationId);
        Assert.Equal(1, error.ExpectedVersion);
        Assert.Equal(1, state.Version);
        Assert.Empty(first.ChangeTracker.Entries());
        await using var verify = database.CreateContext();
        var persisted = Assert.Single(await verify.Set<OrderState>().AsNoTracking().ToListAsync());
        Assert.Equal(id, persisted.CorrelationId);
        Assert.Equal(SagaStates.Final.Name, persisted.CurrentState);
        Assert.Equal(2, persisted.Version);
        Assert.Equal(9m, persisted.Amount);
    }

    private static EfCoreSagaRepository<OrderState> Repository(SagaDbContext context)
        => new(new ReadRepository<OrderState>(context, null!), new WriteRepository<OrderState>(context, null!));
}
