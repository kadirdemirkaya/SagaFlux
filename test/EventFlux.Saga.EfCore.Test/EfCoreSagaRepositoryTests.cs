using EfCore.Repository.Concretes;

namespace EventFlux.Saga.EfCore.Test;

public sealed class EfCoreSagaRepositoryTests : IDisposable
{
    private readonly SqliteDatabase _db = new();

    private static EfCoreSagaRepository<OrderState> Repository(SagaDbContext context)
        => new(new ReadRepository<OrderState>(context, null!), new WriteRepository<OrderState>(context, null!));

    [Fact]
    public async Task Instance_survives_a_new_context()
    {
        var id = Guid.NewGuid();
        await using (var context = _db.CreateContext())
        {
            var state = new OrderState { CorrelationId = id, CurrentState = "AwaitingPayment", Amount = 7m };
            await Repository(context).SaveAsync(state, default);
            Assert.Equal(1, state.Version);
        }

        await using (var context = _db.CreateContext())
        {
            var loaded = await Repository(context).FindAsync(id, default);
            Assert.NotNull(loaded);
            Assert.Equal("AwaitingPayment", loaded!.CurrentState);
            Assert.Equal(7m, loaded.Amount);
            Assert.Equal(1, loaded.Version);
        }
    }

    [Fact]
    public async Task Remove_deletes_the_instance_after_it_was_saved_in_the_same_context()
    {
        var id = Guid.NewGuid();
        await using (var context = _db.CreateContext())
        {
            var repository = Repository(context);
            await repository.SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);
            var loaded = (await repository.FindAsync(id, default))!;
            loaded.CurrentState = SagaStates.Final.Name;
            await repository.SaveAsync(loaded, default);

            await repository.RemoveAsync(id, default);
            await repository.RemoveAsync(Guid.NewGuid(), default); // missing: no-op
        }

        await using var verify = _db.CreateContext();
        Assert.Null(await Repository(verify).FindAsync(id, default));
    }

    [Fact]
    public async Task Update_increments_version()
    {
        var id = Guid.NewGuid();
        await using var context = _db.CreateContext();
        var repository = Repository(context);
        await repository.SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);

        var loaded = (await repository.FindAsync(id, default))!;
        loaded.CurrentState = "B";
        await repository.SaveAsync(loaded, default);

        await using var verify = _db.CreateContext();
        var stored = (await Repository(verify).FindAsync(id, default))!;
        Assert.Equal("B", stored.CurrentState);
        Assert.Equal(2, stored.Version);
    }

    [Fact]
    public async Task Stale_update_throws_concurrency_exception()
    {
        var id = Guid.NewGuid();
        await using (var seed = _db.CreateContext())
            await Repository(seed).SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);

        await using var first = _db.CreateContext();
        await using var second = _db.CreateContext();
        var a = (await Repository(first).FindAsync(id, default))!;
        var b = (await Repository(second).FindAsync(id, default))!;

        a.CurrentState = "B";
        await Repository(first).SaveAsync(a, default);

        b.CurrentState = "C";
        var ex = await Assert.ThrowsAsync<SagaConcurrencyException>(() => Repository(second).SaveAsync(b, default));
        Assert.Equal(1, ex.ExpectedVersion);
        Assert.Equal(1, b.Version);

        var reloaded = (await Repository(second).FindAsync(id, default))!;
        Assert.Equal("B", reloaded.CurrentState);
    }

    [Fact]
    public async Task Detached_stale_instance_is_detected()
    {
        var id = Guid.NewGuid();
        await using (var seed = _db.CreateContext())
            await Repository(seed).SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);
        await using (var bump = _db.CreateContext())
        {
            var s = (await Repository(bump).FindAsync(id, default))!;
            await Repository(bump).SaveAsync(s, default);
        }

        await using var context = _db.CreateContext();
        var stale = new OrderState { CorrelationId = id, CurrentState = "X", Version = 1 };
        await Assert.ThrowsAsync<SagaConcurrencyException>(() => Repository(context).SaveAsync(stale, default));
    }

    [Fact]
    public async Task Concurrent_creation_throws_concurrency_exception()
    {
        var id = Guid.NewGuid();
        await using (var first = _db.CreateContext())
            await Repository(first).SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);

        await using var second = _db.CreateContext();
        await Assert.ThrowsAsync<SagaConcurrencyException>(
            () => Repository(second).SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default));
    }

    public void Dispose() => _db.Dispose();
}
