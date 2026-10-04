using EventFlux.Saga.Repositories;
using EventFlux.Saga.Test.Fixtures;

namespace EventFlux.Saga.Test;

public class InMemorySagaRepositoryTests
{
    private readonly InMemorySagaRepository<OrderState> _repository = new();

    [Fact]
    public async Task Save_then_find_returns_isolated_copy_with_incremented_version()
    {
        var state = new OrderState { CorrelationId = Guid.NewGuid(), CurrentState = "A", Amount = 5m };

        await _repository.SaveAsync(state, default);
        var loaded = await _repository.FindAsync(state.CorrelationId, default);

        Assert.NotNull(loaded);
        Assert.NotSame(state, loaded);
        Assert.Equal(1, state.Version);
        Assert.Equal(1, loaded!.Version);
        Assert.Equal(5m, loaded.Amount);
    }

    [Fact]
    public async Task Stale_update_throws_concurrency_exception()
    {
        var id = Guid.NewGuid();
        await _repository.SaveAsync(new OrderState { CorrelationId = id, CurrentState = "A" }, default);

        var first = (await _repository.FindAsync(id, default))!;
        var second = (await _repository.FindAsync(id, default))!;
        first.CurrentState = "B";
        await _repository.SaveAsync(first, default);

        second.CurrentState = "C";
        var ex = await Assert.ThrowsAsync<SagaConcurrencyException>(() => _repository.SaveAsync(second, default));
        Assert.Equal(1, ex.ExpectedVersion);
        Assert.Equal(1, second.Version);
        Assert.Equal("B", (await _repository.FindAsync(id, default))!.CurrentState);
    }

    [Fact]
    public async Task Inserting_existing_instance_as_new_throws()
    {
        var id = Guid.NewGuid();
        await _repository.SaveAsync(new OrderState { CorrelationId = id }, default);

        await Assert.ThrowsAsync<SagaConcurrencyException>(
            () => _repository.SaveAsync(new OrderState { CorrelationId = id }, default));
    }

    [Fact]
    public async Task Find_unknown_returns_null()
    {
        Assert.Null(await _repository.FindAsync(Guid.NewGuid(), default));
    }
}
