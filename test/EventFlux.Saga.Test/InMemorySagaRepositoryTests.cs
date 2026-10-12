using System.Text.Json;
using System.Text.Json.Serialization;
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

    [Fact]
    public async Task New_save_serialization_failure_preserves_version_and_allows_retry()
    {
        var repository = new InMemorySagaRepository<SerializationFailureState>();
        var error = new JsonException("State serialization failed.");
        var state = new SerializationFailureState
        {
            CorrelationId = Guid.NewGuid(),
            CurrentState = "A",
            Value = "new",
            SerializationError = error
        };

        var thrown = await Assert.ThrowsAsync<JsonException>(() => repository.SaveAsync(state, default));

        Assert.Same(error, thrown);
        Assert.Equal(0, state.Version);
        Assert.Equal(0, repository.Count);
        Assert.Null(await repository.FindAsync(state.CorrelationId, default));

        state.SerializationError = null;
        await repository.SaveAsync(state, default);

        Assert.Equal(1, state.Version);
        Assert.Equal(1, repository.Count);
        var loaded = await repository.FindAsync(state.CorrelationId, default);
        Assert.NotNull(loaded);
        Assert.NotSame(state, loaded);
        Assert.Equal(1, loaded.Version);
        Assert.Equal("A", loaded.CurrentState);
        Assert.Equal("new", loaded.Value);
    }

    [Fact]
    public async Task Update_serialization_failure_preserves_stored_state_and_allows_retry()
    {
        var repository = new InMemorySagaRepository<SerializationFailureState>();
        var state = new SerializationFailureState
        {
            CorrelationId = Guid.NewGuid(),
            CurrentState = "A",
            Value = "original"
        };
        await repository.SaveAsync(state, default);

        var error = new JsonException("State serialization failed.");
        state.CurrentState = "B";
        state.Value = "updated";
        state.SerializationError = error;

        var thrown = await Assert.ThrowsAsync<JsonException>(() => repository.SaveAsync(state, default));

        Assert.Same(error, thrown);
        Assert.Equal(1, state.Version);
        Assert.Equal(1, repository.Count);
        var previous = await repository.FindAsync(state.CorrelationId, default);
        Assert.NotNull(previous);
        Assert.Equal(1, previous.Version);
        Assert.Equal("A", previous.CurrentState);
        Assert.Equal("original", previous.Value);

        state.SerializationError = null;
        await repository.SaveAsync(state, default);

        Assert.Equal(2, state.Version);
        Assert.Equal(1, repository.Count);
        var loaded = await repository.FindAsync(state.CorrelationId, default);
        Assert.NotNull(loaded);
        Assert.NotSame(state, loaded);
        Assert.Equal(2, loaded.Version);
        Assert.Equal("B", loaded.CurrentState);
        Assert.Equal("updated", loaded.Value);
    }

    public sealed class SerializationFailureState : ISagaState
    {
        private string _value = "";

        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = "";
        public int Version { get; set; }

        [JsonIgnore]
        public JsonException? SerializationError { get; set; }

        public string Value
        {
            get => SerializationError is { } error ? throw error : _value;
            set => _value = value;
        }
    }
}
