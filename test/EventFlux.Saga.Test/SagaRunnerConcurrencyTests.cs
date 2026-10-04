using EventFlux.Abstractions;
using EventFlux.Saga.Repositories;
using EventFlux.Saga.Test.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class SagaRunnerConcurrencyTests
{
    [Fact]
    public async Task Conflicting_save_is_retried_with_fresh_state()
    {
        var repository = new ConflictOnceRepository();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSagaFlux(o => o.MaxConcurrencyRetries = 2)
                .AddSaga<OrderSaga, OrderState>()
                .UseTransport<NullTransport>()
                .Services.AddSingleton<ISagaRepository<OrderState>>(repository);
        await using var provider = services.BuildServiceProvider();
        var orderId = Guid.NewGuid();

        await provider.GetRequiredService<ISagaEventRouter>().RouteAsync(new OrderPlaced(orderId, 1m), default);

        Assert.Equal(2, repository.SaveAttempts);
        Assert.Equal(OrderSaga.AwaitingPayment.Name, (await repository.FindAsync(orderId, default))!.CurrentState);
    }

    [Fact]
    public async Task Conflict_after_last_retry_propagates()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSagaFlux(o => o.MaxConcurrencyRetries = 1)
                .AddSaga<OrderSaga, OrderState>()
                .UseTransport<NullTransport>()
                .Services.AddSingleton<ISagaRepository<OrderState>>(new ConflictOnceRepository());
        await using var provider = services.BuildServiceProvider();

        await Assert.ThrowsAsync<SagaConcurrencyException>(() =>
            provider.GetRequiredService<ISagaEventRouter>().RouteAsync(new OrderPlaced(Guid.NewGuid(), 1m), default));
    }

    private sealed class ConflictOnceRepository : ISagaRepository<OrderState>
    {
        private readonly InMemorySagaRepository<OrderState> _inner = new();
        public int SaveAttempts { get; private set; }

        public Task<OrderState?> FindAsync(Guid correlationId, CancellationToken ct) => _inner.FindAsync(correlationId, ct);

        public Task SaveAsync(OrderState state, CancellationToken ct)
        {
            if (++SaveAttempts == 1)
                throw new SagaConcurrencyException(state.CorrelationId, state.Version);
            return _inner.SaveAsync(state, ct);
        }
    }

    public sealed class NullTransport : ISagaTransport
    {
        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest => Task.CompletedTask;
    }
}
