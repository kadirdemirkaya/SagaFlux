using EfCore.Repository.Abstractions;
using EfCore.Repository.Extensions;
using EventFlux.Abstractions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.EfCore.Test;

public sealed class EfCoreSagaIntegrationTests : IDisposable
{
    private readonly SqliteDatabase _db = new();

    [Fact]
    public async Task Saga_state_is_persisted_across_scopes()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDbContext<SagaDbContext>(o => o.UseSqlite(_db.Connection));
        services.AddEventBus(typeof(EfCoreSagaIntegrationTests).Assembly);
        services.AddSagaFlux()
                .AddSaga<OrderSaga, OrderState>()
                .UseEfCore<OrderState, SagaDbContext>();
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var orderId = Guid.NewGuid();

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new OrderPlaced(orderId, 12m));

        await using (var context = _db.CreateContext())
        {
            var stored = await context.Set<OrderState>().SingleAsync(s => s.CorrelationId == orderId);
            Assert.Equal(OrderSaga.AwaitingPayment.Name, stored.CurrentState);
            Assert.Equal(12m, stored.Amount);
        }

        await using (var scope = provider.CreateAsyncScope())
            await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(new PaymentReceived(orderId));

        await using (var context = _db.CreateContext())
        {
            var stored = await context.Set<OrderState>().SingleAsync(s => s.CorrelationId == orderId);
            Assert.Equal(SagaStates.Final.Name, stored.CurrentState);
            Assert.Equal(2, stored.Version);
        }
    }

    [Fact]
    public void UseEfCore_replaces_the_in_memory_repository()
    {
        var services = new ServiceCollection();
        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseEfCore<OrderState, SagaDbContext>();

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(ISagaRepository<OrderState>));
        Assert.Equal(typeof(EfCoreSagaRepository<OrderState>), descriptor.ImplementationType);
        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
    }

    [Fact]
    public void Repositories_use_the_scoped_context_and_are_registered_once()
    {
        var services = new ServiceCollection();
        services.AddDbContext<SagaDbContext>(o => o.UseSqlite(_db.Connection));
        services.AddSagaFlux()
                .AddSaga<OrderSaga, OrderState>()
                .UseEfCore<OrderState, SagaDbContext>()
                .UseEfCore<OrderState, SagaDbContext>();
        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        Assert.Single(services, d => d.ServiceType == typeof(IReadRepository<OrderState>));
        Assert.Single(services, d => d.ServiceType == typeof(IWriteRepository<OrderState>));

        using var first = provider.CreateScope();
        using var second = provider.CreateScope();
        var context = first.ServiceProvider.GetRequiredService<SagaDbContext>();
        Assert.Same(context, first.ServiceProvider.GetRequiredService<IWriteRepository<OrderState>>().Table);
        Assert.Same(context, first.ServiceProvider.GetRequiredService<IReadRepository<OrderState>>().Table);
        Assert.NotSame(context, second.ServiceProvider.GetRequiredService<IWriteRepository<OrderState>>().Table);
    }

    [Fact]
    public void Repositories_registered_by_the_application_are_kept()
    {
        var services = new ServiceCollection();
        services.AddEfCoreRepository<OrderState, SagaDbContext>();
        var before = services.Count(d => d.ServiceType == typeof(IReadRepository<OrderState>));

        services.AddSagaFlux().AddSaga<OrderSaga, OrderState>().UseEfCore<OrderState, SagaDbContext>();

        Assert.Equal(before, services.Count(d => d.ServiceType == typeof(IReadRepository<OrderState>)));
    }

    public void Dispose() => _db.Dispose();
}
