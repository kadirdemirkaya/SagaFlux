using System.Reflection;
using EventFlux.Abstractions;
using EventFlux.Saga.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace EventFlux.Saga.Test;

public class AdditiveApiTests
{
    public sealed record QuotaRequested(Guid CorrelationId, int Units) : ICorrelatedEvent;
    public sealed record UncorrelatedSignal(string Text);
    public sealed record CheckQuota(int Units) : IEventRequest<QuotaDecision>;
    public sealed record QuotaDecision(bool Granted) : IEventResponse;

    public sealed class QuotaState : ISagaState
    {
        public Guid CorrelationId { get; set; }
        public string CurrentState { get; set; } = string.Empty;
        public int Version { get; set; }
        public int Limit { get; set; }
        public bool? Granted { get; set; }
    }

    public sealed class CheckQuotaHandler : IEventHandler<CheckQuota, QuotaDecision>
    {
        public async Task<QuotaDecision> Handle(CheckQuota @event, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return new QuotaDecision(@event.Units <= 10);
        }
    }

    private sealed class LimitedQuotaSaga : Saga<QuotaState>
    {
        public static readonly SagaState Decided = SagaStates.Define(nameof(Decided));

        public LimitedQuotaSaga(int limit) =>
            During(SagaStates.Initial,
                When<QuotaRequested>()
                    .Then(ctx => ctx.State.Limit = limit)
                    .AskAsync(ctx => new CheckQuota(ctx.Event.Units), async (ctx, decision) =>
                    {
                        await Task.Delay(10);
                        ctx.State.Granted = decision.Granted && ctx.Event.Units <= ctx.State.Limit;
                    })
                    .TransitionTo(Decided));
    }

    private sealed class UncorrelatableSaga : Saga<QuotaState>
    {
        public UncorrelatableSaga(string _) => During(SagaStates.Initial, When<UncorrelatedSignal>().Finalize());
    }

    private static ServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddEventBus(typeof(AdditiveApiTests).Assembly);
        return services;
    }

    [Fact]
    public async Task AddSaga_with_an_instance_registers_that_instance_and_routes_its_events()
    {
        var saga = new LimitedQuotaSaga(5);
        var services = Services();
        services.AddSagaFlux().AddSaga<LimitedQuotaSaga, QuotaState>(saga);
        await using var provider = services.BuildServiceProvider();
        var granted = Guid.NewGuid();
        var overLimit = Guid.NewGuid();
        var denied = Guid.NewGuid();

        Assert.Same(saga, provider.GetRequiredService<LimitedQuotaSaga>());
        var dispatcher = provider.GetRequiredService<IEventDispatcher>();
        await dispatcher.PublishAsync(new QuotaRequested(granted, 3));
        await dispatcher.PublishAsync(new QuotaRequested(overLimit, 7));
        await dispatcher.PublishAsync(new QuotaRequested(denied, 30));

        var repository = provider.GetRequiredService<ISagaRepository<QuotaState>>();
        foreach (var (id, expected) in new[] { (granted, true), (overLimit, false), (denied, false) })
        {
            var state = (await repository.FindAsync(id, default))!;
            Assert.Equal((LimitedQuotaSaga.Decided.Name, (bool?)expected, 5), (state.CurrentState, state.Granted, state.Limit));
        }
    }

    [Fact]
    public void AddSaga_with_an_instance_ignores_a_second_registration_of_the_same_saga_type()
    {
        var first = new LimitedQuotaSaga(5);
        var services = Services();

        services.AddSagaFlux().AddSaga<LimitedQuotaSaga, QuotaState>(first).AddSaga<LimitedQuotaSaga, QuotaState>(new LimitedQuotaSaga(9));

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(LimitedQuotaSaga));
        Assert.Same(first, descriptor.ImplementationInstance);
        Assert.Single(services, d => d.ServiceType == typeof(ISagaRunner));
        using var provider = services.BuildServiceProvider();
        Assert.Same(first, provider.GetRequiredService<LimitedQuotaSaga>());
    }

    [Fact]
    public void AddSaga_with_an_instance_validates_the_saga_and_rejects_null()
    {
        var builder = Services().AddSagaFlux();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddSaga<UncorrelatableSaga, QuotaState>(new UncorrelatableSaga("x")));
        Assert.Contains(nameof(UncorrelatedSignal), ex.Message);
        Assert.DoesNotContain(builder.Services, d => d.ServiceType == typeof(UncorrelatableSaga));
        Assert.Throws<ArgumentNullException>(() => builder.AddSaga<LimitedQuotaSaga, QuotaState>(null!));
    }

    [Fact]
    public void AddSagas_still_scans_assemblies_next_to_the_instance_overload()
    {
        var services = Services();

        services.AddSagaFlux().AddSagas(typeof(Fixtures.OrderSaga).Assembly);

        Assert.Contains(services, d => d.ServiceType == typeof(Fixtures.OrderSaga));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(LimitedQuotaSaga));
    }

    [Fact]
    public void The_previously_shipped_overloads_keep_their_signatures()
    {
        var addSaga = typeof(SagaFluxBuilder).GetMethods().Where(m => m.Name == nameof(SagaFluxBuilder.AddSaga)).ToList();
        var parameterless = Assert.Single(addSaga, m => m.GetParameters().Length == 0);
        Assert.True(parameterless.GetGenericArguments()[0].GenericParameterAttributes.HasFlag(GenericParameterAttributes.DefaultConstructorConstraint));
        Assert.Equal(typeof(SagaFluxBuilder), parameterless.ReturnType);
        var withInstance = Assert.Single(addSaga, m => m.GetParameters().Length == 1);
        Assert.False(withInstance.GetGenericArguments()[0].GenericParameterAttributes.HasFlag(GenericParameterAttributes.DefaultConstructorConstraint));

        var whenAll = typeof(Saga<QuotaState>).GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(m => m.Name == "WhenAll" && m.IsFamily).Select(m => m.GetGenericArguments().Length).OrderBy(n => n);
        Assert.Equal(new[] { 2, 3, 4, 5 }, whenAll);

        var builder = typeof(Saga<QuotaState>.SagaTransitionBuilder<QuotaRequested>);
        var ask = Assert.Single(builder.GetMethods(), m => m.Name == "Ask");
        Assert.Equal(typeof(Action<,>), ask.GetParameters()[1].ParameterType.GetGenericTypeDefinition());
        var askAsync = Assert.Single(builder.GetMethods(), m => m.Name == "AskAsync");
        Assert.Equal(typeof(Func<,,>), askAsync.GetParameters()[1].ParameterType.GetGenericTypeDefinition());
        Assert.Equal(typeof(Task), askAsync.GetParameters()[1].ParameterType.GetGenericArguments()[2]);
    }
}
