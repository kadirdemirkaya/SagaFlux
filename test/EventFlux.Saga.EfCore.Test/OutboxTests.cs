using System.Collections.Concurrent;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed record InvoiceIssued(Guid CorrelationId) : ICorrelatedEvent;
public sealed record EmailInvoice(Guid InvoiceId) : IEventRequest;

public sealed class InvoiceState : ISagaState
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = string.Empty;
    public int Version { get; set; }
}

public sealed class InvoiceSaga : Saga<InvoiceState>
{
    public static readonly SagaState Issued = SagaStates.Define(nameof(Issued));

    public InvoiceSaga() => During(SagaStates.Initial,
        When<InvoiceIssued>().Publish(ctx => new EmailInvoice(ctx.CorrelationId)).TransitionTo(Issued));
}

public sealed class OutboxDbContext(DbContextOptions<OutboxDbContext> options) : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureSagaState<InvoiceState>().ToTable("InvoiceSagas");
        modelBuilder.ConfigureSagaOutbox();
    }
}

/// <summary>A broker stand-in that can be switched off.</summary>
public sealed class SwitchableTransport : ISagaTransport
{
    public bool Down { get; set; }
    public ConcurrentQueue<IEventRequest> Sent { get; } = new();

    public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
    {
        if (Down) throw new InvalidOperationException("broker unreachable");
        Sent.Enqueue(@event);
        return Task.CompletedTask;
    }
}

public sealed class OutboxTests : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 9, 27, 12, 0, 0, TimeSpan.Zero));
    private readonly SwitchableTransport _transport = new();

    public OutboxTests()
    {
        _connection.Open();
        using var context = CreateContext();
        context.Database.EnsureCreated();
    }

    private OutboxDbContext CreateContext() => new(new DbContextOptionsBuilder<OutboxDbContext>().UseSqlite(_connection).Options);

    private ServiceProvider Build(int maxConcurrencyRetries = 3, Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(_time);
        services.AddDbContext<OutboxDbContext>(o => o.UseSqlite(_connection));
        services.AddEventBus(typeof(OutboxTests).Assembly);
        services.AddSingleton(_transport);
        services.AddSagaFlux(o =>
            {
                o.MaxConcurrencyRetries = maxConcurrencyRetries;
                o.EnableOutboxPolling = false;
            })
            .AddSaga<InvoiceSaga, InvoiceState>()
            .UseEfCore<InvoiceState, OutboxDbContext>()
            .UseEfCoreOutbox<OutboxDbContext>();
        services.AddSingleton<ISagaTransport>(sp => sp.GetRequiredService<SwitchableTransport>());
        configure?.Invoke(services);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
    }

    private static async Task PublishAsync(IServiceProvider provider, IEventRequest @event)
    {
        await using var scope = provider.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IEventDispatcher>().PublishAsync(@event);
    }

    private int OutboxRows()
    {
        using var context = CreateContext();
        return context.Set<SagaOutboxRecord>().Count();
    }

    [Fact]
    public async Task Events_are_sent_after_the_save_and_the_outbox_is_emptied()
    {
        await using var provider = Build();
        var id = Guid.NewGuid();

        await PublishAsync(provider, new InvoiceIssued(id));

        Assert.Equal(id, Assert.IsType<EmailInvoice>(Assert.Single(_transport.Sent)).InvoiceId);
        Assert.Equal(0, OutboxRows());
    }

    [Fact]
    public async Task A_send_failure_keeps_the_saved_event_until_the_dispatcher_sends_it()
    {
        await using var provider = Build();
        var id = Guid.NewGuid();
        _transport.Down = true;

        await PublishAsync(provider, new InvoiceIssued(id)); // does not fail: the event is safe in the outbox

        await using (var context = CreateContext())
            Assert.Equal(InvoiceSaga.Issued.Name, (await context.Set<InvoiceState>().SingleAsync()).CurrentState);
        Assert.Equal(1, OutboxRows());
        var dispatcher = provider.GetRequiredService<ISagaOutboxDispatcher>();

        _transport.Down = false;
        Assert.Equal(0, await dispatcher.DispatchPendingAsync()); // younger than OutboxDispatchDelay
        _time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(1, await dispatcher.DispatchPendingAsync());

        Assert.Equal(id, Assert.IsType<EmailInvoice>(Assert.Single(_transport.Sent)).InvoiceId);
        Assert.Equal(0, OutboxRows());
    }

    [Fact]
    public async Task The_dispatcher_keeps_messages_while_the_transport_is_down()
    {
        await using var provider = Build();
        _transport.Down = true;
        await PublishAsync(provider, new InvoiceIssued(Guid.NewGuid()));
        _time.Advance(TimeSpan.FromMinutes(1));

        Assert.Equal(0, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());

        Assert.Equal(1, OutboxRows());
    }

    /// <summary>Makes the first insert of an instance collide with a row written by "another node".</summary>
    private sealed class CollidingRepository(EfCoreSagaRepository<InvoiceState> inner, Func<OutboxDbContext> otherNode) : ISagaRepository<InvoiceState>
    {
        private static int _collisions;

        public static void Reset() => _collisions = 0;

        public Task<InvoiceState?> FindAsync(Guid correlationId, CancellationToken ct) => inner.FindAsync(correlationId, ct);

        public async Task SaveAsync(InvoiceState state, CancellationToken ct)
        {
            if (state.Version == 0 && Interlocked.Increment(ref _collisions) == 1)
            {
                await using var other = otherNode();
                other.Add(new InvoiceState { CorrelationId = state.CorrelationId, CurrentState = SagaStates.Initial.Name, Version = 1 });
                await other.SaveChangesAsync(ct);
            }
            await inner.SaveAsync(state, ct);
        }
    }

    private void UseCollidingRepository(IServiceCollection services)
    {
        CollidingRepository.Reset();
        services.AddScoped<EfCoreSagaRepository<InvoiceState>>();
        services.AddScoped<ISagaRepository<InvoiceState>>(sp => new CollidingRepository(sp.GetRequiredService<EfCoreSagaRepository<InvoiceState>>(), CreateContext));
    }

    [Fact]
    public async Task A_retried_save_does_not_duplicate_outbox_messages()
    {
        await using var provider = Build(configure: UseCollidingRepository);
        var id = Guid.NewGuid();

        await PublishAsync(provider, new InvoiceIssued(id));

        Assert.Single(_transport.Sent);
        Assert.Equal(0, OutboxRows());
        await using var context = CreateContext();
        Assert.Equal((InvoiceSaga.Issued.Name, 2), await context.Set<InvoiceState>().Select(s => ValueTuple.Create(s.CurrentState, s.Version)).SingleAsync());
    }

    [Fact]
    public async Task A_failed_save_commits_no_outbox_message()
    {
        await using var provider = Build(maxConcurrencyRetries: 1, configure: UseCollidingRepository);

        await Assert.ThrowsAsync<SagaConcurrencyException>(() => PublishAsync(provider, new InvoiceIssued(Guid.NewGuid())));

        Assert.Empty(_transport.Sent);
        Assert.Equal(0, OutboxRows());
    }

    [Fact]
    public async Task An_unresolvable_message_is_skipped_not_deleted()
    {
        await using (var context = CreateContext())
        {
            context.Add(new SagaOutboxRecord { Id = Guid.NewGuid(), EventType = "Gone.Event, Gone", Payload = "{}", StagedAtUtcTicks = 0 });
            await context.SaveChangesAsync();
        }
        await using var provider = Build();

        Assert.Equal(0, await provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync());

        Assert.Equal(1, OutboxRows());
    }

    public void Dispose() => _connection.Dispose();
}
