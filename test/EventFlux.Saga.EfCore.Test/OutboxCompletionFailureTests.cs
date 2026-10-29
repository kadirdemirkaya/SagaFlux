using System.Text.Json;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed class OutboxCompletionFailureTests
{
    [Fact]
    public async Task Completion_failure_propagates_after_commit_and_send_then_dispatcher_redelivers_the_same_message()
    {
        await using var connection = new SqliteConnection("DataSource=:memory:");
        await connection.OpenAsync();
        var contextOptions = new DbContextOptionsBuilder<OutboxDbContext>().UseSqlite(connection).Options;
        await using (var context = new OutboxDbContext(contextOptions))
            await context.Database.EnsureCreatedAsync();

        var time = new FakeTimeProvider(new DateTimeOffset(2026, 10, 29, 12, 0, 0, TimeSpan.Zero));
        var stagedAt = time.GetUtcNow();
        var transport = new SwitchableTransport();
        var probe = new CompletionProbe(transport);
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(time);
        services.AddDbContext<OutboxDbContext>(o => o.UseSqlite(connection));
        services.AddEventBus(typeof(OutboxCompletionFailureTests).Assembly);
        services.AddSagaFlux(o => o.EnableOutboxPolling = false)
            .AddSaga<InvoiceSaga, InvoiceState>()
            .UseEfCore<InvoiceState, OutboxDbContext>()
            .UseEfCoreOutbox<OutboxDbContext>();
        services.AddSingleton<ISagaTransport>(transport);
        services.AddScoped<EfCoreSagaOutbox>();
        services.AddScoped<ISagaOutbox>(sp => new FailFirstCompletionOutbox(
            sp.GetRequiredService<EfCoreSagaOutbox>(), probe));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        Assert.Equal(TimeSpan.FromSeconds(30), provider.GetRequiredService<SagaFluxOptions>().OutboxDispatchDelay);
        var invoiceId = Guid.NewGuid();

        await using (var scope = provider.CreateAsyncScope())
        {
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => scope.ServiceProvider
                .GetRequiredService<IEventDispatcher>().PublishAsync(new InvoiceIssued(invoiceId)));
            Assert.Same(probe.Failure, exception);
        }

        var staged = Assert.Single(probe.Staged);
        Assert.NotEqual(Guid.Empty, staged.Id);
        Assert.Equal(stagedAt, staged.StagedAt);
        var firstSend = Assert.IsType<EmailInvoice>(Assert.Single(transport.Sent));
        Assert.Same(staged.Event, firstSend);
        Assert.Equal(invoiceId, firstSend.InvoiceId);
        Assert.Equal(staged.Id, Assert.Single(Assert.Single(probe.CompletionAttempts)));
        Assert.Equal(new[] { 1 }, probe.SentCountsAtCompletion);
        Assert.Equal(0, probe.SuccessfulCompletions);
        Assert.Equal(0, probe.Discards);
        await AssertSavedInvoiceAsync(contextOptions, invoiceId);
        await AssertPendingRowAsync(contextOptions, staged);

        var dispatcher = provider.GetRequiredService<ISagaOutboxDispatcher>();
        Assert.Equal(0, await dispatcher.DispatchPendingAsync());
        Assert.Empty(Assert.Single(probe.PendingBatches));
        Assert.Single(transport.Sent);
        Assert.Single(probe.CompletionAttempts);
        await AssertPendingRowAsync(contextOptions, staged);

        time.Advance(TimeSpan.FromSeconds(31));
        Assert.Equal(1, await dispatcher.DispatchPendingAsync());

        Assert.Equal(2, probe.PendingBatches.Count);
        var recovered = Assert.Single(probe.PendingBatches[1]);
        Assert.Equal(staged.Id, recovered.Id);
        Assert.Equal(staged.StagedAt, recovered.StagedAt);
        Assert.Equal(firstSend, Assert.IsType<EmailInvoice>(recovered.Event));
        Assert.NotSame(firstSend, recovered.Event);
        Assert.Collection(transport.Sent,
            sent => Assert.Same(firstSend, sent),
            sent => Assert.Same(recovered.Event, sent));
        Assert.Equal(2, probe.CompletionAttempts.Count);
        Assert.All(probe.CompletionAttempts, ids => Assert.Equal(staged.Id, Assert.Single(ids)));
        Assert.Equal(new[] { 1, 2 }, probe.SentCountsAtCompletion);
        Assert.Equal(1, probe.SuccessfulCompletions);
        Assert.Single(probe.Staged);
        Assert.Equal(0, probe.Discards);
        await AssertSavedInvoiceAsync(contextOptions, invoiceId);
        await using (var context = new OutboxDbContext(contextOptions))
            Assert.Empty(await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync());

        Assert.Equal(0, await dispatcher.DispatchPendingAsync());
        Assert.Equal(3, probe.PendingBatches.Count);
        Assert.Empty(probe.PendingBatches[2]);
        Assert.Equal(2, transport.Sent.Count);
        Assert.Equal(2, probe.CompletionAttempts.Count);
        Assert.Equal(1, probe.SuccessfulCompletions);
    }

    private static async Task AssertSavedInvoiceAsync(DbContextOptions<OutboxDbContext> options, Guid invoiceId)
    {
        await using var context = new OutboxDbContext(options);
        var saved = Assert.Single(await context.Set<InvoiceState>().AsNoTracking().ToListAsync());
        Assert.Equal(invoiceId, saved.CorrelationId);
        Assert.Equal(InvoiceSaga.Issued.Name, saved.CurrentState);
        Assert.Equal(1, saved.Version);
    }

    private static async Task AssertPendingRowAsync(DbContextOptions<OutboxDbContext> options, SagaOutboxMessage staged)
    {
        await using var context = new OutboxDbContext(options);
        var row = Assert.Single(await context.Set<SagaOutboxRecord>().AsNoTracking().ToListAsync());
        Assert.Equal(staged.Id, row.Id);
        Assert.Equal(SagaEventTypeNames.GetName(typeof(EmailInvoice)), row.EventType);
        Assert.Equal(JsonSerializer.Serialize((EmailInvoice)staged.Event), row.Payload);
        Assert.Equal(staged.StagedAt.UtcTicks, row.StagedAtUtcTicks);
    }

    private sealed class CompletionProbe(SwitchableTransport transport)
    {
        public InvalidOperationException Failure { get; } = new("outbox completion unavailable");
        public List<SagaOutboxMessage> Staged { get; } = [];
        public List<Guid[]> CompletionAttempts { get; } = [];
        public List<int> SentCountsAtCompletion { get; } = [];
        public List<SagaOutboxMessage[]> PendingBatches { get; } = [];
        public int SuccessfulCompletions { get; set; }
        public int Discards { get; set; }
        public int SentCount => transport.Sent.Count;
    }

    private sealed class FailFirstCompletionOutbox(EfCoreSagaOutbox inner, CompletionProbe probe) : ISagaOutbox
    {
        public void Stage(IReadOnlyList<SagaOutboxMessage> messages)
        {
            inner.Stage(messages);
            probe.Staged.AddRange(messages);
        }

        public void Discard()
        {
            probe.Discards++;
            inner.Discard();
        }

        public async Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct)
        {
            probe.CompletionAttempts.Add(ids.ToArray());
            probe.SentCountsAtCompletion.Add(probe.SentCount);
            if (probe.CompletionAttempts.Count == 1)
                throw probe.Failure;
            await inner.CompleteAsync(ids, ct);
            probe.SuccessfulCompletions++;
        }

        public async Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct)
        {
            var pending = await inner.GetPendingAsync(stagedBefore, maxCount, ct);
            probe.PendingBatches.Add(pending.ToArray());
            return pending;
        }
    }
}
