using System.Text.Json;
using EventFlux.Abstractions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace EventFlux.Saga.EfCore.Test;

public sealed class OutboxDispatcherBoundaryTests
{
    [Fact]
    public async Task Pre_cancelled_query_propagates_without_sending_or_completing()
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        caller.Cancel();

        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.DispatchAsync(caller.Token));

        Assert.Equal(caller.Token, error.CancellationToken);
        Assert.Same(error, Assert.Single(fixture.Probe.QueryErrors));
        Assert.Equal(caller.Token, Assert.Single(fixture.Probe.QueryTokens));
        Assert.Empty(fixture.Probe.Batches);
        Assert.Empty(fixture.Probe.Attempts);
        Assert.Empty(fixture.Probe.CompletionAttempts);
        await fixture.AssertRowsAsync(fixture.Ids);
        fixture.AssertDisposedScopes(1);
        await fixture.AssertRecoveryAsync(fixture.Ids);
    }

    [Fact]
    public async Task Cancellation_after_pending_query_stops_before_first_send()
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        fixture.Probe.AfterPending = caller.Cancel;

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => fixture.DispatchAsync(caller.Token));

        Assert.Equal(caller.Token, error.CancellationToken);
        Assert.Equal(caller.Token, Assert.Single(fixture.Probe.QueryTokens));
        Assert.Equal(fixture.Ids, Assert.Single(fixture.Probe.Batches));
        Assert.Empty(fixture.Probe.QueryErrors);
        Assert.Empty(fixture.Probe.Attempts);
        Assert.Empty(fixture.Probe.CompletionAttempts);
        await fixture.AssertRowsAsync(fixture.Ids);
        fixture.AssertDisposedScopes(1);
        fixture.Probe.AfterPending = null;
        await fixture.AssertRecoveryAsync(fixture.Ids);
    }

    [Fact]
    public async Task Cancellation_after_successful_send_reaches_real_completion_and_leaves_message_for_redelivery()
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        fixture.Probe.AfterSend = id =>
        {
            if (id == fixture.Ids[1]) caller.Cancel();
        };

        var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.DispatchAsync(caller.Token));

        Assert.Equal(caller.Token, error.CancellationToken);
        Assert.Same(error, Assert.Single(fixture.Probe.CompletionErrors));
        Assert.Equal(fixture.Ids, Assert.Single(fixture.Probe.Batches));
        Assert.Equal(fixture.Ids[..2], fixture.Probe.Sent);
        Assert.Equal(fixture.Ids[..2], fixture.Probe.CompletionAttempts.Select(x => x.Id));
        Assert.All(fixture.Probe.CompletionAttempts, x => Assert.Equal(caller.Token, x.Token));
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Completed);
        Assert.All(fixture.Probe.Attempts, x => Assert.Equal(caller.Token, x.Token));
        await fixture.AssertRowsAsync(fixture.Ids[1..]);
        fixture.AssertDisposedScopes(1);
        fixture.Probe.AfterSend = null;
        await fixture.AssertRecoveryAsync(fixture.Ids[1..]);
        Assert.Equal(new[] { fixture.Ids[0], fixture.Ids[1], fixture.Ids[1], fixture.Ids[2] }, fixture.Probe.Sent);
    }

    [Fact]
    public async Task Completion_failure_escapes_dispatcher_after_send_without_processing_later_rows()
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        var failure = new InvalidOperationException("completion boundary unavailable");
        fixture.Probe.CompletionFailure = (fixture.Ids[1], failure);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.DispatchAsync(caller.Token));

        Assert.Same(failure, error);
        Assert.Same(failure, Assert.Single(fixture.Probe.CompletionErrors));
        Assert.Equal(fixture.Ids, Assert.Single(fixture.Probe.Batches));
        Assert.Equal(fixture.Ids[..2], fixture.Probe.Sent);
        Assert.Equal(fixture.Ids[..2], fixture.Probe.CompletionAttempts.Select(x => x.Id));
        Assert.All(fixture.Probe.CompletionAttempts, x => Assert.Equal(caller.Token, x.Token));
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Completed);
        Assert.False(caller.IsCancellationRequested);
        await fixture.AssertRowsAsync(fixture.Ids[1..]);
        fixture.AssertDisposedScopes(1);
        fixture.Probe.CompletionFailure = null;
        await fixture.AssertRecoveryAsync(fixture.Ids[1..]);
        Assert.Equal(new[] { fixture.Ids[0], fixture.Ids[1], fixture.Ids[1], fixture.Ids[2] }, fixture.Probe.Sent);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Transport_failure_breaks_batch_including_non_caller_cancellation(bool cancellation)
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        using var unrelated = new CancellationTokenSource();
        unrelated.Cancel();
        Exception failure = cancellation
            ? new OperationCanceledException("transport cancelled independently", unrelated.Token)
            : new InvalidOperationException("transport unavailable");
        fixture.Probe.SendFailure = (fixture.Ids[1], failure);

        Assert.Equal(1, await fixture.DispatchAsync(caller.Token));

        Assert.False(caller.IsCancellationRequested);
        Assert.Same(failure, Assert.Single(fixture.Probe.SendErrors));
        if (cancellation)
            Assert.Equal(unrelated.Token, Assert.IsType<OperationCanceledException>(failure).CancellationToken);
        Assert.Equal(fixture.Ids, Assert.Single(fixture.Probe.Batches));
        Assert.Equal(fixture.Ids[..2], fixture.Probe.Attempts.Select(x => x.Id));
        Assert.All(fixture.Probe.Attempts, x => Assert.Equal(caller.Token, x.Token));
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Sent);
        Assert.Equal(new[] { (fixture.Ids[0], caller.Token) }, fixture.Probe.CompletionAttempts);
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Completed);
        await fixture.AssertRowsAsync(fixture.Ids[1..]);
        fixture.AssertDisposedScopes(1);
        fixture.Probe.SendFailure = null;
        await fixture.AssertRecoveryAsync(fixture.Ids[1..]);
        Assert.Equal(fixture.Ids, fixture.Probe.Sent);
    }

    [Fact]
    public async Task Caller_cancelled_transport_exception_propagates_identically_and_preserves_remaining_batch()
    {
        await using var fixture = await DispatcherFixture.CreateAsync();
        using var caller = new CancellationTokenSource();
        var failure = new OperationCanceledException("caller cancelled during send", caller.Token);
        fixture.Probe.BeforeSend = id =>
        {
            if (id == fixture.Ids[1]) caller.Cancel();
        };
        fixture.Probe.SendFailure = (fixture.Ids[1], failure);

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => fixture.DispatchAsync(caller.Token));

        Assert.Same(failure, error);
        Assert.Equal(caller.Token, error.CancellationToken);
        Assert.Same(error, Assert.Single(fixture.Probe.SendErrors));
        Assert.Equal(fixture.Ids, Assert.Single(fixture.Probe.Batches));
        Assert.Equal(fixture.Ids[..2], fixture.Probe.Attempts.Select(x => x.Id));
        Assert.All(fixture.Probe.Attempts, x => Assert.Equal(caller.Token, x.Token));
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Sent);
        Assert.Equal(new[] { (fixture.Ids[0], caller.Token) }, fixture.Probe.CompletionAttempts);
        Assert.Equal(new[] { fixture.Ids[0] }, fixture.Probe.Completed);
        await fixture.AssertRowsAsync(fixture.Ids[1..]);
        fixture.AssertDisposedScopes(1);
        fixture.Probe.BeforeSend = null;
        fixture.Probe.SendFailure = null;
        await fixture.AssertRecoveryAsync(fixture.Ids[1..]);
    }

    private sealed class BoundaryProbe
    {
        public Action? AfterPending { get; set; }
        public Action<Guid>? BeforeSend { get; set; }
        public Action<Guid>? AfterSend { get; set; }
        public (Guid Id, Exception Error)? SendFailure { get; set; }
        public (Guid Id, Exception Error)? CompletionFailure { get; set; }
        public List<CancellationToken> QueryTokens { get; } = [];
        public List<Exception> QueryErrors { get; } = [];
        public List<Guid[]> Batches { get; } = [];
        public List<(Guid Id, CancellationToken Token)> Attempts { get; } = [];
        public List<Exception> SendErrors { get; } = [];
        public List<Guid> Sent { get; } = [];
        public List<(Guid Id, CancellationToken Token)> CompletionAttempts { get; } = [];
        public List<Exception> CompletionErrors { get; } = [];
        public List<Guid> Completed { get; } = [];
        public List<BoundaryOutbox> Scopes { get; } = [];
    }

    private sealed class BoundaryTransport(BoundaryProbe probe) : ISagaTransport
    {
        public Task PublishAsync<TEvent>(TEvent @event, CancellationToken ct) where TEvent : IEventRequest
        {
            var id = Assert.IsType<EmailInvoice>(@event).InvoiceId;
            probe.Attempts.Add((id, ct));
            probe.BeforeSend?.Invoke(id);
            if (probe.SendFailure is { } failure && failure.Id == id)
            {
                probe.SendErrors.Add(failure.Error);
                throw failure.Error;
            }
            probe.Sent.Add(id);
            probe.AfterSend?.Invoke(id);
            return Task.CompletedTask;
        }
    }

    private sealed class BoundaryOutbox(EfCoreSagaOutbox inner, BoundaryProbe probe) : ISagaOutbox, IDisposable, IAsyncDisposable
    {
        public int SyncDisposals { get; private set; }
        public int AsyncDisposals { get; private set; }

        public void Stage(IReadOnlyList<SagaOutboxMessage> messages) => inner.Stage(messages);
        public void Discard() => inner.Discard();

        public async Task<IReadOnlyList<SagaOutboxMessage>> GetPendingAsync(DateTimeOffset stagedBefore, int maxCount, CancellationToken ct)
        {
            probe.QueryTokens.Add(ct);
            IReadOnlyList<SagaOutboxMessage> pending;
            try
            {
                pending = await inner.GetPendingAsync(stagedBefore, maxCount, ct);
            }
            catch (Exception error)
            {
                probe.QueryErrors.Add(error);
                throw;
            }
            probe.Batches.Add(pending.Select(x => x.Id).ToArray());
            probe.AfterPending?.Invoke();
            return pending;
        }

        public async Task CompleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken ct)
        {
            var id = Assert.Single(ids);
            probe.CompletionAttempts.Add((id, ct));
            Assert.Equal(id, probe.Sent.Last());
            try
            {
                if (probe.CompletionFailure is { } failure && failure.Id == id)
                    throw failure.Error;
                await inner.CompleteAsync(ids, ct);
            }
            catch (Exception error)
            {
                probe.CompletionErrors.Add(error);
                throw;
            }
            probe.Completed.Add(id);
        }

        public void Dispose() => SyncDisposals++;
        public ValueTask DisposeAsync()
        {
            AsyncDisposals++;
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DispatcherFixture(SqliteConnection connection, ServiceProvider provider, BoundaryProbe probe,
        DbContextOptions<OutboxDbContext> contextOptions, Guid[] ids, DateTimeOffset stagedAt) : IAsyncDisposable
    {
        public BoundaryProbe Probe => probe;
        public Guid[] Ids => ids;

        public static async Task<DispatcherFixture> CreateAsync()
        {
            var connection = new SqliteConnection("DataSource=:memory:");
            await connection.OpenAsync();
            var contextOptions = new DbContextOptionsBuilder<OutboxDbContext>().UseSqlite(connection).Options;
            var time = new FakeTimeProvider(new DateTimeOffset(2026, 10, 29, 12, 0, 0, TimeSpan.Zero));
            var stagedAt = time.GetUtcNow() - TimeSpan.FromMinutes(1);
            Guid[] ids = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];
            await using (var context = new OutboxDbContext(contextOptions))
            {
                await context.Database.EnsureCreatedAsync();
                for (var i = 0; i < ids.Length; i++)
                    context.Add(new SagaOutboxRecord
                    {
                        Id = ids[i],
                        EventType = SagaEventTypeNames.GetName(typeof(EmailInvoice)),
                        Payload = JsonSerializer.Serialize(new EmailInvoice(ids[i])),
                        StagedAtUtcTicks = stagedAt.AddSeconds(i).UtcTicks,
                    });
                await context.SaveChangesAsync();
            }
            var probe = new BoundaryProbe();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<TimeProvider>(time);
            services.AddDbContext<OutboxDbContext>(o => o.UseSqlite(connection));
            services.AddSagaFlux(o => o.EnableOutboxPolling = false).UseEfCoreOutbox<OutboxDbContext>();
            services.AddSingleton<ISagaTransport>(new BoundaryTransport(probe));
            services.AddScoped<EfCoreSagaOutbox>();
            services.AddScoped<ISagaOutbox>(sp =>
            {
                var outbox = new BoundaryOutbox(sp.GetRequiredService<EfCoreSagaOutbox>(), probe);
                probe.Scopes.Add(outbox);
                return outbox;
            });
            var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
            return new DispatcherFixture(connection, provider, probe, contextOptions, ids, stagedAt);
        }

        public Task<int> DispatchAsync(CancellationToken ct = default) =>
            provider.GetRequiredService<ISagaOutboxDispatcher>().DispatchPendingAsync(ct);

        public async Task AssertRowsAsync(Guid[] expected)
        {
            await using var context = new OutboxDbContext(contextOptions);
            var rows = await context.Set<SagaOutboxRecord>().AsNoTracking().OrderBy(x => x.StagedAtUtcTicks).ToListAsync();
            Assert.Equal(expected, rows.Select(x => x.Id));
            foreach (var row in rows)
            {
                Assert.Equal(SagaEventTypeNames.GetName(typeof(EmailInvoice)), row.EventType);
                Assert.Equal(JsonSerializer.Serialize(new EmailInvoice(row.Id)), row.Payload);
                Assert.Equal(stagedAt.AddSeconds(Array.IndexOf(ids, row.Id)).UtcTicks, row.StagedAtUtcTicks);
            }
        }

        public void AssertDisposedScopes(int count)
        {
            Assert.Equal(count, probe.Scopes.Count);
            Assert.All(probe.Scopes, scope =>
            {
                Assert.Equal(0, scope.SyncDisposals);
                Assert.Equal(1, scope.AsyncDisposals);
            });
        }

        public async Task AssertRecoveryAsync(Guid[] expected)
        {
            var sentBefore = probe.Sent.Count;
            var completedBefore = probe.Completed.Count;
            Assert.Equal(expected.Length, await DispatchAsync());
            Assert.Equal(expected, probe.Batches.Last());
            Assert.Equal(CancellationToken.None, probe.QueryTokens.Last());
            Assert.Equal(expected, probe.Sent.Skip(sentBefore));
            Assert.Equal(expected, probe.Completed.Skip(completedBefore));
            Assert.All(probe.CompletionAttempts.TakeLast(expected.Length), x => Assert.Equal(CancellationToken.None, x.Token));
            await AssertRowsAsync([]);
            AssertDisposedScopes(2);
            Assert.NotSame(probe.Scopes[0], probe.Scopes[1]);
            Assert.Equal(0, await DispatchAsync());
            Assert.Empty(probe.Batches.Last());
            Assert.Equal(sentBefore + expected.Length, probe.Sent.Count);
            Assert.Equal(completedBefore + expected.Length, probe.Completed.Count);
            AssertDisposedScopes(3);
        }

        public async ValueTask DisposeAsync()
        {
            await provider.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}
