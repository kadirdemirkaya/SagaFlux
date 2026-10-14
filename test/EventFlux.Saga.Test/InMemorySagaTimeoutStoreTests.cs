using EventFlux.Saga.Test.Fixtures;
using EventFlux.Saga.Timeouts;

namespace EventFlux.Saga.Test;

public class InMemorySagaTimeoutStoreTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 13, 12, 0, 0, TimeSpan.Zero);
    private readonly InMemorySagaTimeoutStore _store = new();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Pre_cancelled_schedule_preserves_pending_timeouts(bool replacement)
    {
        var pending = await SeedAsync();
        var candidate = CreateTimeout(pending[0].CorrelationId, replacement ? pending[0].Key : "new", Now);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var error = await Assert.ThrowsAsync<OperationCanceledException>(
            () => _store.ScheduleAsync(candidate, cancellation.Token));

        Assert.Equal(cancellation.Token, error.CancellationToken);
        await AssertPendingAsync(pending);
    }

    [Theory]
    [InlineData("cancel")]
    [InlineData("cancel-all")]
    [InlineData("complete")]
    [InlineData("get-due")]
    public async Task Pre_cancelled_operation_preserves_pending_timeouts(string operation)
    {
        var pending = await SeedAsync();
        var target = pending[0];
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        var error = await Assert.ThrowsAsync<OperationCanceledException>(() => operation switch
        {
            "cancel" => _store.CancelAsync(target.CorrelationId, target.Key, cancellation.Token),
            "cancel-all" => _store.CancelAllAsync(target.CorrelationId, cancellation.Token),
            "complete" => _store.CompleteAsync(target.Id, cancellation.Token),
            "get-due" => _store.GetDueAsync(Now, 2, cancellation.Token),
            _ => throw new ArgumentOutOfRangeException(nameof(operation))
        });

        Assert.Equal(cancellation.Token, error.CancellationToken);
        await AssertPendingAsync(pending);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Schedule_validates_null_before_cancellation(bool cancelled)
    {
        var pending = await SeedAsync();
        using var cancellation = new CancellationTokenSource();
        if (cancelled)
            cancellation.Cancel();

        var error = await Assert.ThrowsAsync<ArgumentNullException>(
            () => _store.ScheduleAsync(null!, cancellation.Token));

        Assert.Equal("timeout", error.ParamName);
        await AssertPendingAsync(pending);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Uncancelled_replacement_preserves_other_keys_and_stale_completion_is_no_op(bool explicitToken)
    {
        using var cancellation = new CancellationTokenSource();
        var token = explicitToken ? cancellation.Token : default;
        var pending = await SeedAsync(token);
        var replacement = CreateTimeout(pending[0].CorrelationId, pending[0].Key, Now.AddMinutes(1));

        await _store.ScheduleAsync(replacement, token);
        await AssertPendingAsync([replacement, pending[1], pending[2]]);

        await _store.CompleteAsync(pending[0].Id, token);
        await AssertPendingAsync([replacement, pending[1], pending[2]]);

        await _store.CompleteAsync(replacement.Id, token);
        await AssertPendingAsync([pending[1], pending[2]]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Uncancelled_get_due_returns_earliest_first_up_to_max_count_without_removal(bool explicitToken)
    {
        using var cancellation = new CancellationTokenSource();
        var token = explicitToken ? cancellation.Token : default;
        var id = Guid.NewGuid();
        var earliest = CreateTimeout(id, "earliest", Now.AddMinutes(-2));
        var middle = CreateTimeout(id, "middle", Now.AddMinutes(-1));
        var atNow = CreateTimeout(id, "now", Now);
        var future = CreateTimeout(id, "future", Now.AddMinutes(1));
        foreach (var timeout in new[] { future, atNow, earliest, middle })
            await _store.ScheduleAsync(timeout, token);

        Assert.Equal(new[] { earliest, middle }, await _store.GetDueAsync(Now, 2, token));
        Assert.Equal(new[] { earliest, middle, atNow }, await _store.GetDueAsync(Now, 10, token));
        Assert.Empty(await _store.GetDueAsync(Now, 0, token));
        await AssertPendingAsync([earliest, middle, atNow, future]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Uncancelled_cancel_operations_preserve_other_correlations(bool explicitToken)
    {
        using var cancellation = new CancellationTokenSource();
        var token = explicitToken ? cancellation.Token : default;
        var pending = await SeedAsync(token);

        await _store.CancelAsync(pending[0].CorrelationId, pending[0].Key, token);
        await AssertPendingAsync([pending[1], pending[2]]);

        await _store.CancelAsync(pending[0].CorrelationId, pending[0].Key, token);
        await _store.CompleteAsync(pending[0].Id, token);
        await AssertPendingAsync([pending[1], pending[2]]);

        await _store.CancelAllAsync(pending[0].CorrelationId, token);
        await AssertPendingAsync([pending[2]]);

        await _store.CancelAllAsync(pending[0].CorrelationId, token);
        await AssertPendingAsync([pending[2]]);
    }

    private async Task<SagaTimeout[]> SeedAsync(CancellationToken token = default)
    {
        var id = Guid.NewGuid();
        SagaTimeout[] pending =
        [
            CreateTimeout(id, "first", Now.AddMinutes(-2)),
            CreateTimeout(id, "second", Now.AddMinutes(-1)),
            CreateTimeout(Guid.NewGuid(), "first", Now.AddMinutes(1))
        ];
        foreach (var timeout in pending)
            await _store.ScheduleAsync(timeout, token);
        return pending;
    }

    private async Task AssertPendingAsync(SagaTimeout[] expected)
    {
        Assert.Equal(expected.Length, _store.Count);
        var actual = await _store.GetDueAsync(DateTimeOffset.MaxValue, int.MaxValue, default);
        Assert.Equal(expected.Length, actual.Count);
        var ordered = expected.OrderBy(t => t.DueAt).ToArray();
        for (var i = 0; i < ordered.Length; i++)
        {
            Assert.Equal(ordered[i].Id, actual[i].Id);
            Assert.Equal(ordered[i].CorrelationId, actual[i].CorrelationId);
            Assert.Equal(ordered[i].Key, actual[i].Key);
            Assert.Equal(ordered[i].DueAt, actual[i].DueAt);
            Assert.Same(ordered[i].Event, actual[i].Event);
        }
        Assert.Equal(expected.Length, _store.Count);
    }

    private static SagaTimeout CreateTimeout(Guid correlationId, string key, DateTimeOffset dueAt)
        => new(Guid.NewGuid(), correlationId, key, dueAt, new ReservationExpired(correlationId));
}
