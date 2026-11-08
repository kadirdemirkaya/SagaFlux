using System.Diagnostics.Metrics;

namespace EventFlux.Saga;

internal static class SagaOperationalMetrics
{
    internal const string MeterName = "EventFlux.Saga";
    internal const string DispatchInstrumentName = "sagaflux.dispatcher.dispatch.operations";
    internal const string CompletionInstrumentName = "sagaflux.dispatcher.completion.operations";
    internal const string OperationUnit = "{operation}";
    internal const string KindTag = "kind";
    internal const string OutcomeTag = "outcome";
    internal const string OutboxKind = "outbox";
    internal const string TimeoutKind = "timeout";
    internal const string SuccessOutcome = "success";
    internal const string FailureOutcome = "failure";
    internal const string CancelledOutcome = "cancelled";

    private static readonly Meter Meter = new(MeterName, typeof(SagaOperationalMetrics).Assembly.GetName().Version?.ToString());

    private static readonly Counter<long> Dispatch = Meter.CreateCounter<long>(DispatchInstrumentName, OperationUnit,
        "Dispatch phases entered by the SagaFlux outbox and timeout batch dispatchers, by terminal outcome.");

    private static readonly Counter<long> Completion = Meter.CreateCounter<long>(CompletionInstrumentName, OperationUnit,
        "Store completion calls made by the SagaFlux outbox and timeout batch dispatchers, by terminal outcome.");

    private static readonly KeyValuePair<string, object?> Success = new(OutcomeTag, SuccessOutcome);
    private static readonly KeyValuePair<string, object?> Failure = new(OutcomeTag, FailureOutcome);
    private static readonly KeyValuePair<string, object?> Cancelled = new(OutcomeTag, CancelledOutcome);
    private static readonly KeyValuePair<string, object?> Outbox = new(KindTag, OutboxKind);
    private static readonly KeyValuePair<string, object?> Timeout = new(KindTag, TimeoutKind);

    internal static void OutboxDispatchSucceeded() => Record(Dispatch, Outbox, Success);

    internal static void OutboxDispatchFailed(Exception error, CancellationToken ct) => Record(Dispatch, Outbox, Outcome(error, ct));

    internal static void OutboxCompletionSucceeded() => Record(Completion, Outbox, Success);

    internal static void OutboxCompletionFailed(Exception error, CancellationToken ct) => Record(Completion, Outbox, Outcome(error, ct));

    internal static void TimeoutDispatchSucceeded() => Record(Dispatch, Timeout, Success);

    internal static void TimeoutDispatchFailed(Exception error, CancellationToken ct) => Record(Dispatch, Timeout, Outcome(error, ct));

    internal static void TimeoutCompletionSucceeded() => Record(Completion, Timeout, Success);

    internal static void TimeoutCompletionFailed(Exception error, CancellationToken ct) => Record(Completion, Timeout, Outcome(error, ct));

    private static KeyValuePair<string, object?> Outcome(Exception error, CancellationToken ct)
        => error is OperationCanceledException && ct.IsCancellationRequested ? Cancelled : Failure;

    private static void Record(Counter<long> counter, KeyValuePair<string, object?> kind, KeyValuePair<string, object?> outcome)
    {
        if (!counter.Enabled) return;
        try
        {
            counter.Add(1, kind, outcome);
        }
        catch
        {
        }
    }
}
