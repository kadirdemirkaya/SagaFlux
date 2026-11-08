using System.Diagnostics.Metrics;

namespace EventFlux.Saga.EfCore;

internal static class SagaStoreMetrics
{
    internal const string MeterName = "EventFlux.Saga.EfCore";
    internal const string UnresolvedRowInstrumentName = "sagaflux.store.unresolved_row.observations";

    private static readonly Meter Meter = new(MeterName, typeof(SagaStoreMetrics).Assembly.GetName().Version?.ToString());

    private static readonly Counter<long> UnresolvedRows = Meter.CreateCounter<long>(UnresolvedRowInstrumentName, "{observation}",
        "Unresolvable or unusable stored-event row encounters skipped while the EF Core outbox and timeout stores read; a retained row counts again on every read.");

    private static readonly KeyValuePair<string, object?> Outbox = new("kind", "outbox");
    private static readonly KeyValuePair<string, object?> Timeout = new("kind", "timeout");

    internal static void OutboxRowSkipped() => Record(Outbox);

    internal static void TimeoutRowSkipped() => Record(Timeout);

    private static void Record(KeyValuePair<string, object?> kind)
    {
        if (!UnresolvedRows.Enabled) return;
        try
        {
            UnresolvedRows.Add(1, kind);
        }
        catch
        {
        }
    }
}
