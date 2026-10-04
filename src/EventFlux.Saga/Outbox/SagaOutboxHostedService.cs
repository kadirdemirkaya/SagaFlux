using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.Outbox;

internal sealed class SagaOutboxHostedService(
    ISagaOutboxDispatcher dispatcher,
    TimeProvider time,
    SagaFluxOptions options,
    ILogger<SagaOutboxHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.EnableOutboxPolling)
            return;

        using var timer = new PeriodicTimer(options.OutboxPollingInterval, time);
        try
        {
            do
            {
                try
                {
                    int sent;
                    do
                    {
                        sent = await dispatcher.DispatchPendingAsync(stoppingToken).ConfigureAwait(false);
                    }
                    while (sent >= options.OutboxBatchSize && !stoppingToken.IsCancellationRequested);
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Saga outbox polling failed.");
                }
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
