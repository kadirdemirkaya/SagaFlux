using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.Timeouts;

internal sealed class SagaTimeoutHostedService(
    ISagaTimeoutDispatcher dispatcher,
    TimeProvider time,
    SagaFluxOptions options,
    ILogger<SagaTimeoutHostedService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.EnableTimeoutPolling)
            return;

        using var timer = new PeriodicTimer(options.TimeoutPollingInterval, time);
        try
        {
            do
            {
                try
                {
                    int delivered;
                    do
                    {
                        delivered = await dispatcher.DispatchDueAsync(stoppingToken).ConfigureAwait(false);
                    }
                    while (delivered >= options.TimeoutBatchSize && !stoppingToken.IsCancellationRequested);
                }
                catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(ex, "Saga timeout polling failed.");
                }
            }
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }
}
