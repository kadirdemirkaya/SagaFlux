using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventFlux.Saga.Outbox;

internal sealed class SagaOutboxDispatcher(
    IServiceScopeFactory scopeFactory,
    TimeProvider time,
    SagaFluxOptions options,
    ILogger<SagaOutboxDispatcher> logger) : ISagaOutboxDispatcher
{
    public async Task<int> DispatchPendingAsync(CancellationToken ct = default)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var outbox = scope.ServiceProvider.GetRequiredService<ISagaOutbox>();
        var transport = scope.ServiceProvider.GetRequiredService<ISagaTransport>();
        var pending = await outbox.GetPendingAsync(time.GetUtcNow() - options.OutboxDispatchDelay, options.OutboxBatchSize, ct).ConfigureAwait(false);

        var sent = 0;
        foreach (var message in pending)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await SagaPublishedEvent.FromEvent(message.Event).SendAsync(transport, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                logger.LogError(ex, "Saga outbox message {Id} ({Event}) could not be sent; it stays pending.",
                    message.Id, message.Event.GetType().Name);
                break;
            }

            await outbox.CompleteAsync([message.Id], ct).ConfigureAwait(false);
            sent++;
        }

        return sent;
    }
}
