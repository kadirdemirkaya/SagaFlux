namespace EventFlux.Saga.Internal;

internal static class SagaPipelineScope
{
    private static readonly AsyncLocal<object?> _current = new();

    public static bool IsRouting(object @event) => ReferenceEquals(_current.Value, @event);

    public static async Task RunAsync(object @event, Func<Task> next)
    {
        var previous = _current.Value;
        _current.Value = @event;
        try
        {
            await next().ConfigureAwait(false);
        }
        finally
        {
            _current.Value = previous;
        }
    }
}
