namespace EventFlux.Saga.Internal;

internal sealed class SagaEventCatalog : ISagaEventCatalog
{
    private readonly HashSet<Type> _eventTypes = new();

    public IReadOnlyCollection<Type> EventTypes => _eventTypes;

    public void Add(Type eventType) => _eventTypes.Add(eventType);
}
