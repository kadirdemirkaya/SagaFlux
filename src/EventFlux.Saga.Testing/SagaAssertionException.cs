namespace EventFlux.Saga.Testing;

/// <summary>
/// Thrown when a <see cref="SagaAssertions{TState}"/> assertion fails. Any test framework reports it as a failure.
/// </summary>
/// <param name="message">The failure description.</param>
public sealed class SagaAssertionException(string message) : Exception(message);
