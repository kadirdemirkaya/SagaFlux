namespace EventFlux.Saga;

/// <summary>
/// Thrown when a saga cannot run because of how it is set up (for example an <c>Ask</c> step without a requester).
/// It is never handled by <c>OnFault&lt;TException&gt;(...)</c>, so a setup mistake cannot be mistaken for a
/// business fault.
/// </summary>
/// <param name="message">The error message.</param>
public sealed class SagaConfigurationException(string message) : Exception(message);
