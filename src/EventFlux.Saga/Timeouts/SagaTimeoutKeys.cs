namespace EventFlux.Saga;

/// <summary>
/// Default timeout keys.
/// </summary>
public static class SagaTimeoutKeys
{
    /// <summary>
    /// Gets the default key of a timeout event type: its full name.
    /// </summary>
    /// <typeparam name="TTimeout">The timeout event type.</typeparam>
    /// <returns>The key.</returns>
    public static string For<TTimeout>() => For(typeof(TTimeout));

    /// <summary>
    /// Gets the default key of a timeout event type: its full name.
    /// </summary>
    /// <param name="timeoutType">The timeout event type.</param>
    /// <returns>The key.</returns>
    public static string For(Type timeoutType)
    {
        ArgumentNullException.ThrowIfNull(timeoutType);
        return timeoutType.FullName ?? timeoutType.Name;
    }
}
