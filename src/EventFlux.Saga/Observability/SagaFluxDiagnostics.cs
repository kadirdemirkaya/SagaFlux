using System.Diagnostics;

namespace EventFlux.Saga;

/// <summary>
/// The <see cref="System.Diagnostics.ActivitySource"/> SagaFlux reports saga work to, so any tracing pipeline that
/// listens to it (for example OpenTelemetry with <c>AddSource(SagaFluxDiagnostics.ActivitySourceName)</c>) sees saga
/// transitions next to the rest of the application's traces. Nothing is recorded when no listener is attached.
/// </summary>
/// <remarks>
/// One activity (<see cref="ActivityKind.Internal"/>) covers an event applied to one saga instance, named
/// <c>"{Saga} {Event}"</c>. Its tags are listed as the <c>Tag*</c> constants. A failure sets the activity status to
/// <see cref="ActivityStatusCode.Error"/>.
/// </remarks>
public static class SagaFluxDiagnostics
{
    /// <summary>The name of the activity source: <c>EventFlux.Saga</c>.</summary>
    public const string ActivitySourceName = "EventFlux.Saga";

    /// <summary>Tag: the saga type name.</summary>
    public const string TagSaga = "sagaflux.saga";

    /// <summary>Tag: the event type name.</summary>
    public const string TagEvent = "sagaflux.event";

    /// <summary>Tag: the instance correlation id.</summary>
    public const string TagCorrelationId = "sagaflux.correlation_id";

    /// <summary>Tag: whether a transition handled the event.</summary>
    public const string TagHandled = "sagaflux.handled";

    /// <summary>Tag: the state before the transition.</summary>
    public const string TagFromState = "sagaflux.from_state";

    /// <summary>Tag: the state after the transition.</summary>
    public const string TagToState = "sagaflux.to_state";

    /// <summary>Tag: the saved instance version.</summary>
    public const string TagVersion = "sagaflux.version";

    /// <summary>Tag: how many times the event was applied because of concurrent changes (1 when none).</summary>
    public const string TagAttempts = "sagaflux.attempts";

    /// <summary>Tag: the exception type an <c>OnFault</c> handler handled.</summary>
    public const string TagFault = "sagaflux.fault";

    /// <summary>Tag: whether the instance was removed on reaching Final.</summary>
    public const string TagRemoved = "sagaflux.removed";

    internal static readonly ActivitySource Source = new(ActivitySourceName, typeof(SagaFluxDiagnostics).Assembly.GetName().Version?.ToString());
}
