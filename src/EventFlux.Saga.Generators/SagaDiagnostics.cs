using Microsoft.CodeAnalysis;

namespace EventFlux.Saga.Generators;

internal static class SagaDiagnostics
{
    private const string Category = "SagaFlux";

    public static readonly DiagnosticDescriptor StateRemoved = new(
        "SAGA001",
        "Saga state removed",
        "State '{0}' was removed from '{1}' but is listed in {1}.sagastates; saga instances persisted in this state may need a data migration before this deployment",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A saga state that existed in a previous deployment is gone. Instances stored in that state can no longer transition. Migrate or clean up the data, then remove the name from the .sagastates snapshot.");

    public static readonly DiagnosticDescriptor StateNotInSnapshot = new(
        "SAGA002",
        "Saga state not in snapshot",
        "State '{0}' of '{1}' is not listed in {1}.sagastates; add it once it is deployed so that its removal is detected later",
        Category,
        DiagnosticSeverity.Info,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ReservedName = new(
        "SAGA003",
        "Reserved saga state name",
        "State '{0}' of '{1}' collides with a member of the generated {1}Set class; rename it",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}
