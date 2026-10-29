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

    public static readonly DiagnosticDescriptor GeneratedSetCollision = new(
        "SAGA007",
        "Generated saga state set collision",
        "Enum '{0}' would generate '{1}' in the same namespace as another saga state set; rename an enum or move it to another namespace",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor InaccessibleEnum = new(
        "SAGA005",
        "Inaccessible saga state enum",
        "Enum '{0}' or a containing type is inaccessible from its namespace-level generated set; use public, internal or protected internal accessibility",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor GenericContainer = new(
        "SAGA006",
        "Generic saga state enum container",
        "Enum '{0}' is nested in a generic type; move it to a non-generic type or namespace for state generation",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor AliasedValues = new(
        "SAGA004",
        "Aliased saga state values",
        "Enum '{0}' contains members with the same numeric value; assign distinct values so From can identify each state",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public static readonly DiagnosticDescriptor ReservedName = new(
        "SAGA003",
        "Reserved saga state name",
        "State '{0}' of '{1}' collides with a member of the generated {1}Set class; rename it",
        Category,
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);
}
