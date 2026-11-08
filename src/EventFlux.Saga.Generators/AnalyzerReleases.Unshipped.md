; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SAGA001 | SagaFlux | Warning | SagaStateSetGenerator
SAGA002 | SagaFlux | Info | SagaStateSetGenerator
SAGA003 | SagaFlux | Error | SagaStateSetGenerator
SAGA004 | SagaFlux | Error | SagaStateSetGenerator: aliased numeric values
SAGA005 | SagaFlux | Error | SagaStateSetGenerator: inaccessible enum or containing type, including file-local types
SAGA006 | SagaFlux | Error | SagaStateSetGenerator: generic containing type
SAGA007 | SagaFlux | Error | SagaStateSetGenerator: colliding generated set names
