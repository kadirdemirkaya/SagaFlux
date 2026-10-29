using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit.Abstractions;

namespace EventFlux.Saga.Generators.Test;

public class EnumBoundaryDriverTests(ITestOutputHelper output)
{
    private (Compilation Compilation, ImmutableArray<Diagnostic> Diagnostics, GeneratorDriverRunResult Run) Run(string source)
    {
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Append(MetadataReference.CreateFromFile(typeof(SagaStateSetAttribute).Assembly.Location));
        var compilation = CSharpCompilation.Create("BoundaryProbe",
            new[] { CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest), path: "BoundaryProbe.cs") },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new[] { new SagaStateSetGenerator().AsSourceGenerator() },
            parseOptions: (CSharpParseOptions)compilation.SyntaxTrees[0].Options);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var result, out var diagnostics);
        foreach (var diagnostic in diagnostics.Concat(result.GetDiagnostics()))
            output.WriteLine(diagnostic.ToString());
        return (result, diagnostics, driver.GetRunResult());
    }

    private static void AssertRejected(
        (Compilation Compilation, ImmutableArray<Diagnostic> Diagnostics, GeneratorDriverRunResult Run) result,
        string id, int count = 1)
    {
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.Equal(count, result.Diagnostics.Length);
        Assert.All(result.Diagnostics, d =>
        {
            Assert.Equal(id, d.Id);
            Assert.Equal(DiagnosticSeverity.Error, d.Severity);
            Assert.Equal("BoundaryProbe.cs", d.Location.GetLineSpan().Path);
            Assert.True(d.Location.SourceSpan.Length > 0);
        });
        Assert.Empty(result.Run.GeneratedTrees);
    }

    [Fact]
    public void Large_unsigned_values_use_the_existing_declaration_index_ordinal_fallback()
    {
        var result = Run("[EventFlux.Saga.SagaStateSet] public enum Steps : ulong { A = ulong.MaxValue - 1, B = ulong.MaxValue }");

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        var generated = Assert.Single(result.Run.GeneratedTrees).ToString();
        Assert.Contains("SagaStates.Define(\"A\", 0)", generated);
        Assert.Contains("SagaStates.Define(\"B\", 1)", generated);
    }

    [Fact]
    public void Unsigned_alias_values_report_a_diagnostic_without_overflow()
    {
        AssertRejected(Run("[EventFlux.Saga.SagaStateSet] public enum Steps : ulong { A = ulong.MaxValue, B = A }"), "SAGA004");
    }

    [Theory]
    [InlineData("public class BadHost { [EventFlux.Saga.SagaStateSet] private enum Steps { A, B } }", "SAGA005")]
    [InlineData("public class BadHost<T> { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }", "SAGA006")]
    [InlineData("public class BadHost { [EventFlux.Saga.SagaStateSet] public enum Steps { A = 1, B = 1 } }", "SAGA004")]
    [InlineData("public class BadHost { [EventFlux.Saga.SagaStateSet] public enum Steps { A, All } }", "SAGA003")]
    public void Rejected_enum_does_not_suppress_an_accessible_same_named_sibling(string rejected, string id)
    {
        var result = Run(rejected + " public class GoodHost { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }");

        Assert.Equal(id, Assert.Single(result.Diagnostics).Id);
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Contains("global::GoodHost.Steps", Assert.Single(result.Run.GeneratedTrees).ToString());
    }

    [Fact]
    public void Same_named_nested_enums_report_each_collision_without_duplicate_sets()
    {
        var result = Run("""
            namespace Shop;
            public class First { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }
            public class Second { [EventFlux.Saga.SagaStateSet] public enum Steps { C, D } }
            """);

        AssertRejected(result, "SAGA007", 2);
        Assert.All(result.Diagnostics, d => Assert.Contains("StepsSet", d.GetMessage()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("namespace Shop;")]
    public void Top_level_public_and_internal_sets_still_compile(string namespaceDeclaration)
    {
        var result = Run($$"""
            {{namespaceDeclaration}}
            [EventFlux.Saga.SagaStateSet] public enum Steps { Initial, Running = 10, Final }
            [EventFlux.Saga.SagaStateSet] internal enum HiddenSteps { A, B }
            internal static class Probe
            {
                internal static EventFlux.Saga.SagaState Pick() => StepsSet.From(Steps.Running);
                internal static EventFlux.Saga.SagaState PickHidden() => HiddenStepsSet.From(HiddenSteps.B);
            }
            """);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Equal(2, result.Run.GeneratedTrees.Length);
    }

    [Fact]
    public void Same_named_nested_enums_in_different_namespaces_still_compile()
    {
        var result = Run("""
            namespace Shop { public class Host { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } } }
            namespace Shipping { public class Host { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } } }
            internal static class Probe
            {
                internal static EventFlux.Saga.SagaState Pick() => Shop.StepsSet.From(Shop.Host.Steps.A);
                internal static EventFlux.Saga.SagaState PickOther() => Shipping.StepsSet.From(Shipping.Host.Steps.B);
            }
            """);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Equal(2, result.Run.GeneratedTrees.Length);
    }

    [Theory]
    [InlineData("private")]
    [InlineData("protected")]
    [InlineData("private protected")]
    public void Inaccessible_nested_enums_report_a_diagnostic(string access)
    {
        AssertRejected(Run($"public class Host {{ [EventFlux.Saga.SagaStateSet] {access} enum Steps {{ A, B }} }}"), "SAGA005");
    }

    [Theory]
    [InlineData("private")]
    [InlineData("protected")]
    [InlineData("private protected")]
    public void Inaccessible_containing_types_report_a_diagnostic(string access)
    {
        AssertRejected(Run($"public class Host {{ {access} class Hidden {{ [EventFlux.Saga.SagaStateSet] public enum Steps {{ A, B }} }} }}"), "SAGA005");
    }

    [Theory]
    [InlineData("public class Host<T> { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }")]
    [InlineData("public class Host<T> { public class Inner { [EventFlux.Saga.SagaStateSet] internal enum Steps { A, B } } }")]
    public void Generic_containing_types_report_a_diagnostic(string source)
    {
        AssertRejected(Run(source), "SAGA006");
    }

    [Theory]
    [InlineData("public", "public", "public")]
    [InlineData("public", "internal", "internal")]
    [InlineData("internal", "public", "internal")]
    [InlineData("public", "protected internal", "internal")]
    public void Accessible_nested_enums_keep_their_namespace_level_set(string hostAccess, string enumAccess, string setAccess)
    {
        var result = Run($$"""
            namespace Shop;
            {{hostAccess}} class Host { [EventFlux.Saga.SagaStateSet] {{enumAccess}} enum Steps { Initial, Running = 10, Final } }
            internal static class Probe
            {
                internal static EventFlux.Saga.SagaState Pick() => StepsSet.From(Host.Steps.Running);
                internal static bool Parse() => StepsSet.TryParse("Running", out _);
            }
            """);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Contains($"{setAccess} static partial class StepsSet", Assert.Single(result.Run.GeneratedTrees).ToString());
    }

    [Theory]
    [InlineData("A = 1, B = A")]
    [InlineData("Initial = 0, Running = 1, Alias = 1, Final = 2")]
    [InlineData("A = -1, B = -1")]
    public void Aliased_values_report_a_diagnostic_without_invalid_switch_arms(string members)
    {
        var result = Run($"[EventFlux.Saga.SagaStateSet] public enum Steps {{ {members} }}");

        AssertRejected(result, "SAGA004");
        Assert.Contains("Steps", Assert.Single(result.Diagnostics).GetMessage());
    }
}
