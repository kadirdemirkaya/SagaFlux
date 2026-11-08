using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

namespace EventFlux.Saga.Generators.Test;

public class GeneratorDriverTests
{
    private const string Source = """
        namespace Shop.Orders;

        [EventFlux.Saga.SagaStateSet]
        public enum OrderStates { Initial, AwaitingPayment = 10, Shipped = 20, Final }

        public static class Probe
        {
            public static EventFlux.Saga.SagaState Pick() => OrderStatesSet.AwaitingPayment;
        }
        """;

    private sealed class InMemoryText(string path, string content) : AdditionalText
    {
        public override string Path { get; } = path;

        public override SourceText GetText(CancellationToken cancellationToken = default) => SourceText.From(content);
    }

    private static (Compilation Output, ImmutableArray<Diagnostic> Diagnostics, GeneratorDriverRunResult Run) Run(string source, params AdditionalText[] files)
    {
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Append(MetadataReference.CreateFromFile(typeof(SagaStateSetAttribute).Assembly.Location));
        var compilation = CSharpCompilation.Create("Probe",
            new[] { CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest)) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new[] { new SagaStateSetGenerator().AsSourceGenerator() },
            files,
            (CSharpParseOptions)compilation.SyntaxTrees[0].Options);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var output, out var diagnostics);
        return (output, diagnostics, driver.GetRunResult());
    }

    [Fact]
    public void Generates_compilable_reflection_free_states()
    {
        var (output, diagnostics, run) = Run(Source);

        Assert.Empty(diagnostics);
        Assert.Empty(output.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        var generated = Assert.Single(run.GeneratedTrees).ToString();
        Assert.Contains("public static partial class OrderStatesSet", generated);
        Assert.Contains("SagaStates.Define(\"AwaitingPayment\", 10)", generated);
        Assert.Contains("AwaitingPayment = global::EventFlux.Saga.SagaStates.Define", generated);
        Assert.Contains("Initial = global::EventFlux.Saga.SagaStates.Initial", generated);
        Assert.DoesNotContain("typeof", generated);
        Assert.DoesNotContain("GetType", generated);
    }

    [Fact]
    public void Generated_code_attribute_carries_the_generator_assembly_version()
    {
        var (_, _, run) = Run(Source);

        var version = typeof(SagaStateSetGenerator).Assembly.GetName().Version!.ToString(3);
        var generated = Assert.Single(run.GeneratedTrees).ToString();
        Assert.Equal(3, version.Split('.').Length);
        Assert.Equal(typeof(SagaState).Assembly.GetName().Version!.ToString(3), version);
        Assert.Contains($"[global::System.CodeDom.Compiler.GeneratedCode(\"EventFlux.Saga.Generators\", \"{version}\")]", generated);
        Assert.Single(generated.Split('\n'), line => line.Contains("GeneratedCode("));
    }

    [Fact]
    public void Internal_enum_gets_internal_class()
    {
        var (_, _, run) = Run("[EventFlux.Saga.SagaStateSet] internal enum Steps { Initial, Running }");

        Assert.Contains("internal static partial class StepsSet", Assert.Single(run.GeneratedTrees).ToString());
    }

    [Fact]
    public void Removed_state_listed_in_snapshot_warns()
    {
        var (_, diagnostics, _) = Run(Source, new InMemoryText("/repo/OrderStates.sagastates", "Initial\nAwaitingPayment\nPaymentFailed\nShipped\nFinal\n"));

        var removed = Assert.Single(diagnostics, d => d.Id == "SAGA001");
        Assert.Equal(DiagnosticSeverity.Warning, removed.Severity);
        Assert.Contains("'PaymentFailed'", removed.GetMessage());
        Assert.DoesNotContain(diagnostics, d => d.Id == "SAGA002");
    }

    [Fact]
    public void New_state_missing_from_snapshot_is_reported_as_info()
    {
        var (_, diagnostics, _) = Run(Source, new InMemoryText(@"C:\repo\OrderStates.sagastates", "# deployed states\nInitial\nAwaitingPayment\nFinal\n"));

        var added = Assert.Single(diagnostics, d => d.Id == "SAGA002");
        Assert.Equal(DiagnosticSeverity.Info, added.Severity);
        Assert.Contains("'Shipped'", added.GetMessage());
        Assert.DoesNotContain(diagnostics, d => d.Id == "SAGA001");
    }

    [Fact]
    public void Snapshot_of_another_enum_is_ignored()
    {
        var (_, diagnostics, _) = Run(Source, new InMemoryText("/repo/OtherStates.sagastates", "Gone\n"));

        Assert.Empty(diagnostics);
    }

    [Fact]
    public void Reserved_member_name_is_an_error_and_generates_nothing()
    {
        var (_, diagnostics, run) = Run("[EventFlux.Saga.SagaStateSet] public enum Bad { Initial, All }");

        Assert.Equal(DiagnosticSeverity.Error, Assert.Single(diagnostics, d => d.Id == "SAGA003").Severity);
        Assert.Empty(run.GeneratedTrees);
    }

    [Fact]
    public void Enums_without_the_attribute_are_ignored()
    {
        var (_, diagnostics, run) = Run("public enum Plain { A, B }");

        Assert.Empty(diagnostics);
        Assert.Empty(run.GeneratedTrees);
    }
}
