using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit.Abstractions;

namespace EventFlux.Saga.Generators.Test;

public class FileLocalDriverTests(ITestOutputHelper output)
{
    private (Compilation Compilation, ImmutableArray<Diagnostic> Diagnostics, GeneratorDriverRunResult Run) Run(string source)
    {
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Append(MetadataReference.CreateFromFile(typeof(SagaStateSetAttribute).Assembly.Location));
        var compilation = CSharpCompilation.Create("FileLocalProbe",
            new[] { CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.CSharp12), path: "FileLocalProbe.cs") },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        var inputErrors = compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        foreach (var diagnostic in inputErrors)
            output.WriteLine("Input: " + diagnostic);
        Assert.Empty(inputErrors);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new[] { new SagaStateSetGenerator().AsSourceGenerator() },
            parseOptions: (CSharpParseOptions)compilation.SyntaxTrees[0].Options);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var result, out var diagnostics);
        foreach (var diagnostic in diagnostics.Concat(result.GetDiagnostics()))
            output.WriteLine(diagnostic.ToString());
        return (result, diagnostics, driver.GetRunResult());
    }

    [Theory]
    [InlineData("file class BadHost { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }")]
    [InlineData("file class BadHost { public class Inner { [EventFlux.Saga.SagaStateSet] internal enum Steps { A, B } } }")]
    public void File_local_container_does_not_suppress_an_accessible_same_named_sibling(string rejected)
    {
        var result = Run("namespace Shop; " + rejected
            + " public class GoodHost { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }");

        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Equal("SAGA005", Assert.Single(result.Diagnostics).Id);
        var generated = Assert.Single(result.Run.GeneratedTrees).ToString();
        Assert.Contains("global::Shop.GoodHost.Steps", generated);
        Assert.DoesNotContain("BadHost", generated);
        var consumer = CSharpSyntaxTree.ParseText("""
            internal static class Consumer
            {
                internal static EventFlux.Saga.SagaState Pick() => Shop.StepsSet.From(Shop.GoodHost.Steps.B);
                internal static bool Parse() => Shop.StepsSet.TryParse("B", out _);
            }
            """, (CSharpParseOptions)result.Compilation.SyntaxTrees.First().Options);
        Assert.Empty(result.Compilation.AddSyntaxTrees(consumer).GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
    }

    [Theory]
    [InlineData("[EventFlux.Saga.SagaStateSet] file enum Steps { A, B }")]
    [InlineData("namespace Shop; [EventFlux.Saga.SagaStateSet] file enum Steps { A, B }")]
    [InlineData("file class Host { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } }")]
    [InlineData("namespace Shop; file struct Host { [EventFlux.Saga.SagaStateSet] internal enum Steps { A, B } }")]
    [InlineData("file class Host { public class Inner { [EventFlux.Saga.SagaStateSet] public enum Steps { A, B } } }")]
    public void File_local_enum_or_container_reports_accessibility_error_without_generated_errors(string source)
    {
        var result = Run(source);

        Assert.All(result.Run.Results, r => Assert.Null(r.Exception));
        Assert.Empty(result.Compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        var diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("SAGA005", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Error, diagnostic.Severity);
        Assert.Contains("Steps", diagnostic.GetMessage());
        Assert.Equal("FileLocalProbe.cs", diagnostic.Location.GetLineSpan().Path);
        Assert.True(diagnostic.Location.SourceSpan.Length > 0);
        Assert.Empty(result.Run.GeneratedTrees);
    }
}
