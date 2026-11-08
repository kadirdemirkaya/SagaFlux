using System.Globalization;
using System.Reflection;
using System.Runtime.Loader;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit.Abstractions;

namespace EventFlux.Saga.Generators.Test;

public class CultureOrdinalDriverTests(ITestOutputHelper output)
{
    [Theory]
    [InlineData("int", "int.MinValue, Negative = -7, Zero = 0, Positive = 42, Maximum = int.MaxValue", new int[] { int.MinValue, -7, 0, 42, int.MaxValue })]
    [InlineData("long", "int.MinValue, Negative = -7L, Zero = 0L, Positive = 42L, Maximum = int.MaxValue", new int[] { int.MinValue, -7, 0, 42, int.MaxValue })]
    [InlineData("long", "long.MinValue, Negative = -7L, Zero = 0L, Positive = 42L, Maximum = long.MaxValue", new int[] { 0, -7, 0, 42, 4 })]
    [InlineData("ulong", "ulong.MaxValue - 1, Negative = ulong.MaxValue, Zero = 0, Positive = 42, Maximum = int.MaxValue", new int[] { 0, 1, 0, 42, int.MaxValue })]
    public void Generated_consumer_preserves_ordinals_with_a_non_ascii_negative_sign(string underlyingType, string members, int[] ordinals)
    {
        var originalCulture = CultureInfo.CurrentCulture;
        var originalUiCulture = CultureInfo.CurrentUICulture;
        var culture = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        culture.NumberFormat.NegativeSign = "\u2212";
        try
        {
            CultureInfo.CurrentCulture = culture;
            CultureInfo.CurrentUICulture = culture;
            Assert.Equal("\u22127", (-7).ToString());
            RunConsumer(underlyingType, members, ordinals);
        }
        finally
        {
            CultureInfo.CurrentCulture = originalCulture;
            CultureInfo.CurrentUICulture = originalUiCulture;
        }
        Assert.Same(originalCulture, CultureInfo.CurrentCulture);
        Assert.Same(originalUiCulture, CultureInfo.CurrentUICulture);
    }

    private void RunConsumer(string underlyingType, string members, int[] ordinals)
    {
        var options = new CSharpParseOptions(LanguageVersion.CSharp12);
        var source = $$"""
            [EventFlux.Saga.SagaStateSet]
            public enum Steps : {{underlyingType}} { Minimum = {{members}} }
            """;
        var consumer = """
            public static class Consumer
            {
                public static EventFlux.Saga.SagaState[] Read() => new[]
                {
                    StepsSet.From(Steps.Minimum), StepsSet.From(Steps.Negative),
                    StepsSet.From(Steps.Zero), StepsSet.From(Steps.Positive), StepsSet.From(Steps.Maximum)
                };
                public static bool Parse(string name, out EventFlux.Saga.SagaState state) => StepsSet.TryParse(name, out state);
            }
            """;
        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => a.Location)
            .Append(typeof(SagaStateSetAttribute).Assembly.Location)
            .Distinct()
            .Select(path => MetadataReference.CreateFromFile(path));
        var compilation = CSharpCompilation.Create("CultureProbe_" + Guid.NewGuid().ToString("N"),
            new[] { CSharpSyntaxTree.ParseText(source, options, "States.cs") }, references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        Assert.Empty(compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            new[] { new SagaStateSetGenerator().AsSourceGenerator() }, parseOptions: options);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var generatedCompilation, out var diagnostics);
        Assert.Empty(diagnostics);
        var run = driver.GetRunResult();
        Assert.All(run.Results, result => Assert.Null(result.Exception));
        var generated = Assert.Single(run.GeneratedTrees).ToString();
        var result = generatedCompilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText(consumer, options, "Consumer.cs"));
        foreach (var diagnostic in result.GetDiagnostics())
            output.WriteLine(diagnostic.ToString());
        Assert.Empty(result.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error));
        Assert.DoesNotContain("\u2212", generated);
        var names = new[] { "Minimum", "Negative", "Zero", "Positive", "Maximum" };
        for (var i = 0; i < names.Length; i++)
            Assert.Contains($"SagaStates.Define(\"{names[i]}\", {ordinals[i].ToString(CultureInfo.InvariantCulture)})", generated);
        using var stream = new MemoryStream();
        var emit = result.Emit(stream);
        Assert.True(emit.Success, string.Join(Environment.NewLine, emit.Diagnostics));
        stream.Position = 0;
        var loadContext = new AssemblyLoadContext("CultureProbe", isCollectible: true);
        try
        {
            var assembly = loadContext.LoadFromStream(stream);
            var type = assembly.GetType("Consumer", throwOnError: true)!;
            var states = Assert.IsType<SagaState[]>(type.GetMethod("Read")!.Invoke(null, null));
            Assert.Equal(names, states.Select(state => state.Name));
            Assert.Equal(ordinals, states.Select(state => state.Ordinal));
            var parse = type.GetMethod("Parse")!;
            for (var i = 0; i < names.Length; i++)
            {
                object?[] arguments = { names[i], null };
                Assert.True(Assert.IsType<bool>(parse.Invoke(null, arguments)));
                Assert.Equal(states[i], Assert.IsType<SagaState>(arguments[1]));
            }
        }
        finally
        {
            loadContext.Unload();
        }
    }
}
