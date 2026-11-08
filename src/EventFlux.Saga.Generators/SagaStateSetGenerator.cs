using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace EventFlux.Saga.Generators;

/// <summary>
/// Generates reflection-free <c>SagaState</c> definitions from enums marked with <c>[SagaStateSet]</c>, and reports
/// removed states against a <c>{EnumName}.sagastates</c> snapshot.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class SagaStateSetGenerator : IIncrementalGenerator
{
    private const string AttributeName = "EventFlux.Saga.SagaStateSetAttribute";
    private const string State = "global::EventFlux.Saga.SagaState";
    private const string States = "global::EventFlux.Saga.SagaStates";
    private static readonly string[] ReservedNames = { "All", "From", "TryParse" };

    /// <inheritdoc />
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var enums = context.SyntaxProvider.ForAttributeWithMetadataName(
                AttributeName,
                static (node, _) => node is EnumDeclarationSyntax,
                static (ctx, _) => CreateModel(ctx))
            .Where(static m => m is not null)
            .Select(static (m, _) => m!);

        var snapshots = context.AdditionalTextsProvider
            .Select(static (file, ct) =>
            {
                var name = SagaStateSnapshot.EnumNameOf(file.Path);
                var text = name is null ? null : file.GetText(ct)?.ToString();
                return name is null || text is null ? null : new SagaStateSnapshot(name, SagaStateSnapshot.Parse(text));
            })
            .Where(static s => s is not null)
            .Select(static (s, _) => s!)
            .Collect();

        var collisions = enums.Collect().Select(static (models, _) => models
            .Where(m => m.IsAccessible && !m.HasGenericContainer && !m.HasAliases
                && !m.Members.Intersect(ReservedNames, StringComparer.Ordinal).Any())
            .GroupBy(m => (m.Namespace, m.EnumName))
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray());

        context.RegisterSourceOutput(enums.Combine(snapshots).Combine(collisions), static (spc, pair) =>
        {
            var ((model, all), conflictingNames) = pair;
            if (!model.IsAccessible || model.HasGenericContainer)
            {
                var descriptor = !model.IsAccessible ? SagaDiagnostics.InaccessibleEnum : SagaDiagnostics.GenericContainer;
                spc.ReportDiagnostic(Diagnostic.Create(descriptor, model.ToLocation(), model.EnumName));
                return;
            }
            if (model.HasAliases)
            {
                spc.ReportDiagnostic(Diagnostic.Create(SagaDiagnostics.AliasedValues, model.ToLocation(), model.EnumName));
                return;
            }
            var reserved = model.Members.Intersect(ReservedNames, StringComparer.Ordinal).ToArray();
            if (reserved.Length > 0)
            {
                foreach (var name in reserved)
                    spc.ReportDiagnostic(Diagnostic.Create(SagaDiagnostics.ReservedName, model.ToLocation(), name, model.EnumName));
                return;
            }

            if (conflictingNames.Contains((model.Namespace, model.EnumName)))
            {
                spc.ReportDiagnostic(Diagnostic.Create(SagaDiagnostics.GeneratedSetCollision, model.ToLocation(), model.EnumFullName, model.EnumName + "Set"));
                return;
            }

            var snapshot = all.FirstOrDefault(s => s.EnumName == model.EnumName);
            if (snapshot is not null)
                Report(spc, model, snapshot);
            spc.AddSource($"{model.EnumFullName.Replace("global::", string.Empty)}.SagaStates.g.cs", Emit(model));
        });
    }

    private static SagaStateSetModel? CreateModel(GeneratorAttributeSyntaxContext ctx)
    {
        if (ctx.TargetSymbol is not INamedTypeSymbol symbol || symbol.TypeKind != TypeKind.Enum)
            return null;

        var fields = symbol.GetMembers().OfType<IFieldSymbol>().Where(f => f.HasConstantValue).ToArray();
        var location = ctx.TargetNode.GetLocation();
        var lineSpan = location.GetLineSpan();
        var isAccessible = true;
        var hasGenericContainer = false;
        for (var type = symbol; type is not null; type = type.ContainingType)
        {
            isAccessible &= !type.IsFileLocal
                && type.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal or Accessibility.ProtectedOrInternal;
            hasGenericContainer |= type.Arity > 0;
        }
        return new SagaStateSetModel(
            symbol.ContainingNamespace.IsGlobalNamespace ? null : symbol.ContainingNamespace.ToDisplayString(),
            symbol.Name,
            symbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            IsEffectivelyPublic(symbol),
            fields.Select(f => f.ConstantValue).Distinct().Count() != fields.Length,
            isAccessible,
            hasGenericContainer,
            fields.Select(f => f.Name).ToArray(),
            fields.Select(f => Convert.ToDecimal(f.ConstantValue)).ToArray(),
            lineSpan.Path,
            location.SourceSpan,
            lineSpan.Span);
    }

    private static bool IsEffectivelyPublic(ISymbol symbol)
    {
        for (var s = symbol; s is not null && s is not INamespaceSymbol; s = s.ContainingSymbol)
        {
            if (s.DeclaredAccessibility != Accessibility.Public)
                return false;
        }
        return true;
    }

    private static void Report(SourceProductionContext spc, SagaStateSetModel model, SagaStateSnapshot snapshot)
    {
        var location = model.ToLocation();
        foreach (var removed in snapshot.States.Except(model.Members, StringComparer.Ordinal))
            spc.ReportDiagnostic(Diagnostic.Create(SagaDiagnostics.StateRemoved, location, removed, model.EnumName));
        foreach (var added in model.Members.Except(snapshot.States, StringComparer.Ordinal))
            spc.ReportDiagnostic(Diagnostic.Create(SagaDiagnostics.StateNotInSnapshot, location, added, model.EnumName));
    }

    private static string Emit(SagaStateSetModel model)
    {
        var className = model.EnumName + "Set";
        var access = model.IsPublic ? "public" : "internal";
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        if (model.Namespace is not null)
        {
            sb.Append("namespace ").Append(model.Namespace).AppendLine(";");
        }
        sb.AppendLine();
        sb.Append("/// <summary>Saga states generated from <see cref=\"").Append(model.EnumFullName).AppendLine("\"/>.</summary>");
        sb.AppendLine("[global::System.CodeDom.Compiler.GeneratedCode(\"EventFlux.Saga.Generators\", \"0.1.0\")]");
        sb.Append(access).Append(" static partial class ").AppendLine(className);
        sb.AppendLine("{");

        for (var i = 0; i < model.Members.Length; i++)
        {
            var name = model.Members[i];
            sb.Append("    /// <summary>The <c>").Append(name).AppendLine("</c> state.</summary>");
            sb.Append("    public static readonly ").Append(State).Append(' ').Append(name).Append(" = ")
              .Append(Definition(name, model.Values[i], i)).AppendLine(";");
            sb.AppendLine();
        }

        sb.AppendLine("    /// <summary>Every state of the set, in declaration order.</summary>");
        sb.Append("    public static global::System.Collections.Generic.IReadOnlyList<").Append(State).Append("> All { get; } = new ")
          .Append(State).Append("[] { ").Append(string.Join(", ", model.Members)).AppendLine(" };");
        sb.AppendLine();

        sb.AppendLine("    /// <summary>Returns the state of an enum value.</summary>");
        sb.AppendLine("    /// <param name=\"value\">The enum value.</param>");
        sb.AppendLine("    /// <returns>The matching state.</returns>");
        sb.AppendLine("    /// <exception cref=\"global::System.ArgumentOutOfRangeException\">Thrown for a value that is not a declared member.</exception>");
        sb.Append("    public static ").Append(State).Append(" From(").Append(model.EnumFullName).AppendLine(" value) => value switch");
        sb.AppendLine("    {");
        foreach (var name in model.Members)
            sb.Append("        ").Append(model.EnumFullName).Append('.').Append(name).Append(" => ").Append(name).AppendLine(",");
        sb.AppendLine("        _ => throw new global::System.ArgumentOutOfRangeException(nameof(value), value, null)");
        sb.AppendLine("    };");
        sb.AppendLine();

        sb.AppendLine("    /// <summary>Finds the state with the given persisted name.</summary>");
        sb.AppendLine("    /// <param name=\"name\">The persisted state name.</param>");
        sb.AppendLine("    /// <param name=\"state\">The matching state, when found.</param>");
        sb.AppendLine("    /// <returns><c>true</c> when the name belongs to this set.</returns>");
        sb.Append("    public static bool TryParse(string? name, out ").Append(State).AppendLine(" state)");
        sb.AppendLine("    {");
        sb.AppendLine("        switch (name)");
        sb.AppendLine("        {");
        foreach (var name in model.Members)
        {
            sb.Append("            case \"").Append(name).Append("\": state = ").Append(name).AppendLine("; return true;");
        }
        sb.AppendLine("            default: state = default; return false;");
        sb.AppendLine("        }");
        sb.AppendLine("    }");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string Definition(string name, decimal value, int index) => name switch
    {
        "Initial" => States + ".Initial",
        "Final" => States + ".Final",
        _ => $"{States}.Define(\"{name}\", {(value is >= int.MinValue and <= int.MaxValue ? value : index).ToString(System.Globalization.CultureInfo.InvariantCulture)})"
    };
}
