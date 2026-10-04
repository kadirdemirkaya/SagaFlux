namespace EventFlux.Saga;

/// <summary>
/// One entry of a saga instance's compensation stack.
/// </summary>
/// <remarks>
/// Entries are append-only: running a compensation marks the entry with <see cref="CompensatedAt"/> instead of
/// removing it, so the full history stays available for auditing. Only the <see cref="CompensationActionKey"/> is
/// persisted, never the delegate — keep keys stable across deployments, otherwise instances created by an older
/// version cannot be compensated (see CLAUDE.md §5.1).
/// </remarks>
/// <param name="StateName">The state the instance entered when the entry was recorded.</param>
/// <param name="CompensationActionKey">The key of the compensation action registered with <c>OnCompensate</c>.</param>
/// <param name="EnteredAt">When the entry was recorded.</param>
/// <param name="CompensatedAt">When the compensation ran, or <c>null</c> while it is pending.</param>
public sealed record CompensationEntry(
    string StateName,
    string CompensationActionKey,
    DateTimeOffset EnteredAt,
    DateTimeOffset? CompensatedAt = null)
{
    /// <summary>
    /// Gets whether the compensation action has run.
    /// </summary>
    public bool IsCompensated => CompensatedAt.HasValue;
}
