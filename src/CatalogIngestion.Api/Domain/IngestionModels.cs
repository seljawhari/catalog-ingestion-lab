using System.Text.Json.Serialization;

namespace CatalogIngestion.Api.Domain;

public enum IngestionStatus
{
    Queued,
    Processing,
    RetryScheduled,
    Completed,
    DeadLettered,
}

public enum PayloadFormat
{
    Csv,
    Json,
}

public sealed record IngestionJob(
    Guid Id,
    IngestionStatus Status,
    PayloadFormat Format,
    string OriginalFileName,
    int AttemptCount,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? NextAttemptAt,
    string? LastError,
    IReadOnlyList<ValidationIssue> ValidationIssues);

public sealed record ClaimedIngestionJob(
    Guid Id,
    PayloadFormat Format,
    byte[] Payload,
    int AttemptCount,
    int MaxAttempts);

public sealed record ValidationIssue(int? Row, string Field, string Message);

public sealed record CatalogItem(
    string Sku,
    string Name,
    long PriceMinor,
    string Currency,
    int Stock,
    Guid SourceJobId,
    DateTimeOffset UpdatedAt)
{
    public decimal Price => PriceMinor / 100m;
}

public sealed class CatalogItemDocument
{
    [JsonRequired]
    public string? Sku { get; init; }

    [JsonRequired]
    public string? Name { get; init; }

    [JsonRequired]
    public decimal? Price { get; init; }

    [JsonRequired]
    public string? Currency { get; init; }

    [JsonRequired]
    public int? Stock { get; init; }
}

public sealed record ParsedCatalogItem(
    string Sku,
    string Name,
    long PriceMinor,
    string Currency,
    int Stock);

public sealed record ParseResult(
    IReadOnlyList<ParsedCatalogItem> Items,
    IReadOnlyList<ValidationIssue> Issues);
