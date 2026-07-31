using CatalogIngestion.Api.Domain;

namespace CatalogIngestion.Api.Contracts;

public sealed record SubmitIngestionResponse(
    Guid JobId,
    string Status,
    bool Replayed,
    string StatusUrl);

public sealed record IngestionJobResponse(
    Guid JobId,
    string Status,
    string Format,
    string OriginalFileName,
    int AttemptCount,
    int MaxAttempts,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? NextAttemptAt,
    string? LastError,
    IReadOnlyList<ValidationIssue> ValidationIssues,
    bool Requeueable)
{
    public static IngestionJobResponse From(IngestionJob job) => new(
        job.Id,
        job.Status.ToString(),
        job.Format.ToString(),
        job.OriginalFileName,
        job.AttemptCount,
        job.MaxAttempts,
        job.CreatedAt,
        job.UpdatedAt,
        job.NextAttemptAt,
        job.LastError,
        job.ValidationIssues,
        job.Status == IngestionStatus.DeadLettered && job.PayloadRetained);
}

public sealed record CatalogItemResponse(
    string Sku,
    string Name,
    decimal Price,
    string Currency,
    int Stock,
    Guid SourceJobId,
    DateTimeOffset UpdatedAt)
{
    public static CatalogItemResponse From(CatalogItem item) => new(
        item.Sku,
        item.Name,
        item.Price,
        item.Currency,
        item.Stock,
        item.SourceJobId,
        item.UpdatedAt);
}
