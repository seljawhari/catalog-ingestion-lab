using CatalogIngestion.Api.Domain;

namespace CatalogIngestion.Api.Persistence;

public sealed record CreateIngestionRequest(
    string IdempotencyKey,
    string PayloadHash,
    PayloadFormat Format,
    string OriginalFileName,
    byte[] Payload,
    int MaxAttempts,
    DateTimeOffset CreatedAt);

public enum CreateIngestionOutcome
{
    Created,
    Replayed,
    Conflict,
}

public sealed record CreateIngestionResult(
    CreateIngestionOutcome Outcome,
    IngestionJob Job);

public enum RequeueOutcome
{
    Requeued,
    NotFound,
    NotRequeueable,
}

public interface ICatalogStore
{
    Task InitializeAsync(CancellationToken cancellationToken);

    Task RequeueInterruptedJobsAsync(DateTimeOffset now, CancellationToken cancellationToken);

    Task<CreateIngestionResult> CreateOrGetIngestionAsync(
        CreateIngestionRequest request,
        CancellationToken cancellationToken);

    Task<IngestionJob?> GetIngestionAsync(Guid jobId, CancellationToken cancellationToken);

    Task<ClaimedIngestionJob?> ClaimNextIngestionAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken);

    Task CompleteIngestionAsync(
        ClaimedIngestionJob job,
        IReadOnlyList<ParsedCatalogItem> items,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken);

    Task<IngestionStatus> FailIngestionAsync(
        ClaimedIngestionJob job,
        string error,
        IReadOnlyList<ValidationIssue> issues,
        bool retryable,
        DateTimeOffset? retryAt,
        DateTimeOffset failedAt,
        CancellationToken cancellationToken);

    Task<RequeueOutcome> RequeueDeadLetterAsync(
        Guid jobId,
        DateTimeOffset now,
        CancellationToken cancellationToken);

    Task<IReadOnlyList<IngestionJob>> ListDeadLettersAsync(
        int limit,
        CancellationToken cancellationToken);

    Task<CatalogItem?> GetCatalogItemAsync(string sku, CancellationToken cancellationToken);

    Task<IReadOnlyList<CatalogItem>> ListCatalogItemsAsync(
        int limit,
        string? afterSku,
        CancellationToken cancellationToken);

    Task<bool> CanConnectAsync(CancellationToken cancellationToken);
}
