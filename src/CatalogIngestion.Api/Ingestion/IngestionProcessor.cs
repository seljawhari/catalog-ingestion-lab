using System.Diagnostics;
using CatalogIngestion.Api.Configuration;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Observability;
using CatalogIngestion.Api.Persistence;
using Microsoft.Extensions.Options;

namespace CatalogIngestion.Api.Ingestion;

public sealed class IngestionProcessor(
    ICatalogStore store,
    ICatalogPayloadParser parser,
    IngestionTelemetry telemetry,
    IOptions<IngestionOptions> options,
    TimeProvider timeProvider,
    ILogger<IngestionProcessor> logger)
{
    private readonly IngestionOptions _options = options.Value;

    public async Task<bool> ProcessNextAsync(CancellationToken cancellationToken)
    {
        DateTimeOffset claimedAt = timeProvider.GetUtcNow();
        ClaimedIngestionJob? job = await store.ClaimNextIngestionAsync(claimedAt, cancellationToken);
        if (job is null)
        {
            return false;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();
        using Activity? activity = telemetry.StartProcessing(job.Id, job.AttemptCount);
        using IDisposable? scope = logger.BeginScope(new Dictionary<string, object>
        {
            ["JobId"] = job.Id,
            ["Attempt"] = job.AttemptCount,
        });

        try
        {
            ParseResult result = parser.Parse(job.Format, job.Payload, _options.MaxItems);
            if (result.Issues.Count > 0)
            {
                IngestionStatus rejectedStatus = await store.FailIngestionAsync(
                    job,
                    "Payload validation failed.",
                    result.Issues,
                    retryable: false,
                    retryAt: null,
                    timeProvider.GetUtcNow(),
                    cancellationToken);
                telemetry.RecordFailure(rejectedStatus, stopwatch.Elapsed.TotalMilliseconds);
                activity?.SetStatus(ActivityStatusCode.Error, "validation_failed");
                logger.LogWarning(
                    "Catalog ingestion rejected with {ValidationIssueCount} validation issues",
                    result.Issues.Count);
                return true;
            }

            await store.CompleteIngestionAsync(
                job,
                result.Items,
                timeProvider.GetUtcNow(),
                cancellationToken);
            telemetry.RecordCompletion(stopwatch.Elapsed.TotalMilliseconds, result.Items.Count);
            activity?.SetTag("catalog.item.count", result.Items.Count);
            activity?.SetStatus(ActivityStatusCode.Ok);
            logger.LogInformation("Catalog ingestion completed with {ItemCount} items", result.Items.Count);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            DateTimeOffset failedAt = timeProvider.GetUtcNow();
            int exponent = Math.Clamp(job.AttemptCount - 1, 0, 5);
            TimeSpan delay = TimeSpan.FromSeconds(
                Math.Min(60, _options.RetryBaseDelaySeconds * Math.Pow(2, exponent)));
            IngestionStatus status = await store.FailIngestionAsync(
                job,
                "Transient processing failure. Inspect structured logs using the job ID.",
                [],
                retryable: true,
                failedAt.Add(delay),
                failedAt,
                cancellationToken);
            telemetry.RecordFailure(status, stopwatch.Elapsed.TotalMilliseconds);
            activity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
            logger.LogError(exception, "Catalog ingestion failed; new status is {Status}", status);
            return true;
        }
    }
}
