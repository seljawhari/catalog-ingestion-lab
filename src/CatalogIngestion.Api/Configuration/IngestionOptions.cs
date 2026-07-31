namespace CatalogIngestion.Api.Configuration;

public sealed class IngestionOptions
{
    public const string SectionName = "Ingestion";

    public int MaxPayloadBytes { get; init; } = 5 * 1024 * 1024;

    public int MaxItems { get; init; } = 10_000;

    public int MaxAttempts { get; init; } = 3;

    public int PollIntervalMilliseconds { get; init; } = 250;

    public int RetryBaseDelaySeconds { get; init; } = 2;

    public int MaxRetainedDeadLetterPayloads { get; init; } = 20;

    public bool WorkerEnabled { get; init; } = true;
}
