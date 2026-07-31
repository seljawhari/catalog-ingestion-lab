using System.Security.Cryptography;
using System.Text;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Persistence;
using Microsoft.Extensions.Configuration;

namespace CatalogIngestion.Tests;

public sealed class SqliteCatalogStoreTests : IAsyncLifetime
{
    private string _directory = null!;
    private string _connectionString = null!;
    private SqliteCatalogStore _store = null!;

    public async Task InitializeAsync()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"catalog-store-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
        _connectionString = $"Data Source={Path.Combine(_directory, "catalog.db")};Default Timeout=5";
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Catalog"] = _connectionString,
            })
            .Build();
        _store = new SqliteCatalogStore(configuration);
        await _store.InitializeAsync(CancellationToken.None);
    }

    public Task DisposeAsync()
    {
        Directory.Delete(_directory, recursive: true);
        return Task.CompletedTask;
    }

    [Fact]
    public async Task CreateOrGet_IsIdempotentAndRejectsKeyReuseWithDifferentPayload()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        byte[] firstPayload = Encoding.UTF8.GetBytes("first");
        CreateIngestionRequest first = Request("catalog-batch-001", firstPayload, now);

        CreateIngestionResult created = await _store.CreateOrGetIngestionAsync(first, CancellationToken.None);
        CreateIngestionResult replayed = await _store.CreateOrGetIngestionAsync(first, CancellationToken.None);
        CreateIngestionResult conflict = await _store.CreateOrGetIngestionAsync(
            Request("catalog-batch-001", Encoding.UTF8.GetBytes("different"), now),
            CancellationToken.None);

        Assert.Equal(CreateIngestionOutcome.Created, created.Outcome);
        Assert.Equal(CreateIngestionOutcome.Replayed, replayed.Outcome);
        Assert.Equal(created.Job.Id, replayed.Job.Id);
        Assert.Equal(CreateIngestionOutcome.Conflict, conflict.Outcome);
    }

    [Fact]
    public async Task ConcurrentIdenticalSubmissions_CreateExactlyOneJob()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        CreateIngestionRequest request = Request(
            "catalog-concurrent-001",
            Encoding.UTF8.GetBytes("same-payload"),
            now);

        CreateIngestionResult[] results = await Task.WhenAll(
            Enumerable.Range(0, 8)
                .Select(_ => _store.CreateOrGetIngestionAsync(request, CancellationToken.None)));

        Assert.Single(results, result => result.Outcome == CreateIngestionOutcome.Created);
        Assert.Equal(7, results.Count(result => result.Outcome == CreateIngestionOutcome.Replayed));
        Assert.Single(results.Select(result => result.Job.Id).Distinct());
    }

    [Fact]
    public async Task CompleteIngestion_AtomicallyUpsertsCatalogAndCompletesJob()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        CreateIngestionResult created = await _store.CreateOrGetIngestionAsync(
            Request("catalog-batch-002", Encoding.UTF8.GetBytes("payload"), now),
            CancellationToken.None);
        ClaimedIngestionJob job = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now, CancellationToken.None));

        await _store.CompleteIngestionAsync(
            job,
            [new ParsedCatalogItem("SKU-1", "Widget", 1299, "USD", 4)],
            now.AddSeconds(1),
            CancellationToken.None);

        IngestionJob completed = Assert.IsType<IngestionJob>(
            await _store.GetIngestionAsync(created.Job.Id, CancellationToken.None));
        CatalogItem item = Assert.IsType<CatalogItem>(
            await _store.GetCatalogItemAsync("sku-1", CancellationToken.None));
        Assert.Equal(IngestionStatus.Completed, completed.Status);
        Assert.Equal(12.99m, item.Price);
        Assert.Equal(created.Job.Id, item.SourceJobId);
    }

    [Fact]
    public async Task FailAndRequeue_PreservesDeadLetterPayloadForManualRecovery()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        CreateIngestionResult created = await _store.CreateOrGetIngestionAsync(
            Request("catalog-batch-003", Encoding.UTF8.GetBytes("invalid"), now, maxAttempts: 1),
            CancellationToken.None);
        ClaimedIngestionJob claimed = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now, CancellationToken.None));

        IngestionStatus status = await _store.FailIngestionAsync(
            claimed,
            "Payload validation failed.",
            [new ValidationIssue(1, "sku", "SKU is invalid.")],
            retryable: false,
            retryAt: null,
            now.AddSeconds(1),
            CancellationToken.None);
        RequeueOutcome requeue = await _store.RequeueDeadLetterAsync(
            created.Job.Id,
            now.AddSeconds(2),
            CancellationToken.None);

        Assert.Equal(IngestionStatus.DeadLettered, status);
        Assert.Equal(RequeueOutcome.Requeued, requeue);
        IngestionJob queued = Assert.IsType<IngestionJob>(
            await _store.GetIngestionAsync(created.Job.Id, CancellationToken.None));
        Assert.Equal(IngestionStatus.Queued, queued.Status);
    }

    [Fact]
    public async Task StartupRecovery_ReschedulesAnInterruptedProcessingJob()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        CreateIngestionResult created = await _store.CreateOrGetIngestionAsync(
            Request("catalog-recovery-001", Encoding.UTF8.GetBytes("payload"), now),
            CancellationToken.None);
        Assert.NotNull(await _store.ClaimNextIngestionAsync(now, CancellationToken.None));

        SqliteCatalogStore restartedStore = CreateStore(_connectionString);
        await restartedStore.InitializeAsync(CancellationToken.None);
        await restartedStore.RequeueInterruptedJobsAsync(now.AddMinutes(1), CancellationToken.None);

        IngestionJob recovered = Assert.IsType<IngestionJob>(
            await restartedStore.GetIngestionAsync(created.Job.Id, CancellationToken.None));
        Assert.Equal(IngestionStatus.RetryScheduled, recovered.Status);
        Assert.Equal(1, recovered.AttemptCount);
        Assert.Equal(now.AddMinutes(1), recovered.NextAttemptAt);
    }

    private static SqliteCatalogStore CreateStore(string connectionString)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Catalog"] = connectionString,
            })
            .Build();
        return new SqliteCatalogStore(configuration);
    }

    private static CreateIngestionRequest Request(
        string key,
        byte[] payload,
        DateTimeOffset now,
        int maxAttempts = 3) => new(
            key,
            Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant(),
            PayloadFormat.Json,
            "catalog.json",
            payload,
            maxAttempts,
            now);
}
