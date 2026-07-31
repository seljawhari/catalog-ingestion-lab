using System.Security.Cryptography;
using System.Text;
using CatalogIngestion.Api.Configuration;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

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
        _store = new SqliteCatalogStore(configuration, Options.Create(new IngestionOptions()));
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
    public async Task FailAndRequeue_PreservesPayloadAndJoinsTailAsNewOperatorDecision()
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
        CreateIngestionResult newer = await _store.CreateOrGetIngestionAsync(
            Request("catalog-batch-004", Encoding.UTF8.GetBytes("newer"), now.AddSeconds(2)),
            CancellationToken.None);
        RequeueOutcome requeue = await _store.RequeueDeadLetterAsync(
            created.Job.Id,
            now.AddSeconds(3),
            CancellationToken.None);

        Assert.Equal(IngestionStatus.DeadLettered, status);
        Assert.Equal(RequeueOutcome.Requeued, requeue);
        IngestionJob queued = Assert.IsType<IngestionJob>(
            await _store.GetIngestionAsync(created.Job.Id, CancellationToken.None));
        Assert.Equal(IngestionStatus.Queued, queued.Status);
        ClaimedIngestionJob next = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now.AddSeconds(3), CancellationToken.None));
        Assert.Equal(newer.Job.Id, next.Id);
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

    [Fact]
    public async Task ScheduledRetry_BlocksNewerSubmissionSoLatestAcceptedValueWins()
    {
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        CreateIngestionResult older = await _store.CreateOrGetIngestionAsync(
            Request("catalog-ordered-old", Encoding.UTF8.GetBytes("old"), now),
            CancellationToken.None);
        ClaimedIngestionJob firstAttempt = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now, CancellationToken.None));
        await _store.FailIngestionAsync(
            firstAttempt,
            "Synthetic transient failure.",
            [],
            retryable: true,
            now.AddSeconds(10),
            now.AddSeconds(1),
            CancellationToken.None);

        CreateIngestionResult newer = await _store.CreateOrGetIngestionAsync(
            Request("catalog-ordered-new", Encoding.UTF8.GetBytes("new"), now.AddSeconds(2)),
            CancellationToken.None);

        Assert.Null(await _store.ClaimNextIngestionAsync(now.AddSeconds(5), CancellationToken.None));

        ClaimedIngestionJob retry = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now.AddSeconds(10), CancellationToken.None));
        Assert.Equal(older.Job.Id, retry.Id);
        await _store.CompleteIngestionAsync(
            retry,
            [new ParsedCatalogItem("SKU-ORDERED", "Older value", 100, "USD", 1)],
            now.AddSeconds(11),
            CancellationToken.None);

        ClaimedIngestionJob next = Assert.IsType<ClaimedIngestionJob>(
            await _store.ClaimNextIngestionAsync(now.AddSeconds(11), CancellationToken.None));
        Assert.Equal(newer.Job.Id, next.Id);
        await _store.CompleteIngestionAsync(
            next,
            [new ParsedCatalogItem("SKU-ORDERED", "Newest accepted value", 200, "USD", 2)],
            now.AddSeconds(12),
            CancellationToken.None);

        CatalogItem item = Assert.IsType<CatalogItem>(
            await _store.GetCatalogItemAsync("SKU-ORDERED", CancellationToken.None));
        Assert.Equal("Newest accepted value", item.Name);
        Assert.Equal(newer.Job.Id, item.SourceJobId);
    }

    [Fact]
    public async Task DeadLetters_RetainOnlyConfiguredNumberOfPayloads()
    {
        SqliteCatalogStore boundedStore = CreateStore(_connectionString, maxRetainedDeadLetterPayloads: 2);
        DateTimeOffset now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");
        List<Guid> jobIds = [];

        for (int index = 0; index < 3; index++)
        {
            CreateIngestionResult created = await boundedStore.CreateOrGetIngestionAsync(
                Request(
                    $"catalog-retention-{index:D3}",
                    Encoding.UTF8.GetBytes($"invalid-{index}"),
                    now.AddSeconds(index),
                    maxAttempts: 1),
                CancellationToken.None);
            jobIds.Add(created.Job.Id);
            ClaimedIngestionJob claimed = Assert.IsType<ClaimedIngestionJob>(
                await boundedStore.ClaimNextIngestionAsync(now.AddSeconds(index), CancellationToken.None));
            await boundedStore.FailIngestionAsync(
                claimed,
                "Payload validation failed.",
                [new ValidationIssue(1, "sku", "SKU is invalid.")],
                retryable: false,
                retryAt: null,
                now.AddSeconds(index),
                CancellationToken.None);
        }

        IReadOnlyList<IngestionJob> deadLetters = await boundedStore.ListDeadLettersAsync(
            10,
            CancellationToken.None);
        Assert.Equal(3, deadLetters.Count);
        Assert.Equal(2, deadLetters.Count(job => job.PayloadRetained));
        Assert.False((await boundedStore.GetIngestionAsync(jobIds[0], CancellationToken.None))!.PayloadRetained);
        Assert.Equal(
            RequeueOutcome.NotRequeueable,
            await boundedStore.RequeueDeadLetterAsync(jobIds[0], now.AddMinutes(1), CancellationToken.None));
    }

    private static SqliteCatalogStore CreateStore(
        string connectionString,
        int maxRetainedDeadLetterPayloads = 20)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Catalog"] = connectionString,
            })
            .Build();
        return new SqliteCatalogStore(
            configuration,
            Options.Create(new IngestionOptions
            {
                MaxRetainedDeadLetterPayloads = maxRetainedDeadLetterPayloads,
            }));
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
