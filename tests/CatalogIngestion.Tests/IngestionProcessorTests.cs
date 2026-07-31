using System.Security.Cryptography;
using System.Text;
using CatalogIngestion.Api.Configuration;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Ingestion;
using CatalogIngestion.Api.Observability;
using CatalogIngestion.Api.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace CatalogIngestion.Tests;

public sealed class IngestionProcessorTests : IAsyncLifetime
{
    private string _directory = null!;
    private SqliteCatalogStore _store = null!;
    private readonly DateTimeOffset _now = DateTimeOffset.Parse("2026-07-30T12:00:00Z");

    public async Task InitializeAsync()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"catalog-processor-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:Catalog"] = $"Data Source={Path.Combine(_directory, "catalog.db")};Default Timeout=5",
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
    public async Task ProcessNext_ValidPayloadCompletesAndInvalidPayloadDeadLettersWithoutPartialWrites()
    {
        byte[] valid = Encoding.UTF8.GetBytes("""
            [{"sku":"SKU-1","name":"Widget","price":9.95,"currency":"USD","stock":5}]
            """);
        byte[] invalid = Encoding.UTF8.GetBytes("""
            [{"sku":"","name":"Bad","price":-1,"currency":"USD","stock":5}]
            """);
        CreateIngestionResult validJob = await CreateAsync("processor-valid-001", valid);
        CreateIngestionResult invalidJob = await CreateAsync("processor-invalid-001", invalid);

        using IngestionTelemetry telemetry = new();
        IngestionProcessor processor = CreateProcessor(new CatalogPayloadParser(), telemetry);
        Assert.True(await processor.ProcessNextAsync(CancellationToken.None));
        Assert.True(await processor.ProcessNextAsync(CancellationToken.None));

        Assert.Equal(
            IngestionStatus.Completed,
            (await _store.GetIngestionAsync(validJob.Job.Id, CancellationToken.None))!.Status);
        Assert.Equal(
            IngestionStatus.DeadLettered,
            (await _store.GetIngestionAsync(invalidJob.Job.Id, CancellationToken.None))!.Status);
        Assert.Single(await _store.ListCatalogItemsAsync(100, null, CancellationToken.None));
    }

    [Fact]
    public async Task ProcessNext_UnexpectedFailureDeadLettersAtAttemptLimit()
    {
        await CreateAsync("processor-retry-001", Encoding.UTF8.GetBytes("[]"));
        using IngestionTelemetry telemetry = new();
        IngestionProcessor processor = CreateProcessor(new ThrowingParser(), telemetry);

        Assert.True(await processor.ProcessNextAsync(CancellationToken.None));

        IngestionJob job = Assert.Single(await _store.ListDeadLettersAsync(100, CancellationToken.None));
        Assert.Equal(IngestionStatus.DeadLettered, job.Status);
        Assert.Equal(1, job.AttemptCount);
    }

    private async Task<CreateIngestionResult> CreateAsync(string key, byte[] payload) =>
        await _store.CreateOrGetIngestionAsync(
            new CreateIngestionRequest(
                key,
                Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant(),
                PayloadFormat.Json,
                "catalog.json",
                payload,
                MaxAttempts: 1,
                _now),
            CancellationToken.None);

    private IngestionProcessor CreateProcessor(ICatalogPayloadParser parser, IngestionTelemetry telemetry) => new(
        _store,
        parser,
        telemetry,
        Options.Create(new IngestionOptions
        {
            MaxItems = 100,
            MaxAttempts = 1,
            RetryBaseDelaySeconds = 1,
        }),
        new FixedTimeProvider(_now),
        NullLogger<IngestionProcessor>.Instance);

    private sealed class ThrowingParser : ICatalogPayloadParser
    {
        public ParseResult Parse(PayloadFormat format, ReadOnlyMemory<byte> payload, int maxItems) =>
            throw new IOException("Synthetic transient failure.");
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
