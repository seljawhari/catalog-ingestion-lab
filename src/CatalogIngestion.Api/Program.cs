using System.Net;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using CatalogIngestion.Api.Configuration;
using CatalogIngestion.Api.Contracts;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Ingestion;
using CatalogIngestion.Api.Observability;
using CatalogIngestion.Api.Persistence;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Options;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddOptions<IngestionOptions>()
    .Bind(builder.Configuration.GetSection(IngestionOptions.SectionName))
    .Validate(options => options.MaxPayloadBytes is >= 1 and <= 25 * 1024 * 1024, "MaxPayloadBytes must be 1-25 MiB.")
    .Validate(options => options.MaxItems is >= 1 and <= 100_000, "MaxItems must be 1-100,000.")
    .Validate(options => options.MaxAttempts is >= 1 and <= 10, "MaxAttempts must be 1-10.")
    .Validate(options => options.PollIntervalMilliseconds is >= 25 and <= 60_000, "PollIntervalMilliseconds must be 25-60,000.")
    .Validate(options => options.RetryBaseDelaySeconds is >= 1 and <= 60, "RetryBaseDelaySeconds must be 1-60.")
    .Validate(
        options => options.MaxRetainedDeadLetterPayloads is >= 0 and <= 100,
        "MaxRetainedDeadLetterPayloads must be 0-100.")
    .ValidateOnStart();

int maxPayloadBytes = builder.Configuration.GetValue<int?>("Ingestion:MaxPayloadBytes") ?? 5 * 1024 * 1024;
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = maxPayloadBytes + (256 * 1024);
    options.ValueLengthLimit = 16 * 1024;
});
builder.WebHost.ConfigureKestrel(options =>
    options.Limits.MaxRequestBodySize = maxPayloadBytes + (256 * 1024));

builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();
builder.Services.AddRateLimiter(options =>
{
    options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context =>
        RateLimitPartition.GetFixedWindowLimiter(
            context.Connection.RemoteIpAddress?.ToString() ?? "unknown",
            _ => new FixedWindowRateLimiterOptions
            {
                AutoReplenishment = true,
                PermitLimit = 120,
                QueueLimit = 0,
                Window = TimeSpan.FromMinutes(1),
            }));
});

builder.Services.AddSingleton<TimeProvider>(TimeProvider.System);
builder.Services.AddSingleton<ICatalogStore, SqliteCatalogStore>();
builder.Services.AddSingleton<ICatalogPayloadParser, CatalogPayloadParser>();
builder.Services.AddSingleton<IngestionTelemetry>();
builder.Services.AddSingleton<IngestionProcessor>();
if (builder.Configuration.GetValue("Ingestion:WorkerEnabled", true))
{
    builder.Services.AddHostedService<IngestionWorker>();
}

WebApplication app = builder.Build();

app.UseExceptionHandler();
app.UseMiddleware<CorrelationIdMiddleware>();
app.UseRateLimiter();

await using (AsyncServiceScope scope = app.Services.CreateAsyncScope())
{
    ICatalogStore store = scope.ServiceProvider.GetRequiredService<ICatalogStore>();
    TimeProvider timeProvider = scope.ServiceProvider.GetRequiredService<TimeProvider>();
    await store.InitializeAsync(CancellationToken.None);
    await store.RequeueInterruptedJobsAsync(timeProvider.GetUtcNow(), CancellationToken.None);
}

app.MapGet("/", () => Results.Ok(new
{
    service = "catalog-ingestion-lab",
    version = "v1",
    openApi = "/openapi/v1.json",
    liveness = "/health/live",
    readiness = "/health/ready",
}));

app.MapOpenApi();

app.MapGet("/health/live", () => Results.Ok(new { status = "live" }))
    .ExcludeFromDescription();

app.MapGet("/health/ready", async (ICatalogStore store, CancellationToken cancellationToken) =>
    await store.CanConnectAsync(cancellationToken)
        ? Results.Ok(new { status = "ready" })
        : Results.Json(new { status = "unavailable" }, statusCode: StatusCodes.Status503ServiceUnavailable))
    .ExcludeFromDescription();

app.MapPost("/v1/ingestions", async (
    HttpRequest request,
    ICatalogStore store,
    IngestionTelemetry telemetry,
    TimeProvider timeProvider,
    IOptions<IngestionOptions> options,
    CancellationToken cancellationToken) =>
{
    string? idempotencyKey = request.Headers["Idempotency-Key"].FirstOrDefault();
    if (!RequestValidation.IsValidIdempotencyKey(idempotencyKey))
    {
        return Results.Problem(
            title: "Invalid idempotency key",
            detail: "Idempotency-Key must be 8-128 characters using letters, numbers, '-', '_', '.' or ':'.",
            statusCode: StatusCodes.Status400BadRequest);
    }

    if (!request.HasFormContentType)
    {
        return Results.Problem(
            title: "Unsupported content type",
            detail: "Submit a multipart/form-data request containing a file field named 'file'.",
            statusCode: StatusCodes.Status415UnsupportedMediaType);
    }

    IFormCollection form;
    try
    {
        form = await request.ReadFormAsync(cancellationToken);
    }
    catch (InvalidDataException)
    {
        return Results.Problem(
            title: "Invalid multipart request",
            detail: "The multipart body is malformed or exceeds the configured size limit.",
            statusCode: StatusCodes.Status400BadRequest);
    }

    IFormFile? file = form.Files.GetFile("file");
    if (file is null || file.Length == 0)
    {
        return Results.Problem(
            title: "File required",
            detail: "Provide a non-empty CSV or JSON file in the 'file' form field.",
            statusCode: StatusCodes.Status400BadRequest);
    }

    if (file.Length > options.Value.MaxPayloadBytes)
    {
        return Results.Problem(
            title: "Payload too large",
            detail: $"Files may not exceed {options.Value.MaxPayloadBytes} bytes.",
            statusCode: StatusCodes.Status413PayloadTooLarge);
    }

    if (!RequestValidation.TryDetectFormat(file, out PayloadFormat format))
    {
        return Results.Problem(
            title: "Unsupported file format",
            detail: "Only .csv and .json catalog documents are accepted.",
            statusCode: StatusCodes.Status415UnsupportedMediaType);
    }

    await using MemoryStream buffer = new(capacity: checked((int)file.Length));
    await file.CopyToAsync(buffer, cancellationToken);
    byte[] payload = buffer.ToArray();
    string payloadHash = Convert.ToHexString(SHA256.HashData(payload)).ToLowerInvariant();
    string originalFileName = Path.GetFileName(file.FileName);
    if (string.IsNullOrWhiteSpace(originalFileName))
    {
        originalFileName = format == PayloadFormat.Csv ? "catalog.csv" : "catalog.json";
    }
    else if (originalFileName.Length > 255)
    {
        originalFileName = originalFileName[..255];
    }

    CreateIngestionResult result = await store.CreateOrGetIngestionAsync(
        new CreateIngestionRequest(
            idempotencyKey!,
            payloadHash,
            format,
            originalFileName,
            payload,
            options.Value.MaxAttempts,
            timeProvider.GetUtcNow()),
        cancellationToken);

    if (result.Outcome == CreateIngestionOutcome.Conflict)
    {
        return Results.Problem(
            title: "Idempotency key conflict",
            detail: "This idempotency key was already used with a different payload.",
            statusCode: StatusCodes.Status409Conflict);
    }

    bool replayed = result.Outcome == CreateIngestionOutcome.Replayed;
    if (replayed)
    {
        telemetry.RecordReplay();
    }
    else
    {
        telemetry.RecordSubmission(format);
    }

    string statusUrl = $"/v1/ingestions/{result.Job.Id:D}";
    return Results.Accepted(
        statusUrl,
        new SubmitIngestionResponse(result.Job.Id, result.Job.Status.ToString(), replayed, statusUrl));
})
    .DisableAntiforgery()
    .WithName("SubmitCatalogIngestion")
    .Produces<SubmitIngestionResponse>(StatusCodes.Status202Accepted)
    .ProducesProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status409Conflict)
    .ProducesProblem(StatusCodes.Status413PayloadTooLarge)
    .ProducesProblem(StatusCodes.Status415UnsupportedMediaType);

app.MapGet("/v1/ingestions/{jobId:guid}", async (
    Guid jobId,
    ICatalogStore store,
    CancellationToken cancellationToken) =>
{
    IngestionJob? job = await store.GetIngestionAsync(jobId, cancellationToken);
    return job is null
        ? Results.NotFound()
        : Results.Ok(IngestionJobResponse.From(job));
})
    .WithName("GetCatalogIngestion")
    .Produces<IngestionJobResponse>()
    .Produces(StatusCodes.Status404NotFound);

app.MapGet("/v1/dead-letters", async (
    int? limit,
    ICatalogStore store,
    CancellationToken cancellationToken) =>
{
    int boundedLimit = Math.Clamp(limit ?? 50, 1, 100);
    IReadOnlyList<IngestionJob> jobs = await store.ListDeadLettersAsync(boundedLimit, cancellationToken);
    return Results.Ok(jobs.Select(IngestionJobResponse.From));
})
    .WithName("ListDeadLetters");

app.MapPost("/v1/dead-letters/{jobId:guid}/requeue", async (
    Guid jobId,
    ICatalogStore store,
    TimeProvider timeProvider,
    CancellationToken cancellationToken) =>
{
    RequeueOutcome outcome = await store.RequeueDeadLetterAsync(
        jobId,
        timeProvider.GetUtcNow(),
        cancellationToken);
    return outcome switch
    {
        RequeueOutcome.Requeued => Results.Accepted($"/v1/ingestions/{jobId:D}", new { jobId, status = "Queued" }),
        RequeueOutcome.NotFound => Results.NotFound(),
        RequeueOutcome.NotRequeueable => Results.Conflict(new
        {
            title = "Job is not requeueable",
            detail = "Only dead-lettered jobs with a retained payload can be requeued.",
        }),
        _ => throw new ArgumentOutOfRangeException(nameof(outcome), outcome, "Unknown requeue outcome."),
    };
})
    .WithName("RequeueDeadLetter")
    .Produces(StatusCodes.Status202Accepted)
    .Produces(StatusCodes.Status404NotFound)
    .Produces(StatusCodes.Status409Conflict);

app.MapGet("/v1/catalog/items/{sku}", async (
    string sku,
    ICatalogStore store,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(sku) || sku.Length > 64)
    {
        return Results.BadRequest();
    }

    CatalogItem? item = await store.GetCatalogItemAsync(sku.Trim(), cancellationToken);
    return item is null
        ? Results.NotFound()
        : Results.Ok(CatalogItemResponse.From(item));
})
    .WithName("GetCatalogItem")
    .Produces<CatalogItemResponse>()
    .Produces(StatusCodes.Status400BadRequest)
    .Produces(StatusCodes.Status404NotFound);

app.MapGet("/v1/catalog/items", async (
    int? limit,
    string? afterSku,
    ICatalogStore store,
    CancellationToken cancellationToken) =>
{
    int boundedLimit = Math.Clamp(limit ?? 100, 1, 500);
    if (afterSku?.Length > 64)
    {
        return Results.BadRequest();
    }

    IReadOnlyList<CatalogItem> items = await store.ListCatalogItemsAsync(
        boundedLimit,
        afterSku?.Trim(),
        cancellationToken);
    return Results.Ok(items.Select(CatalogItemResponse.From));
})
    .WithName("ListCatalogItems");

await app.RunAsync();

public partial class Program;
