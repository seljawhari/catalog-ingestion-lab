using System.Data;
using System.Globalization;
using System.Text.Json;
using CatalogIngestion.Api.Domain;
using Microsoft.Data.Sqlite;

namespace CatalogIngestion.Api.Persistence;

public sealed class SqliteCatalogStore : ICatalogStore
{
    private const int SqliteConstraintError = 19;
    private const int MaxStoredErrorLength = 512;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly string _connectionString;

    public SqliteCatalogStore(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("Catalog")
            ?? throw new InvalidOperationException("ConnectionStrings:Catalog is required.");
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        EnsureDatabaseDirectoryExists();
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using (SqliteCommand journalMode = connection.CreateCommand())
        {
            journalMode.CommandText = "PRAGMA journal_mode = WAL;";
            string? selectedMode = Convert.ToString(
                await journalMode.ExecuteScalarAsync(cancellationToken),
                CultureInfo.InvariantCulture);
            if (!string.Equals(selectedMode, "wal", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("SQLite WAL mode is required for the durable local queue.");
            }
        }

        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ingestion_jobs (
                id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                payload_hash TEXT NOT NULL,
                payload_format TEXT NOT NULL CHECK (payload_format IN ('Csv', 'Json')),
                original_file_name TEXT NOT NULL,
                payload BLOB,
                status TEXT NOT NULL CHECK (
                    status IN ('Queued', 'Processing', 'RetryScheduled', 'Completed', 'DeadLettered')
                ),
                attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
                max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT,
                validation_errors_json TEXT NOT NULL DEFAULT '[]'
            );

            CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_dispatch
                ON ingestion_jobs(status, next_attempt_at, created_at);

            CREATE TABLE IF NOT EXISTS catalog_items (
                sku TEXT PRIMARY KEY COLLATE NOCASE,
                name TEXT NOT NULL,
                price_minor INTEGER NOT NULL CHECK (price_minor >= 0),
                currency TEXT NOT NULL,
                stock INTEGER NOT NULL CHECK (stock >= 0),
                source_job_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_job_id) REFERENCES ingestion_jobs(id)
            );

            INSERT OR IGNORE INTO schema_migrations(version, applied_at)
                VALUES (1, $appliedAt);
            """;
        command.Parameters.AddWithValue("$appliedAt", FormatTimestamp(DateTimeOffset.UtcNow));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RequeueInterruptedJobsAsync(DateTimeOffset now, CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            UPDATE ingestion_jobs
            SET status = CASE
                    WHEN attempt_count >= max_attempts THEN 'DeadLettered'
                    ELSE 'RetryScheduled'
                END,
                next_attempt_at = CASE
                    WHEN attempt_count >= max_attempts THEN NULL
                    ELSE $now
                END,
                updated_at = $now,
                last_error = 'Worker stopped before acknowledgement; recovery scheduled a safe retry.'
            WHERE status = 'Processing';
            """;
        command.Parameters.AddWithValue("$now", FormatTimestamp(now));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<CreateIngestionResult> CreateOrGetIngestionAsync(
        CreateIngestionRequest request,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        IngestionJobWithHash? existing = await FindByIdempotencyKeyAsync(
            connection,
            request.IdempotencyKey,
            cancellationToken);
        if (existing is not null)
        {
            return new CreateIngestionResult(
                string.Equals(existing.PayloadHash, request.PayloadHash, StringComparison.Ordinal)
                    ? CreateIngestionOutcome.Replayed
                    : CreateIngestionOutcome.Conflict,
                existing.Job);
        }

        Guid jobId = Guid.NewGuid();
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            INSERT INTO ingestion_jobs (
                id, idempotency_key, payload_hash, payload_format, original_file_name, payload,
                status, attempt_count, max_attempts, next_attempt_at, created_at, updated_at,
                last_error, validation_errors_json
            ) VALUES (
                $id, $idempotencyKey, $payloadHash, $payloadFormat, $originalFileName, $payload,
                'Queued', 0, $maxAttempts, NULL, $createdAt, $createdAt, NULL, '[]'
            );
            """;
        command.Parameters.AddWithValue("$id", jobId.ToString("D"));
        command.Parameters.AddWithValue("$idempotencyKey", request.IdempotencyKey);
        command.Parameters.AddWithValue("$payloadHash", request.PayloadHash);
        command.Parameters.AddWithValue("$payloadFormat", request.Format.ToString());
        command.Parameters.AddWithValue("$originalFileName", request.OriginalFileName);
        command.Parameters.Add("$payload", SqliteType.Blob).Value = request.Payload;
        command.Parameters.AddWithValue("$maxAttempts", request.MaxAttempts);
        command.Parameters.AddWithValue("$createdAt", FormatTimestamp(request.CreatedAt));

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (SqliteException exception) when (exception.SqliteErrorCode == SqliteConstraintError)
        {
            existing = await FindByIdempotencyKeyAsync(connection, request.IdempotencyKey, cancellationToken);
            if (existing is null)
            {
                throw;
            }
            return new CreateIngestionResult(
                string.Equals(existing.PayloadHash, request.PayloadHash, StringComparison.Ordinal)
                    ? CreateIngestionOutcome.Replayed
                    : CreateIngestionOutcome.Conflict,
                existing.Job);
        }

        IngestionJob job = new(
            jobId,
            IngestionStatus.Queued,
            request.Format,
            request.OriginalFileName,
            0,
            request.MaxAttempts,
            request.CreatedAt,
            request.CreatedAt,
            null,
            null,
            []);
        return new CreateIngestionResult(CreateIngestionOutcome.Created, job);
    }

    public async Task<IngestionJob?> GetIngestionAsync(Guid jobId, CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT {JobColumns} FROM ingestion_jobs WHERE id = $id;";
        command.Parameters.AddWithValue("$id", jobId.ToString("D"));
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<ClaimedIngestionJob?> ClaimNextIngestionAsync(
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        using SqliteTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        Guid id;
        PayloadFormat format;
        byte[] payload;
        int attemptCount;
        int maxAttempts;
        string priorStatus;

        await using (SqliteCommand select = connection.CreateCommand())
        {
            select.Transaction = transaction;
            select.CommandText = """
                SELECT id, payload_format, payload, attempt_count, max_attempts, status
                FROM ingestion_jobs
                WHERE payload IS NOT NULL
                  AND (
                    status = 'Queued'
                    OR (status = 'RetryScheduled' AND next_attempt_at <= $now)
                  )
                ORDER BY created_at, id
                LIMIT 1;
                """;
            select.Parameters.AddWithValue("$now", FormatTimestamp(now));
            await using SqliteDataReader reader = await select.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                transaction.Rollback();
                return null;
            }

            id = Guid.Parse(reader.GetString(0));
            format = Enum.Parse<PayloadFormat>(reader.GetString(1), ignoreCase: false);
            payload = (byte[])reader[2];
            attemptCount = reader.GetInt32(3);
            maxAttempts = reader.GetInt32(4);
            priorStatus = reader.GetString(5);
        }

        await using (SqliteCommand update = connection.CreateCommand())
        {
            update.Transaction = transaction;
            update.CommandText = """
                UPDATE ingestion_jobs
                SET status = 'Processing',
                    attempt_count = attempt_count + 1,
                    next_attempt_at = NULL,
                    updated_at = $now
                WHERE id = $id AND status = $priorStatus;
                """;
            update.Parameters.AddWithValue("$now", FormatTimestamp(now));
            update.Parameters.AddWithValue("$id", id.ToString("D"));
            update.Parameters.AddWithValue("$priorStatus", priorStatus);
            int updated = await update.ExecuteNonQueryAsync(cancellationToken);
            if (updated != 1)
            {
                transaction.Rollback();
                return null;
            }
        }

        transaction.Commit();
        return new ClaimedIngestionJob(id, format, payload, attemptCount + 1, maxAttempts);
    }

    public async Task CompleteIngestionAsync(
        ClaimedIngestionJob job,
        IReadOnlyList<ParsedCatalogItem> items,
        DateTimeOffset completedAt,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        using SqliteTransaction transaction = connection.BeginTransaction(IsolationLevel.Serializable);

        foreach (ParsedCatalogItem item in items)
        {
            await using SqliteCommand upsert = connection.CreateCommand();
            upsert.Transaction = transaction;
            upsert.CommandText = """
                INSERT INTO catalog_items (
                    sku, name, price_minor, currency, stock, source_job_id, updated_at
                ) VALUES (
                    $sku, $name, $priceMinor, $currency, $stock, $sourceJobId, $updatedAt
                )
                ON CONFLICT(sku) DO UPDATE SET
                    name = excluded.name,
                    price_minor = excluded.price_minor,
                    currency = excluded.currency,
                    stock = excluded.stock,
                    source_job_id = excluded.source_job_id,
                    updated_at = excluded.updated_at;
                """;
            upsert.Parameters.AddWithValue("$sku", item.Sku);
            upsert.Parameters.AddWithValue("$name", item.Name);
            upsert.Parameters.AddWithValue("$priceMinor", item.PriceMinor);
            upsert.Parameters.AddWithValue("$currency", item.Currency);
            upsert.Parameters.AddWithValue("$stock", item.Stock);
            upsert.Parameters.AddWithValue("$sourceJobId", job.Id.ToString("D"));
            upsert.Parameters.AddWithValue("$updatedAt", FormatTimestamp(completedAt));
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using SqliteCommand complete = connection.CreateCommand();
        complete.Transaction = transaction;
        complete.CommandText = """
            UPDATE ingestion_jobs
            SET status = 'Completed',
                payload = NULL,
                next_attempt_at = NULL,
                updated_at = $completedAt,
                last_error = NULL,
                validation_errors_json = '[]'
            WHERE id = $id AND status = 'Processing';
            """;
        complete.Parameters.AddWithValue("$completedAt", FormatTimestamp(completedAt));
        complete.Parameters.AddWithValue("$id", job.Id.ToString("D"));
        if (await complete.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            transaction.Rollback();
            throw new InvalidOperationException($"Ingestion job {job.Id} was not in Processing state.");
        }

        transaction.Commit();
    }

    public async Task<IngestionStatus> FailIngestionAsync(
        ClaimedIngestionJob job,
        string error,
        IReadOnlyList<ValidationIssue> issues,
        bool retryable,
        DateTimeOffset? retryAt,
        DateTimeOffset failedAt,
        CancellationToken cancellationToken)
    {
        bool shouldRetry = retryable && job.AttemptCount < job.MaxAttempts;
        IngestionStatus status = shouldRetry
            ? IngestionStatus.RetryScheduled
            : IngestionStatus.DeadLettered;

        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            UPDATE ingestion_jobs
            SET status = $status,
                next_attempt_at = $nextAttemptAt,
                updated_at = $failedAt,
                last_error = $lastError,
                validation_errors_json = $validationErrors
            WHERE id = $id AND status = 'Processing';
            """;
        command.Parameters.AddWithValue("$status", status.ToString());
        command.Parameters.AddWithValue(
            "$nextAttemptAt",
            shouldRetry && retryAt.HasValue ? FormatTimestamp(retryAt.Value) : DBNull.Value);
        command.Parameters.AddWithValue("$failedAt", FormatTimestamp(failedAt));
        command.Parameters.AddWithValue("$lastError", Truncate(error, MaxStoredErrorLength));
        command.Parameters.AddWithValue("$validationErrors", JsonSerializer.Serialize(issues, JsonOptions));
        command.Parameters.AddWithValue("$id", job.Id.ToString("D"));
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
        {
            throw new InvalidOperationException($"Ingestion job {job.Id} was not in Processing state.");
        }

        return status;
    }

    public async Task<RequeueOutcome> RequeueDeadLetterAsync(
        Guid jobId,
        DateTimeOffset now,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand update = connection.CreateCommand();
        update.CommandText = """
            UPDATE ingestion_jobs
            SET status = 'Queued',
                attempt_count = 0,
                next_attempt_at = NULL,
                updated_at = $now,
                last_error = NULL,
                validation_errors_json = '[]'
            WHERE id = $id AND status = 'DeadLettered' AND payload IS NOT NULL;
            """;
        update.Parameters.AddWithValue("$now", FormatTimestamp(now));
        update.Parameters.AddWithValue("$id", jobId.ToString("D"));
        if (await update.ExecuteNonQueryAsync(cancellationToken) == 1)
        {
            return RequeueOutcome.Requeued;
        }

        await using SqliteCommand exists = connection.CreateCommand();
        exists.CommandText = "SELECT status FROM ingestion_jobs WHERE id = $id;";
        exists.Parameters.AddWithValue("$id", jobId.ToString("D"));
        object? value = await exists.ExecuteScalarAsync(cancellationToken);
        return value is null ? RequeueOutcome.NotFound : RequeueOutcome.NotDeadLettered;
    }

    public async Task<IReadOnlyList<IngestionJob>> ListDeadLettersAsync(
        int limit,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT {JobColumns}
            FROM ingestion_jobs
            WHERE status = 'DeadLettered'
            ORDER BY updated_at DESC
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$limit", limit);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        List<IngestionJob> jobs = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            jobs.Add(ReadJob(reader));
        }

        return jobs;
    }

    public async Task<CatalogItem?> GetCatalogItemAsync(string sku, CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT sku, name, price_minor, currency, stock, source_job_id, updated_at
            FROM catalog_items
            WHERE sku = $sku COLLATE NOCASE;
            """;
        command.Parameters.AddWithValue("$sku", sku);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadCatalogItem(reader) : null;
    }

    public async Task<IReadOnlyList<CatalogItem>> ListCatalogItemsAsync(
        int limit,
        string? afterSku,
        CancellationToken cancellationToken)
    {
        await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = """
            SELECT sku, name, price_minor, currency, stock, source_job_id, updated_at
            FROM catalog_items
            WHERE ($afterSku IS NULL OR sku > $afterSku COLLATE NOCASE)
            ORDER BY sku COLLATE NOCASE
            LIMIT $limit;
            """;
        command.Parameters.AddWithValue("$afterSku", afterSku is null ? DBNull.Value : afterSku);
        command.Parameters.AddWithValue("$limit", limit);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        List<CatalogItem> items = [];
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(ReadCatalogItem(reader));
        }

        return items;
    }

    public async Task<bool> CanConnectAsync(CancellationToken cancellationToken)
    {
        try
        {
            await using SqliteConnection connection = await OpenConnectionAsync(cancellationToken);
            await using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT 1;";
            return Convert.ToInt32(
                await command.ExecuteScalarAsync(cancellationToken),
                CultureInfo.InvariantCulture) == 1;
        }
        catch (SqliteException)
        {
            return false;
        }
    }

    private const string JobColumns = """
        id, status, payload_format, original_file_name, attempt_count, max_attempts,
        created_at, updated_at, next_attempt_at, last_error, validation_errors_json
        """;

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        SqliteConnection connection = new(_connectionString);
        await connection.OpenAsync(cancellationToken);
        await using SqliteCommand pragma = connection.CreateCommand();
        pragma.CommandText = "PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000; PRAGMA synchronous = FULL;";
        await pragma.ExecuteNonQueryAsync(cancellationToken);
        return connection;
    }

    private async Task<IngestionJobWithHash?> FindByIdempotencyKeyAsync(
        SqliteConnection connection,
        string idempotencyKey,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"""
            SELECT {JobColumns}, payload_hash
            FROM ingestion_jobs
            WHERE idempotency_key = $idempotencyKey;
            """;
        command.Parameters.AddWithValue("$idempotencyKey", idempotencyKey);
        await using SqliteDataReader reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new IngestionJobWithHash(ReadJob(reader), reader.GetString(11));
    }

    private static IngestionJob ReadJob(SqliteDataReader reader)
    {
        string validationJson = reader.GetString(10);
        IReadOnlyList<ValidationIssue> issues = JsonSerializer.Deserialize<List<ValidationIssue>>(
            validationJson,
            JsonOptions) ?? [];
        return new IngestionJob(
            Guid.Parse(reader.GetString(0)),
            Enum.Parse<IngestionStatus>(reader.GetString(1), ignoreCase: false),
            Enum.Parse<PayloadFormat>(reader.GetString(2), ignoreCase: false),
            reader.GetString(3),
            reader.GetInt32(4),
            reader.GetInt32(5),
            ParseTimestamp(reader.GetString(6)),
            ParseTimestamp(reader.GetString(7)),
            reader.IsDBNull(8) ? null : ParseTimestamp(reader.GetString(8)),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            issues);
    }

    private static CatalogItem ReadCatalogItem(SqliteDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetInt64(2),
        reader.GetString(3),
        reader.GetInt32(4),
        Guid.Parse(reader.GetString(5)),
        ParseTimestamp(reader.GetString(6)));

    private void EnsureDatabaseDirectoryExists()
    {
        SqliteConnectionStringBuilder builder = new(_connectionString);
        if (string.IsNullOrWhiteSpace(builder.DataSource)
            || string.Equals(builder.DataSource, ":memory:", StringComparison.OrdinalIgnoreCase)
            || builder.DataSource.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        string fullPath = Path.GetFullPath(builder.DataSource);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
    }

    private static string FormatTimestamp(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    private static DateTimeOffset ParseTimestamp(string value) =>
        DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];

    private sealed record IngestionJobWithHash(IngestionJob Job, string PayloadHash);
}
