using CatalogIngestion.Api.Configuration;
using Microsoft.Extensions.Options;

namespace CatalogIngestion.Api.Ingestion;

public sealed class IngestionWorker(
    IngestionProcessor processor,
    IOptions<IngestionOptions> options,
    ILogger<IngestionWorker> logger) : BackgroundService
{
    private readonly TimeSpan _pollInterval = TimeSpan.FromMilliseconds(options.Value.PollIntervalMilliseconds);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Catalog ingestion worker started");
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                bool processed = await processor.ProcessNextAsync(stoppingToken);
                if (!processed)
                {
                    await Task.Delay(_pollInterval, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal host shutdown.
        }
        finally
        {
            logger.LogInformation("Catalog ingestion worker stopped");
        }
    }
}
