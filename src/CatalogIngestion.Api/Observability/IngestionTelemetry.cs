using System.Diagnostics;
using System.Diagnostics.Metrics;
using CatalogIngestion.Api.Domain;

namespace CatalogIngestion.Api.Observability;

public sealed class IngestionTelemetry : IDisposable
{
    public const string InstrumentationName = "CatalogIngestionLab.Ingestion";

    private readonly ActivitySource _activitySource = new(InstrumentationName);
    private readonly Meter _meter = new(InstrumentationName);
    private readonly Counter<long> _submissions;
    private readonly Counter<long> _replays;
    private readonly Counter<long> _completed;
    private readonly Counter<long> _retries;
    private readonly Counter<long> _deadLetters;
    private readonly Histogram<double> _duration;

    public IngestionTelemetry()
    {
        _submissions = _meter.CreateCounter<long>("catalog.ingestion.submissions");
        _replays = _meter.CreateCounter<long>("catalog.ingestion.idempotent_replays");
        _completed = _meter.CreateCounter<long>("catalog.ingestion.completed");
        _retries = _meter.CreateCounter<long>("catalog.ingestion.retries_scheduled");
        _deadLetters = _meter.CreateCounter<long>("catalog.ingestion.dead_lettered");
        _duration = _meter.CreateHistogram<double>("catalog.ingestion.duration", unit: "ms");
    }

    public Activity? StartProcessing(Guid jobId, int attempt)
    {
        Activity? activity = _activitySource.StartActivity("catalog.ingestion.process", ActivityKind.Internal);
        activity?.SetTag("catalog.job.id", jobId.ToString("D"));
        activity?.SetTag("catalog.job.attempt", attempt);
        return activity;
    }

    public void RecordSubmission(PayloadFormat format) =>
        _submissions.Add(1, new KeyValuePair<string, object?>("catalog.payload.format", format.ToString()));

    public void RecordReplay() => _replays.Add(1);

    public void RecordCompletion(double elapsedMilliseconds, int itemCount)
    {
        _completed.Add(1, new KeyValuePair<string, object?>("catalog.item.count", itemCount));
        _duration.Record(elapsedMilliseconds, new KeyValuePair<string, object?>("catalog.job.outcome", "completed"));
    }

    public void RecordFailure(IngestionStatus status, double elapsedMilliseconds)
    {
        if (status == IngestionStatus.RetryScheduled)
        {
            _retries.Add(1);
        }
        else if (status == IngestionStatus.DeadLettered)
        {
            _deadLetters.Add(1);
        }

        _duration.Record(elapsedMilliseconds, new KeyValuePair<string, object?>("catalog.job.outcome", status.ToString()));
    }

    public void Dispose()
    {
        _activitySource.Dispose();
        _meter.Dispose();
    }
}
