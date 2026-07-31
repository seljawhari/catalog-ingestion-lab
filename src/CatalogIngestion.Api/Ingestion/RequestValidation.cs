using CatalogIngestion.Api.Domain;

namespace CatalogIngestion.Api.Ingestion;

public static class RequestValidation
{
    public static bool IsValidIdempotencyKey(string? value) =>
        value is { Length: >= 8 and <= 128 }
        && value.All(character =>
            char.IsAsciiLetterOrDigit(character)
            || character is '-' or '_' or '.' or ':');

    public static bool TryDetectFormat(IFormFile file, out PayloadFormat format)
    {
        string mediaType = file.ContentType.Split(';', 2)[0].Trim().ToLowerInvariant();
        string extension = Path.GetExtension(file.FileName).ToLowerInvariant();

        if (mediaType is "application/json" or "text/json" || extension == ".json")
        {
            format = PayloadFormat.Json;
            return true;
        }

        if (mediaType is "text/csv" or "application/csv" or "application/vnd.ms-excel" || extension == ".csv")
        {
            format = PayloadFormat.Csv;
            return true;
        }

        format = default;
        return false;
    }
}
