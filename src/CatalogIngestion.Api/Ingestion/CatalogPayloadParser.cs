using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using CatalogIngestion.Api.Domain;
using CsvHelper;
using CsvHelper.Configuration;

namespace CatalogIngestion.Api.Ingestion;

public interface ICatalogPayloadParser
{
    ParseResult Parse(PayloadFormat format, ReadOnlyMemory<byte> payload, int maxItems);
}

public sealed partial class CatalogPayloadParser : ICatalogPayloadParser
{
    private static readonly string[] RequiredCsvHeaders = ["sku", "name", "price", "currency", "stock"];

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        AllowTrailingCommas = false,
        MaxDepth = 32,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Disallow,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    public ParseResult Parse(PayloadFormat format, ReadOnlyMemory<byte> payload, int maxItems) => format switch
    {
        PayloadFormat.Csv => ParseCsv(payload, maxItems),
        PayloadFormat.Json => ParseJson(payload, maxItems),
        _ => throw new ArgumentOutOfRangeException(nameof(format), format, "Unsupported payload format."),
    };

    private static ParseResult ParseJson(ReadOnlyMemory<byte> payload, int maxItems)
    {
        List<CatalogItemDocument>? documents;
        try
        {
            documents = JsonSerializer.Deserialize<List<CatalogItemDocument>>(payload.Span, JsonOptions);
        }
        catch (JsonException)
        {
            return RejectedDocument("Invalid JSON structure.");
        }

        if (documents is null)
        {
            return RejectedDocument("The JSON body must be an array of catalog items.");
        }

        return ValidateDocuments(documents, maxItems);
    }

    private static ParseResult ParseCsv(ReadOnlyMemory<byte> payload, int maxItems)
    {
        try
        {
            using MemoryStream stream = new(payload.ToArray(), writable: false);
            using StreamReader reader = new(
                stream,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
                detectEncodingFromByteOrderMarks: true);
            using CsvReader csv = new(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                BadDataFound = null,
                DetectDelimiter = false,
                HasHeaderRecord = true,
                IgnoreBlankLines = true,
                MissingFieldFound = null,
                TrimOptions = TrimOptions.Trim,
            });

            if (!csv.Read() || !csv.ReadHeader() || csv.HeaderRecord is null)
            {
                return RejectedDocument("The CSV document must include a header row.");
            }

            string[] normalizedHeaders = csv.HeaderRecord
                .Select(header => header.Trim().ToLowerInvariant())
                .ToArray();
            if (normalizedHeaders.Distinct(StringComparer.Ordinal).Count() != normalizedHeaders.Length)
            {
                return RejectedDocument("CSV headers must be unique.");
            }

            string[] missingHeaders = RequiredCsvHeaders.Except(normalizedHeaders, StringComparer.Ordinal).ToArray();
            string[] extraHeaders = normalizedHeaders.Except(RequiredCsvHeaders, StringComparer.Ordinal).ToArray();
            if (missingHeaders.Length > 0 || extraHeaders.Length > 0)
            {
                string message = $"CSV headers must be exactly: {string.Join(", ", RequiredCsvHeaders)}.";
                return RejectedDocument(message);
            }

            Dictionary<string, int> indexes = normalizedHeaders
                .Select((header, index) => new KeyValuePair<string, int>(header, index))
                .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);

            List<CatalogItemDocument> documents = [];
            while (csv.Read())
            {
                if (documents.Count >= maxItems + 1)
                {
                    break;
                }

                string? priceText = csv.GetField(indexes["price"]);
                string? stockText = csv.GetField(indexes["stock"]);
                documents.Add(new CatalogItemDocument
                {
                    Sku = csv.GetField(indexes["sku"]),
                    Name = csv.GetField(indexes["name"]),
                    Price = decimal.TryParse(
                        priceText,
                        NumberStyles.Number,
                        CultureInfo.InvariantCulture,
                        out decimal price) ? price : null,
                    Currency = csv.GetField(indexes["currency"]),
                    Stock = int.TryParse(
                        stockText,
                        NumberStyles.Integer,
                        CultureInfo.InvariantCulture,
                        out int stock) ? stock : null,
                });
            }

            return ValidateDocuments(documents, maxItems);
        }
        catch (DecoderFallbackException)
        {
            return RejectedDocument("CSV input must be valid UTF-8.");
        }
        catch (CsvHelperException)
        {
            return RejectedDocument("Invalid CSV structure.");
        }
    }

    private static ParseResult ValidateDocuments(IReadOnlyList<CatalogItemDocument> documents, int maxItems)
    {
        List<ValidationIssue> issues = [];
        List<ParsedCatalogItem> items = [];
        HashSet<string> seenSkus = new(StringComparer.OrdinalIgnoreCase);

        if (documents.Count == 0)
        {
            issues.Add(new ValidationIssue(null, "document", "At least one catalog item is required."));
        }

        if (documents.Count > maxItems)
        {
            issues.Add(new ValidationIssue(null, "document", $"A payload may contain at most {maxItems} items."));
            return new ParseResult(items, issues);
        }

        for (int index = 0; index < documents.Count; index++)
        {
            int row = index + 1;
            if (!TryNormalize(documents[index], row, issues, out ParsedCatalogItem? item) || item is null)
            {
                continue;
            }

            if (!seenSkus.Add(item.Sku))
            {
                issues.Add(new ValidationIssue(row, "sku", "SKU values must be unique within a payload."));
                continue;
            }

            items.Add(item);
        }

        return new ParseResult(items, issues);
    }

    private static bool TryNormalize(
        CatalogItemDocument document,
        int row,
        ICollection<ValidationIssue> issues,
        out ParsedCatalogItem? item)
    {
        int originalIssueCount = issues.Count;
        string sku = document.Sku?.Trim() ?? string.Empty;
        string name = document.Name?.Trim() ?? string.Empty;
        string currency = document.Currency?.Trim().ToUpperInvariant() ?? string.Empty;

        if (sku.Length is < 1 or > 64 || !SkuRegex().IsMatch(sku))
        {
            issues.Add(new ValidationIssue(row, "sku", "SKU must be 1-64 characters using letters, numbers, '.', '_' or '-'."));
        }

        if (name.Length is < 1 or > 200)
        {
            issues.Add(new ValidationIssue(row, "name", "Name must be 1-200 characters."));
        }

        if (!document.Price.HasValue || document.Price < 0m || document.Price > 1_000_000_000m)
        {
            issues.Add(new ValidationIssue(row, "price", "Price must be a number from 0 through 1,000,000,000."));
        }
        else if (decimal.Round(document.Price.Value, 2, MidpointRounding.ToEven) != document.Price.Value)
        {
            issues.Add(new ValidationIssue(row, "price", "Price may contain at most two fractional digits."));
        }

        if (!CurrencyRegex().IsMatch(currency))
        {
            issues.Add(new ValidationIssue(row, "currency", "Currency must be a three-letter code."));
        }

        if (!document.Stock.HasValue || document.Stock < 0 || document.Stock > 1_000_000)
        {
            issues.Add(new ValidationIssue(row, "stock", "Stock must be an integer from 0 through 1,000,000."));
        }

        if (issues.Count != originalIssueCount)
        {
            item = null;
            return false;
        }

        item = new ParsedCatalogItem(
            sku.ToUpperInvariant(),
            name,
            checked((long)(document.Price!.Value * 100m)),
            currency,
            document.Stock!.Value);
        return true;
    }

    private static ParseResult RejectedDocument(string message) => new(
        [],
        [new ValidationIssue(null, "document", message)]);

    [GeneratedRegex("^[A-Za-z0-9._-]+$", RegexOptions.CultureInvariant)]
    private static partial Regex SkuRegex();

    [GeneratedRegex("^[A-Z]{3}$", RegexOptions.CultureInvariant)]
    private static partial Regex CurrencyRegex();
}
