using System.Text;
using CatalogIngestion.Api.Domain;
using CatalogIngestion.Api.Ingestion;

namespace CatalogIngestion.Tests;

public sealed class CatalogPayloadParserTests
{
    private readonly CatalogPayloadParser _parser = new();

    [Fact]
    public void ParseCsv_AcceptsQuotedFieldsAndNormalizesIdentifiers()
    {
        byte[] payload = Encoding.UTF8.GetBytes("""
            sku,name,price,currency,stock
            widget-01,"Widget, Deluxe",12.34,usd,7
            """
        );

        ParseResult result = _parser.Parse(PayloadFormat.Csv, payload, maxItems: 10);

        ParsedCatalogItem item = Assert.Single(result.Items);
        Assert.Empty(result.Issues);
        Assert.Equal("WIDGET-01", item.Sku);
        Assert.Equal("Widget, Deluxe", item.Name);
        Assert.Equal(1234, item.PriceMinor);
        Assert.Equal("USD", item.Currency);
        Assert.Equal(7, item.Stock);
    }

    [Fact]
    public void ParseJson_RejectsUnknownFieldsAndMalformedDocuments()
    {
        byte[] payload = Encoding.UTF8.GetBytes("""
            [{"sku":"W-1","name":"Widget","price":10.00,"currency":"USD","stock":2,"unexpected":true}]
            """
        );

        ParseResult result = _parser.Parse(PayloadFormat.Json, payload, maxItems: 10);

        Assert.Empty(result.Items);
        ValidationIssue issue = Assert.Single(result.Issues);
        Assert.Equal("document", issue.Field);
    }

    [Fact]
    public void ParseJson_RejectsDuplicateSkusAndInvalidMoney()
    {
        byte[] payload = Encoding.UTF8.GetBytes("""
            [
              {"sku":"widget-1","name":"Widget","price":1.99,"currency":"usd","stock":2},
              {"sku":"WIDGET-1","name":"Duplicate","price":2.00,"currency":"USD","stock":3},
              {"sku":"widget-2","name":"Bad price","price":1.999,"currency":"USD","stock":1}
            ]
            """
        );

        ParseResult result = _parser.Parse(PayloadFormat.Json, payload, maxItems: 10);

        Assert.Equal(2, result.Issues.Count);
        Assert.Contains(result.Issues, issue => issue.Field == "price");
        Assert.Contains(result.Issues, issue => issue.Field == "sku");
    }

    [Fact]
    public void ParseCsv_RejectsUnexpectedHeadersAndTooManyRows()
    {
        ParseResult wrongHeaders = _parser.Parse(
            PayloadFormat.Csv,
            Encoding.UTF8.GetBytes("sku,name,price,currency,stock,secret\nA,A,1.00,USD,1,nope"),
            maxItems: 10);
        ParseResult tooManyRows = _parser.Parse(
            PayloadFormat.Csv,
            Encoding.UTF8.GetBytes("sku,name,price,currency,stock\nA,A,1.00,USD,1\nB,B,2.00,USD,2"),
            maxItems: 1);

        Assert.Contains(wrongHeaders.Issues, issue => issue.Field == "document");
        Assert.Contains(tooManyRows.Issues, issue => issue.Message.Contains("at most 1", StringComparison.Ordinal));
    }
}
