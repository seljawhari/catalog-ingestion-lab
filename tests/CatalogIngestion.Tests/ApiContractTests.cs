using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using CatalogIngestion.Api.Contracts;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace CatalogIngestion.Tests;

public sealed class ApiContractTests : IAsyncLifetime
{
    private string _directory = null!;
    private CatalogApiFactory _factory = null!;
    private HttpClient _client = null!;

    public Task InitializeAsync()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"catalog-api-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
        _factory = new CatalogApiFactory(Path.Combine(_directory, "catalog.db"));
        _client = _factory.CreateClient();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _factory.DisposeAsync();
        Directory.Delete(_directory, recursive: true);
    }

    [Fact]
    public async Task Submit_ReplaysSamePayloadAndRejectsDifferentPayloadForSameKey()
    {
        const string firstPayload = "sku,name,price,currency,stock\nSKU-1,Widget,12.50,USD,4";
        using HttpResponseMessage first = await SubmitAsync("api-batch-001", firstPayload);
        using HttpResponseMessage replay = await SubmitAsync("api-batch-001", firstPayload);
        using HttpResponseMessage conflict = await SubmitAsync(
            "api-batch-001",
            "sku,name,price,currency,stock\nSKU-2,Other,1.00,USD,1");

        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, replay.StatusCode);
        Assert.Equal(HttpStatusCode.Conflict, conflict.StatusCode);
        SubmitIngestionResponse firstBody = (await first.Content.ReadFromJsonAsync<SubmitIngestionResponse>())!;
        SubmitIngestionResponse replayBody = (await replay.Content.ReadFromJsonAsync<SubmitIngestionResponse>())!;
        Assert.False(firstBody.Replayed);
        Assert.True(replayBody.Replayed);
        Assert.Equal(firstBody.JobId, replayBody.JobId);
    }

    [Fact]
    public async Task Submit_RequiresValidIdempotencyKeyAndSupportedFile()
    {
        using MultipartFormDataContent body = new();
        using ByteArrayContent content = new(Encoding.UTF8.GetBytes("not a catalog"));
        content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
        body.Add(content, "file", "catalog.txt");
        using HttpRequestMessage request = new(HttpMethod.Post, "/v1/ingestions") { Content = body };
        request.Headers.Add("Idempotency-Key", "short");

        using HttpResponseMessage response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    private async Task<HttpResponseMessage> SubmitAsync(string key, string payload)
    {
        MultipartFormDataContent body = new();
        ByteArrayContent content = new(Encoding.UTF8.GetBytes(payload));
        content.Headers.ContentType = new MediaTypeHeaderValue("text/csv");
        body.Add(content, "file", "catalog.csv");
        HttpRequestMessage request = new(HttpMethod.Post, "/v1/ingestions") { Content = body };
        request.Headers.Add("Idempotency-Key", key);
        HttpResponseMessage response = await _client.SendAsync(request);
        request.Dispose();
        return response;
    }

    private sealed class CatalogApiFactory(string databasePath) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseEnvironment("Testing");
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["ConnectionStrings:Catalog"] = $"Data Source={databasePath};Default Timeout=5",
                    ["Ingestion:WorkerEnabled"] = "false",
                }));
        }
    }
}
