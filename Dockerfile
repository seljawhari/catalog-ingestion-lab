# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

COPY global.json Directory.Build.props CatalogIngestionLab.slnx ./
COPY src/CatalogIngestion.Api/CatalogIngestion.Api.csproj src/CatalogIngestion.Api/
RUN dotnet restore src/CatalogIngestion.Api/CatalogIngestion.Api.csproj

COPY src/CatalogIngestion.Api/ src/CatalogIngestion.Api/
RUN dotnet publish src/CatalogIngestion.Api/CatalogIngestion.Api.csproj \
    --configuration Release \
    --no-restore \
    --output /out \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

ENV ASPNETCORE_HTTP_PORTS=8080 \
    DOTNET_EnableDiagnostics=0 \
    ConnectionStrings__Catalog="Data Source=/app/data/catalog.db;Mode=ReadWriteCreate;Foreign Keys=True;Default Timeout=5;Pooling=True"

COPY --from=build --chown=app:app /out ./
RUN mkdir -p /app/data && chown app:app /app/data

USER app
EXPOSE 8080
VOLUME ["/app/data"]
ENTRYPOINT ["dotnet", "CatalogIngestion.Api.dll"]
