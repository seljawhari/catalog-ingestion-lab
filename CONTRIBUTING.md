# Contributing

Small, focused pull requests are welcome. Before opening one:

```bash
dotnet format CatalogIngestionLab.slnx --verify-no-changes
dotnet build CatalogIngestionLab.slnx --configuration Release
dotnet test CatalogIngestionLab.slnx --configuration Release
dotnet list CatalogIngestionLab.slnx package --vulnerable --include-transitive
```

Add tests for behavior changes and update the README or an architecture decision record when a public contract or guarantee changes. Never contribute private employer code, non-public schemas, credentials, or real customer data.
