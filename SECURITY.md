# Security policy

## Reporting a vulnerability

Please use GitHub's **Report a vulnerability** feature on the repository Security tab. Do not include exploit details in a public issue. Include affected version or commit, reproduction steps, impact, and any suggested mitigation. Reports will be acknowledged as soon as practical.

## Supported version

Security fixes target the current default branch. This portfolio project does not maintain older release lines.

## Trust boundaries

Uploads and all headers are untrusted. The service:

- caps HTTP body size, uploaded bytes, parsed rows, field lengths, list limits, and request rate;
- accepts only bounded CSV and JSON data and never executes uploaded content;
- rejects unknown schema fields and normalizes validated values before persistence;
- strips directory components from client filenames;
- stores money as checked integer minor units;
- returns generic parser/processing errors while retaining detailed exceptions only in structured server logs;
- emits or propagates only syntactically valid correlation IDs;
- uses parameterized SQL throughout;
- runs as a non-root user in the supplied container.

The sample does **not** implement end-user authentication or authorization. Deploy it behind an authenticated gateway, terminate TLS at a trusted edge, restrict dead-letter/requeue routes to operators, and protect the data volume with platform access controls and backups. Do not use the SQLite volume on a network filesystem or mount it into multiple replicas.

No credentials are required by the application. Do not commit secrets in configuration; use the deployment platform's secret provider for any future exporter, gateway, or database credentials.

## Dependency hygiene

CI restores with NuGet auditing enabled by the SDK and checks direct and transitive packages for known vulnerabilities. Dependabot is configured for NuGet, GitHub Actions, and Docker updates. Findings still require human review: a clean advisory scan is not proof of security.
