# ADR 0001: Use SQLite for local durable job state

- Status: Accepted
- Date: 2026-07-30

## Decision

Use SQLite in WAL mode as both the catalog store and the durable job-state store, with the API and one hosted worker in the same process.

## Rationale

The sample needs crash recovery, transactions, idempotency constraints, and a zero-infrastructure local experience. SQLite supplies those properties in one inspectable file. Keeping the worker in-process makes the single-writer operating boundary obvious and makes Docker persistence straightforward.

## Consequences

- Submission state survives process and container restarts when the data directory is persisted.
- Catalog upserts and successful job completion can commit atomically.
- Dead-letter metadata is durable, while a configurable cap bounds how many failed payload BLOBs remain requeueable.
- Horizontal replicas, shared/network filesystems, and high write concurrency are unsupported.
- A production scale-out would replace `ICatalogStore` with PostgreSQL or a managed queue/database combination and add leases plus fencing.
