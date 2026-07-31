# ADR 0003: Apply each valid file atomically

- Status: Accepted
- Date: 2026-07-30

## Decision

Parse and validate the full bounded payload before opening the catalog write transaction. Upsert every validated item and mark its job `Completed` in the same transaction.

## Rationale

Partial catalog updates are harder to reason about and recover than a rejected batch. Validation outside the transaction keeps the SQLite writer lock short; the row cap bounds memory and transaction duration.

## Consequences

- Invalid files create zero catalog changes.
- A database failure rolls back both item changes and job completion.
- The current per-row upserts fit the 10,000-row local sample cap; a larger implementation should stage and merge rows set-wise.
