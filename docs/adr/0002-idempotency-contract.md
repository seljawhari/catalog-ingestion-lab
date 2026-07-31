# ADR 0002: Bind idempotency keys to payload hashes

- Status: Accepted
- Date: 2026-07-30

## Decision

Require an `Idempotency-Key` on every submission, uniquely constrain it, and bind it to a SHA-256 hash of the exact uploaded bytes.

## Rationale

Clients cannot reliably distinguish a lost `202` response from a failed submission. Replaying the same key and bytes must return the original job; silently accepting a different file under that key would hide a client error.

## Consequences

- Same key and same bytes returns the original job with `replayed: true`.
- Same key and different bytes returns `409 Conflict`.
- Semantically equivalent files with different encoding or whitespace are different payloads by design.
- A database unique constraint is the final concurrency guard; a pre-read only improves the common path.
