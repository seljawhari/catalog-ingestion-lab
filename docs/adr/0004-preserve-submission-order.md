# ADR 0004: Preserve accepted submission order

- Status: Accepted
- Date: 2026-07-31

## Decision

Assign every accepted job a durable monotonic sequence in the same transaction as its insert. The single worker may claim only the oldest active sequence. An automatic retry keeps its sequence; a manual requeue receives a fresh one because it represents a new operator decision after the original job became terminal.

## Rationale

Allowing a newer batch to pass an older scheduled retry would let the retry finish later and overwrite newer catalog values. Conditional row updates would avoid that regression but could make one file apply only partially. Ordered dispatch preserves the existing all-or-nothing batch transaction and keeps the rule easy to inspect.

## Consequences

- Newer work can wait behind an older retry, but retry delays and attempts are bounded.
- A transient failure cannot reorder accepted batches or produce stale final catalog values.
- Manual requeue goes to the tail rather than silently reviving the job's old priority.
- A future concurrent-worker design must preserve the sequence with leases and fencing or catalog-row version guards.
