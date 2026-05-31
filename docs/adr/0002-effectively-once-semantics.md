# 2. Effectively-once, not exactly-once

- Status: Accepted
- Date: 2026-05

## Context

"Exactly-once delivery" is not achievable over channels that can fail and
redeliver. Claiming it would be misleading.

## Decision

Combine at-least-once delivery with idempotent processing keyed by an
idempotency key, deduplicated in a store. A redelivered message is recognized
and skipped, so each job's effect is applied exactly once — "effectively-once".

## Consequences

- The guarantee is honest and matches what production systems actually provide.
- Correctness depends on the dedup store, not on the transport, so it holds for
  every backend.
- The chaos harness verifies the guarantee empirically.
