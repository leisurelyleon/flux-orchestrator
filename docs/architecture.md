# Architecture

`flux-orchestrator` is a Rust workspace split so that all orchestration *logic*
is pure and testable, while message transport is an abstraction with swappable
backends.

## Crates

```text
flux-core pure logic: job state machine, retry, idempotency, dead-letter (no I/O, no async) flux-bus the EventBus trait + in-memory backend + optional Kafka backend flux-engine orchestration: processing step, idempotent worker, dedup store, telemetry flux-cli the flux binary flux-chaos fault-injection harness proving no job loss under failure
```

## The EventBus seam

The orchestrator depends on the `EventBus` trait, never on a concrete broker.
`InMemoryBus` is a complete at-least-once implementation that powers all tests,
the chaos harness, and the local demo. `KafkaBus` is a feature-gated,
production-style backend. Swapping backends changes no orchestration logic.

## Processing model

Each `step` polls one delivery, deduplicates by idempotency key, runs the
handler, and then either acknowledges (success), re-publishes (transient
failure, within retry budget), or routes to a dead-letter topic (permanent
failure or exhausted retries). The dedup store makes reprocessing safe.

## Observability

`flux-engine` initializes a tracing subscriber; job lifecycle events are emitted
as structured `tracing` spans/events, suitable for export to an OpenTelemetry
collector in a production deployment.
