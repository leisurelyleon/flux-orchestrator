# flux-orchestrator

> A fault-tolerant distributed job orchestrator with effectively-once processing.

`flux-orchestrator` reliably executes distributed jobs in the face of failure.
It models each job as a state machine, delivers work over a pluggable event bus,
retries transient failures with exponential backoff, routes exhausted jobs to a
dead-letter channel, and deduplicates by idempotency key so that a job's effect
is applied exactly once even when it is delivered more than once.

## The Problem

Distributed work is delivered over channels that can duplicate, reorder, or drop
messages, and workers can crash mid-job. True "exactly-once delivery" is
impossible; the practical goal is **effectively-once**: at-least-once delivery
combined with idempotent processing. `flux-orchestrator` implements that
combination and proves it with a chaos-test harness.

## Architecture

```
flux-core      pure logic: job state machine, retry, idempotency, dead-letter (no I/O)
flux-bus       the EventBus abstraction + in-memory and (optional) Kafka backends
flux-engine    orchestration: consumer loop, idempotent worker, dedup store, telemetry
flux-cli       the binary: submit jobs, run workers, run the demo
chaos          a fault-injecting harness proving no job loss under failure
```

The `EventBus` trait is the keystone: core logic depends on the abstraction, not
on Kafka. The in-memory bus runs the entire system — including tests and the
local demo — with no broker. The Kafka backend is feature-gated and optional.

## Build & Test

```bash
cargo build
cargo test          # runs entirely on the in-memory bus; no broker required
```

## Run the demo (no Kafka needed)

```bash
cargo run -p flux-cli -- demo
```

## Optional: live Kafka backend

```bash
docker compose up -d                       # start a single-node broker
cargo run -p flux-cli --features kafka -- run-worker --bus kafka
```

## License

MIT — see [LICENSE](LICENSE).
