# flux-orchestrator
A fault-tolerant distributed job orchestrator in Rust with exactly-once processing semantics. Features idempotent consumers, exponential-backoff retries, dead-letter handling, and backpressure — pluggable over an in-memory bus or Kafka, with OpenTelemetry tracing and a chaos-test harness proving no job loss under failure.
