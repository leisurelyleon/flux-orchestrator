# 4. Observability via structured tracing

- Status: Accepted
- Date: 2026-05

## Context

A backend orchestrator is only trustworthy if its behavior is observable: which
jobs ran, retried, or dead-lettered, and why.

## Decision

Use the `tracing` ecosystem to emit structured events across the job lifecycle,
configured through an env filter. In production this exports to an OpenTelemetry
collector; locally it prints structured logs.

## Consequences

- Job lifecycle is observable without code changes, only configuration.
- The same instrumentation serves local debugging and production telemetry.
