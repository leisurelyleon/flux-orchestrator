# 1. EventBus abstraction over a concrete broker

- Status: Accepted
- Date: 2026-05

## Context

Coupling orchestration logic directly to a broker (e.g. Kafka) makes the system
impossible to test without infrastructure and hard to evolve.

## Decision

Define an `EventBus` trait with at-least-once semantics. Provide a complete
in-memory implementation for tests and local runs, and a feature-gated Kafka
implementation for production. Orchestration depends only on the trait.

## Consequences

- The entire system is testable with no external broker.
- The chaos harness can wrap any bus to inject faults.
- Adding a new transport is an additive change, not a rewrite.
