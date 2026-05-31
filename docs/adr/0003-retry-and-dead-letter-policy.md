# 3. Retry and dead-letter policy

- Status: Accepted
- Date: 2026-05

## Context

Failures differ: some are transient (worth retrying), some permanent (retrying
wastes resources and delays surfacing the problem).

## Decision

Classify failures as transient or permanent. Retry transient failures with
capped exponential backoff (plus jitter) up to a maximum attempt count; route
permanent failures, and transient failures that exhaust their budget, to a
dead-letter topic for inspection.

## Consequences

- Transient blips self-heal; permanent failures fail fast and visibly.
- The dead-letter topic preserves failed jobs for diagnosis or replay.
- Backoff with jitter avoids synchronized retry storms.
