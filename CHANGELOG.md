# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial workspace scaffold: flux-core, flux-bus, flux-engine, flux-cli, chaos.

## [0.1.0] - TBD

### Added
- Pure job lifecycle state machine with retry and dead-letter policies.
- Pluggable EventBus abstraction with in-memory and Kafka backends.
- Idempotent worker processing for effectively-once semantics.
- Chaos-test harness proving no job loss under injected failures.
- OpenTelemetry-style tracing and metrics.

[Unreleased]: https://github.com/leisurelyleon/flux-orchestrator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/leisurelyleon/flux-orchestrator/releases/tag/v0.1.0
