//! Orchestration for `flux`: the processing step, idempotent worker handlers,
//! the dedup store, and telemetry setup. Tested entirely against the in-memory
//! bus — no broker required.

pub mod dedup_store;
pub mod error;
pub mod orchestrator;
pub mod telemetry;
pub mod worker;

pub use dedup_store::DedupStore;
pub use error::{EngineError, EngineResult};
pub use orchestrator::{Orchestrator, StepOutcome};
pub use worker::{EchoHandler, FlakyHandler, JobHandler};
