//! Pure orchestration logic for `flux`: job model, state machine, retry policy,
//! idempotency, and dead-letter rules. No I/O, no async — fully unit-testable.

pub mod dead_letter;
pub mod error;
pub mod idempotency;
pub mod job;
pub mod retry;
pub mod state_machine;

pub use dead_letter::{FailureClass, classify, should_dead_letter};
pub use error::CoreError;
pub use idempotency::DedupSet;
pub use job::{Job, JobId, JobState};
pub use retry::RetryPolicy;
pub use state_machine::{JobEvent, Transition, transition};
