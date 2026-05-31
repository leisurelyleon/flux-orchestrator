//! Job handlers: the trait the orchestrator calls to perform work, plus simple
//! handlers used by the demo and tests.

use async_trait::async_trait;

use flux_core::Job;

/// Performs the actual work for a job. Implementations should be idempotent;
/// the orchestrator additionally deduplicates by idempotency key.
#[async_trait]
pub trait JobHandler: Send + Sync {
    async fn handle(&self, job: &Job) -> Result<(), String>;
}

/// A handler that always succeeds, logging the job.
pub struct EchoHandler;

#[async_trait]
impl JobHandler for EchoHandler {
    async fn handle(&self, job: &Job) -> Result<(), String> {
        tracing::info!(job_id = %job.id, "processed job");
        Ok(())
    }
}

/// A handler that fails until the job reaches `succeed_at_attempt`, used to
/// exercise retry and dead-letter paths.
pub struct FlakyHandler {
    pub succeed_at_attempt: u32,
}

#[async_trait]
impl JobHandler for FlakyHandler {
    async fn handle(&self, job: &Job) -> Result<(), String> {
        if job.attempts >= self.succeed_at_attempt {
            Ok(())
        } else {
            Err("transient failure".to_string())
        }
    }
}
