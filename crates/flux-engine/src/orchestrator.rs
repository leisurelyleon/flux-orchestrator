//! Ties the state machine, bus, retry policy, dedup store, and worker together
//! into one processing step.

use std::sync::Arc;

use flux_bus::{Envelope, EventBus};
use flux_core::{Job, JobState, RetryPolicy, classify, should_dead_letter};

use crate::dedup_store::DedupStore;
use crate::error::EngineResult;
use crate::worker::JobHandler;

/// The outcome of a single orchestration step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepOutcome {
    Idle,
    Completed(String),
    Skipped(String),
    Retried(String),
    DeadLettered(String),
}

/// Orchestrates job processing over an `EventBus`.
pub struct Orchestrator {
    bus: Arc<dyn EventBus>,
    handler: Arc<dyn JobHandler>,
    dedup: Arc<DedupStore>,
    retry: RetryPolicy,
    topic: String,
    dead_letter_topic: String,
}

impl Orchestrator {
    pub fn new(
        bus: Arc<dyn EventBus>,
        handler: Arc<dyn JobHandler>,
        retry: RetryPolicy,
        topic: impl Into<String>,
    ) -> Self {
        let topic = topic.into();
        let dead_letter_topic = format!("{topic}.dead-letter");
        Self {
            bus,
            handler,
            dedup: Arc::new(DedupStore::new()),
            retry,
            topic,
            dead_letter_topic,
        }
    }

    /// The dedup store (exposed for inspection).
    pub fn dedup(&self) -> &DedupStore {
        &self.dedup
    }

    /// The dead-letter topic name.
    pub fn dead_letter_topic(&self) -> &str {
        &self.dead_letter_topic
    }

    /// Submits a job for processing.
    pub async fn submit(&self, job: &Job) -> EngineResult<()> {
        let payload = serde_json::to_vec(job)?;
        self.bus
            .publish(&self.topic, Envelope::new(&job.idempotency_key, payload))
            .await?;
        Ok(())
    }

    /// Processes a single delivery, if any is available.
    pub async fn step(&self) -> EngineResult<StepOutcome> {
        let Some(delivery) = self.bus.poll(&self.topic).await? else {
            return Ok(StepOutcome::Idle);
        };

        let mut job: Job = serde_json::from_slice(&delivery.envelope.payload)?;

        // Effectively-once: skip work already done for this idempotency key.
        if self.dedup.contains(&job.idempotency_key) {
            self.bus.ack(&self.topic, delivery.offset).await?;
            return Ok(StepOutcome::Skipped(job.id.to_string()));
        }

        job.attempts += 1;

        match self.handler.handle(&job).await {
            Ok(()) => {
                self.dedup.record(&job.idempotency_key);
                self.bus.ack(&self.topic, delivery.offset).await?;
                Ok(StepOutcome::Completed(job.id.to_string()))
            }
            Err(message) => {
                let class = classify(&message);
                // Consume the current delivery; we re-publish on retry.
                self.bus.ack(&self.topic, delivery.offset).await?;

                if should_dead_letter(job.attempts, self.retry.max_attempts, class) {
                    job.state = JobState::DeadLettered;
                    let payload = serde_json::to_vec(&job)?;
                    self.bus
                        .publish(
                            &self.dead_letter_topic,
                            Envelope::new(&job.idempotency_key, payload),
                        )
                        .await?;
                    Ok(StepOutcome::DeadLettered(job.id.to_string()))
                } else {
                    let payload = serde_json::to_vec(&job)?;
                    self.bus
                        .publish(&self.topic, Envelope::new(&job.idempotency_key, payload))
                        .await?;
                    Ok(StepOutcome::Retried(job.id.to_string()))
                }
            }
        }
    }

    /// Steps repeatedly until the main topic is empty, collecting outcomes.
    pub async fn run_until_idle(&self) -> EngineResult<Vec<StepOutcome>> {
        let mut outcomes = Vec::new();
        loop {
            let outcome = self.step().await?;
            if outcome == StepOutcome::Idle {
                break;
            }
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{EchoHandler, FlakyHandler};
    use flux_bus::InMemoryBus;

    fn job(id: &str, key: &str) -> Job {
        Job::new(id, key, "{}")
    }

    #[tokio::test]
    async fn processes_single_job_to_completion() {
        let bus = Arc::new(InMemoryBus::new());
        let orch = Orchestrator::new(bus, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");
        orch.submit(&job("j1", "k1")).await.unwrap();
        let outcomes = orch.run_until_idle().await.unwrap();
        assert_eq!(outcomes, vec![StepOutcome::Completed("j1".to_string())]);
    }

    #[tokio::test]
    async fn deduplicates_same_idempotency_key() {
        let bus = Arc::new(InMemoryBus::new());
        let orch = Orchestrator::new(bus, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");
        orch.submit(&job("j1", "dup")).await.unwrap();
        orch.submit(&job("j2", "dup")).await.unwrap();
        let outcomes = orch.run_until_idle().await.unwrap();
        assert_eq!(
            outcomes,
            vec![
                StepOutcome::Completed("j1".to_string()),
                StepOutcome::Skipped("j2".to_string()),
            ]
        );
        assert_eq!(orch.dedup().len(), 1);
    }

    #[tokio::test]
    async fn retries_transient_failure_then_succeeds() {
        let bus = Arc::new(InMemoryBus::new());
        let handler = Arc::new(FlakyHandler {
            succeed_at_attempt: 2,
        });
        let orch = Orchestrator::new(bus, handler, RetryPolicy::default(), "jobs");
        orch.submit(&job("j1", "k1")).await.unwrap();
        let outcomes = orch.run_until_idle().await.unwrap();
        assert_eq!(
            outcomes,
            vec![
                StepOutcome::Retried("j1".to_string()),
                StepOutcome::Completed("j1".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn dead_letters_after_exhausting_retries() {
        let bus = Arc::new(InMemoryBus::new());
        let handler = Arc::new(FlakyHandler {
            succeed_at_attempt: 100,
        });
        let policy = RetryPolicy {
            max_attempts: 2,
            ..RetryPolicy::default()
        };
        let orch = Orchestrator::new(bus.clone(), handler, policy, "jobs");
        orch.submit(&job("j1", "k1")).await.unwrap();
        let outcomes = orch.run_until_idle().await.unwrap();
        assert_eq!(
            outcomes,
            vec![
                StepOutcome::Retried("j1".to_string()),
                StepOutcome::DeadLettered("j1".to_string()),
            ]
        );
        assert_eq!(bus.pending_count(orch.dead_letter_topic()), 1);
    }
}
