//! A fault-injection harness for `flux`.
//!
//! `ChaosBus` wraps any `EventBus` and injects realistic distributed-systems
//! failures — duplicate deliveries and simulated consumer crashes — to prove
//! that the orchestrator's effectively-once guarantee holds under adversity.
//!
//! The failures are driven by a seeded, deterministic RNG so that a failing
//! scenario can be reproduced exactly.

use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use flux_bus::{BusResult, Delivery, Envelope, EventBus, Offset};

/// Configuration for injected faults.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability in `[0.0, 1.0]` that a successful poll is delivered twice.
    pub duplicate_probability: f64,
    /// Seed for the deterministic RNG, so scenarios are reproducible.
    pub seed: u64,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            duplicate_probability: 0.3,
            seed: 42,
        }
    }
}

/// A bus wrapper that injects faults around an inner bus.
pub struct ChaosBus {
    inner: Arc<dyn EventBus>,
    config: ChaosConfig,
    rng: Mutex<StdRng>,
    /// A buffered duplicate to hand back on the next poll, simulating a message
    /// the broker redelivered before the first was acknowledged.
    pending_duplicate: Mutex<Option<Delivery>>,
}

impl ChaosBus {
    pub fn new(inner: Arc<dyn EventBus>, config: ChaosConfig) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);
        Self {
            inner,
            config,
            rng: Mutex::new(rng),
            pending_duplicate: Mutex::new(None),
        }
    }
}

#[async_trait]
impl EventBus for ChaosBus {
    async fn publish(&self, topic: &str, envelope: Envelope) -> BusResult<Offset> {
        self.inner.publish(topic, envelope).await
    }

    async fn poll(&self, topic: &str) -> BusResult<Option<Delivery>> {
        // If we buffered a duplicate last time, deliver it now.
        if let Some(duplicate) = self.pending_duplicate.lock().expect("lock").take() {
            return Ok(Some(duplicate));
        }

        let delivery = self.inner.poll(topic).await?;

        if let Some(ref delivery) = delivery {
            let roll: f64 = self.rng.lock().expect("lock").r#gen();
            if roll < self.config.duplicate_probability {
                // Buffer a copy to be redelivered on the next poll.
                *self.pending_duplicate.lock().expect("lock") = Some(delivery.clone());
            }
        }

        Ok(delivery)
    }

    async fn ack(&self, topic: &str, offset: Offset) -> BusResult<()> {
        self.inner.ack(topic, offset).await
    }

    async fn nack(&self, topic: &str, offset: Offset) -> BusResult<()> {
        self.inner.nack(topic, offset).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flux_bus::InMemoryBus;
    use flux_core::{Job, RetryPolicy};
    use flux_engine::{EchoHandler, Orchestrator, StepOutcome};

    /// Under heavy duplicate injection, every distinct idempotency key must be
    /// processed exactly once: duplicates are skipped, never double-applied.
    #[tokio::test]
    async fn duplicates_never_double_process() {
        let inner = Arc::new(InMemoryBus::new());
        let chaos = Arc::new(ChaosBus::new(
            inner,
            ChaosConfig {
                duplicate_probability: 0.9,
                seed: 7,
            },
        ));
        let orch = Orchestrator::new(chaos, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");

        for i in 0..20 {
            let key = format!("key-{i}");
            orch.submit(&Job::new(format!("job-{i}"), key, "{}"))
                .await
                .unwrap();
        }

        let outcomes = orch.run_until_idle().await.unwrap();

        let completed = outcomes
            .iter()
            .filter(|o| matches!(o, StepOutcome::Completed(_)))
            .count();
        let skipped = outcomes
            .iter()
            .filter(|o| matches!(o, StepOutcome::Skipped(_)))
            .count();

        // Exactly 20 distinct keys completed; any extra deliveries were skipped.
        assert_eq!(completed, 20);
        assert!(
            skipped > 0,
            "chaos should have injected at least one duplicate"
        );
        assert_eq!(orch.dedup().len(), 20);
    }

    /// After a simulated consumer crash mid-flight, recovered messages are
    /// reprocessed without loss and without double-application.
    #[tokio::test]
    async fn no_loss_after_simulated_crash() {
        let inner = Arc::new(InMemoryBus::new());
        let orch = Orchestrator::new(
            inner.clone(),
            Arc::new(EchoHandler),
            RetryPolicy::default(),
            "jobs",
        );

        for i in 0..10 {
            orch.submit(&Job::new(format!("job-{i}"), format!("key-{i}"), "{}"))
                .await
                .unwrap();
        }

        // Process a few, then simulate a crash: return in-flight work to pending.
        orch.step().await.unwrap();
        orch.step().await.unwrap();
        inner.recover("jobs");

        let outcomes = orch.run_until_idle().await.unwrap();
        let total_completed = 2 + outcomes
            .iter()
            .filter(|o| matches!(o, StepOutcome::Completed(_)))
            .count();

        // All 10 distinct keys end up processed exactly once.
        assert_eq!(orch.dedup().len(), 10);
        assert_eq!(total_completed, 10);
    }
}
