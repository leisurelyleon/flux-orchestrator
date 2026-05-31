//! End-to-end pipeline tests exercising submit -> process across the real
//! in-memory bus and the full orchestrator, with no mocking of internals.

use std::sync::Arc;

use flux_bus::InMemoryBus;
use flux_core::{Job, RetryPolicy};
use flux_engine::{EchoHandler, FlakyHandler, Orchestrator, StepOutcome};

#[tokio::test]
async fn full_batch_processes_to_completion() {
    let bus = Arc::new(InMemoryBus::new());
    let orch = Orchestrator::new(bus, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");

    for i in 0..50 {
        orch.submit(&Job::new(format!("job-{i}"), format!("key-{i}"), "{}"))
            .await
            .unwrap();
    }

    let outcomes = orch.run_until_idle().await.unwrap();
    let completed = outcomes
        .iter()
        .filter(|o| matches!(o, StepOutcome::Completed(_)))
        .count();

    assert_eq!(completed, 50);
    assert_eq!(orch.dedup().len(), 50);
}

#[tokio::test]
async fn mixed_success_and_dead_letter_batch() {
    let bus = Arc::new(InMemoryBus::new());
    let handler = Arc::new(FlakyHandler {
        succeed_at_attempt: 100,
    }); // always fails
    let policy = RetryPolicy {
        max_attempts: 2,
        ..RetryPolicy::default()
    };
    let orch = Orchestrator::new(bus.clone(), handler, policy, "jobs");

    orch.submit(&Job::new("doomed", "key-d", "{}"))
        .await
        .unwrap();
    let outcomes = orch.run_until_idle().await.unwrap();

    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, StepOutcome::DeadLettered(_)))
    );
    assert_eq!(bus.pending_count(orch.dead_letter_topic()), 1);
}
