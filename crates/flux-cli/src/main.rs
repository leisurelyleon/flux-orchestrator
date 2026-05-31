//! `flux` command-line entry point.

use std::sync::Arc;

use clap::Parser;

use flux_bus::InMemoryBus;
use flux_cli::cli::{Cli, Command};
use flux_core::{Job, RetryPolicy};
use flux_engine::{EchoHandler, Orchestrator, telemetry};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Demo => run_demo().await,
    }
}

/// Submits a handful of jobs — including a duplicate idempotency key — and runs
/// them to completion on the in-memory bus, showing effectively-once dedup.
async fn run_demo() {
    telemetry::init();

    let bus = Arc::new(InMemoryBus::new());
    let orch = Orchestrator::new(bus, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");

    orch.submit(&Job::new("order-1", "key-A", r#"{"sku":"X"}"#))
        .await
        .unwrap();
    orch.submit(&Job::new("order-2", "key-B", r#"{"sku":"Y"}"#))
        .await
        .unwrap();
    orch.submit(&Job::new("order-3", "key-A", r#"{"sku":"X"}"#))
        .await
        .unwrap(); // duplicate key

    let outcomes = orch.run_until_idle().await.unwrap();

    println!("=== flux-orchestrator demo ===");
    for outcome in &outcomes {
        println!("  {outcome:?}");
    }
    println!(
        "Distinct keys processed (deduplicated): {}",
        orch.dedup().len()
    );
}
