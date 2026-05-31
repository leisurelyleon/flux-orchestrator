//! Throughput benchmark: how fast the orchestrator drains a batch of jobs over
//! the in-memory bus. Measures the pure processing path with no broker latency.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};

use flux_bus::InMemoryBus;
use flux_core::{Job, RetryPolicy};
use flux_engine::{EchoHandler, Orchestrator};

fn bench_throughput(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    c.bench_function("process_1000_jobs", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let bus = Arc::new(InMemoryBus::new());
                let orch =
                    Orchestrator::new(bus, Arc::new(EchoHandler), RetryPolicy::default(), "jobs");
                for i in 0..1000 {
                    orch.submit(&Job::new(format!("j{i}"), format!("k{i}"), "{}"))
                        .await
                        .unwrap();
                }
                orch.run_until_idle().await.unwrap();
            });
        });
    });
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
