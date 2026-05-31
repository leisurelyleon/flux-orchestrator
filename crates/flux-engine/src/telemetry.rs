//! Tracing/telemetry setup.

use tracing_subscriber::EnvFilter;

/// Initializes a tracing subscriber. Safe to call more than once; subsequent
/// calls are ignored.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
