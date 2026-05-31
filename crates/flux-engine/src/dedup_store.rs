//! A thread-safe store of processed idempotency keys, wrapping the pure
//! `DedupSet` from `flux-core`.

use std::sync::Mutex;

use flux_core::DedupSet;

/// Concurrency-safe deduplication store.
#[derive(Default)]
pub struct DedupStore {
    inner: Mutex<DedupSet>,
}

impl DedupStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.inner
            .lock()
            .expect("dedup lock poisoned")
            .contains(key)
    }

    /// Records a key; returns `true` if newly recorded.
    pub fn record(&self, key: &str) -> bool {
        self.inner.lock().expect("dedup lock poisoned").record(key)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("dedup lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().expect("dedup lock poisoned").is_empty()
    }
}
