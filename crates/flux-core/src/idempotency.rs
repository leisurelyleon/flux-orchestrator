//! Idempotency-key deduplication, as a pure set. The async store that wraps
//! this lives in `flux-engine`.

use std::collections::HashSet;

/// Tracks which idempotency keys have already been processed.
#[derive(Debug, Clone, Default)]
pub struct DedupSet {
    seen: HashSet<String>,
}

impl DedupSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a key. Returns `true` if it was newly inserted, `false` if it was
    /// already present.
    pub fn record(&mut self, key: impl Into<String>) -> bool {
        self.seen.insert(key.into())
    }

    pub fn contains(&self, key: &str) -> bool {
        self.seen.contains(key)
    }

    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_returns_true_for_new_key() {
        let mut set = DedupSet::new();
        assert!(set.record("a"));
        assert!(!set.record("a"));
    }

    #[test]
    fn contains_reflects_recorded_keys() {
        let mut set = DedupSet::new();
        set.record("x");
        assert!(set.contains("x"));
        assert!(!set.contains("y"));
    }
}
