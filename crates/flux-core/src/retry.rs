//! Exponential-backoff retry policy. Pure and deterministic: the random jitter
//! factor is supplied by the caller so this logic is fully testable.

use std::time::Duration;

/// A retry policy: exponential backoff capped at a maximum, with an attempt cap.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub max_attempts: u32,
    pub multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_delay_ms: 100,
            max_delay_ms: 30_000,
            max_attempts: 5,
            multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    /// Whether another attempt is permitted after `attempts` have been made.
    pub fn should_retry(&self, attempts: u32) -> bool {
        attempts < self.max_attempts
    }

    /// Backoff for a zero-based attempt index, capped at `max_delay_ms`.
    pub fn backoff(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.powi(attempt as i32);
        let delay = (self.base_delay_ms as f64 * factor).min(self.max_delay_ms as f64);
        Duration::from_millis(delay as u64)
    }

    /// Backoff with a caller-provided jitter factor in `[0.0, 1.0]`, scaling the
    /// delay into `[0.5x, 1.0x]` of the base backoff to spread retries.
    pub fn backoff_with_jitter(&self, attempt: u32, jitter: f64) -> Duration {
        let base = self.backoff(attempt).as_millis() as f64;
        let scaled = base * (0.5 + 0.5 * jitter.clamp(0.0, 1.0));
        Duration::from_millis(scaled as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially() {
        let p = RetryPolicy {
            base_delay_ms: 100,
            max_delay_ms: 10_000,
            max_attempts: 5,
            multiplier: 2.0,
        };
        assert_eq!(p.backoff(0).as_millis(), 100);
        assert_eq!(p.backoff(1).as_millis(), 200);
        assert_eq!(p.backoff(2).as_millis(), 400);
    }

    #[test]
    fn backoff_capped_at_max() {
        let p = RetryPolicy {
            base_delay_ms: 100,
            max_delay_ms: 250,
            max_attempts: 5,
            multiplier: 2.0,
        };
        assert_eq!(p.backoff(5).as_millis(), 250);
    }

    #[test]
    fn should_retry_respects_max_attempts() {
        let p = RetryPolicy { max_attempts: 3, ..RetryPolicy::default() };
        assert!(p.should_retry(2));
        assert!(!p.should_retry(3));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let p = RetryPolicy {
            base_delay_ms: 1000,
            max_delay_ms: 10_000,
            max_attempts: 5,
            multiplier: 2.0,
        };
        assert_eq!(p.backoff_with_jitter(0, 0.0).as_millis(), 500);
        assert_eq!(p.backoff_with_jitter(0, 1.0).as_millis(), 1000);
    }
}
