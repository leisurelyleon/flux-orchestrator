//! Core job types.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A stable job identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The lifecycle state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Pending,
    Running,
    Completed,
    Failed,
    DeadLettered,
}

/// A unit of work to be processed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub idempotency_key: String,
    pub payload: String,
    pub attempts: u32,
    pub state: JobState,
}

impl Job {
    pub fn new(
        id: impl Into<String>,
        idempotency_key: impl Into<String>,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            id: JobId::new(id),
            idempotency_key: idempotency_key.into(),
            payload: payload.into(),
            attempts: 0,
            state: JobState::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_job_starts_pending_with_zero_attempts() {
        let job = Job::new("j1", "k1", "payload");
        assert_eq!(job.state, JobState::Pending);
        assert_eq!(job.attempts, 0);
        assert_eq!(job.id.as_str(), "j1");
    }
}
