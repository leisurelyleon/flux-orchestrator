//! The pure job lifecycle state machine.

use crate::job::JobState;

/// An event that may transition a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobEvent {
    Start,
    Succeed,
    Fail,
}

/// The result of attempting a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    To(JobState),
    Invalid,
}

/// Computes the next state for a `(current, event)` pair. On failure, `can_retry`
/// decides whether the job returns to `Pending` or is dead-lettered.
pub fn transition(current: JobState, event: JobEvent, can_retry: bool) -> Transition {
    use JobEvent::{Fail, Start, Succeed};
    use JobState::{Completed, DeadLettered, Pending, Running};

    match (current, event) {
        (Pending, Start) => Transition::To(Running),
        (Running, Succeed) => Transition::To(Completed),
        (Running, Fail) if can_retry => Transition::To(Pending),
        (Running, Fail) => Transition::To(DeadLettered),
        _ => Transition::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::JobState;

    #[test]
    fn pending_starts_to_running() {
        assert_eq!(
            transition(JobState::Pending, JobEvent::Start, true),
            Transition::To(JobState::Running)
        );
    }

    #[test]
    fn running_succeeds_to_completed() {
        assert_eq!(
            transition(JobState::Running, JobEvent::Succeed, true),
            Transition::To(JobState::Completed)
        );
    }

    #[test]
    fn running_fails_to_pending_when_retryable() {
        assert_eq!(
            transition(JobState::Running, JobEvent::Fail, true),
            Transition::To(JobState::Pending)
        );
    }

    #[test]
    fn running_fails_to_dead_letter_when_exhausted() {
        assert_eq!(
            transition(JobState::Running, JobEvent::Fail, false),
            Transition::To(JobState::DeadLettered)
        );
    }

    #[test]
    fn invalid_transition_is_rejected() {
        assert_eq!(
            transition(JobState::Completed, JobEvent::Start, true),
            Transition::Invalid
        );
    }
}
