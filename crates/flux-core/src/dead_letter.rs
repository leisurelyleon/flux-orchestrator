//! Dead-letter classification rules.

/// How a failure should be treated for retry purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    /// Retrying may succeed (e.g. a timeout).
    Transient,
    /// Retrying cannot succeed (e.g. a malformed request).
    Permanent,
}

/// Classifies a failure from its message. Permanent failures are dead-lettered
/// immediately rather than retried.
pub fn classify(message: &str) -> FailureClass {
    let lower = message.to_lowercase();
    const PERMANENT_MARKERS: [&str; 4] = ["invalid", "not found", "unauthorized", "malformed"];
    if PERMANENT_MARKERS.iter().any(|marker| lower.contains(marker)) {
        FailureClass::Permanent
    } else {
        FailureClass::Transient
    }
}

/// Decides whether a failed job should be dead-lettered, given how many attempts
/// it has had, the cap, and the failure class.
pub fn should_dead_letter(attempts: u32, max_attempts: u32, class: FailureClass) -> bool {
    match class {
        FailureClass::Permanent => true,
        FailureClass::Transient => attempts >= max_attempts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_permanent_failures() {
        assert_eq!(classify("invalid payload"), FailureClass::Permanent);
        assert_eq!(classify("resource not found"), FailureClass::Permanent);
    }

    #[test]
    fn classifies_transient_failures() {
        assert_eq!(classify("connection reset by peer"), FailureClass::Transient);
    }

    #[test]
    fn permanent_failures_dead_letter_immediately() {
        assert!(should_dead_letter(0, 5, FailureClass::Permanent));
    }

    #[test]
    fn transient_dead_letters_only_when_exhausted() {
        assert!(!should_dead_letter(2, 5, FailureClass::Transient));
        assert!(should_dead_letter(5, 5, FailureClass::Transient));
    }
}
