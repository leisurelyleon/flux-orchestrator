//! Core error type.

/// Errors from pure core logic.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error("invalid state transition")]
    InvalidTransition,
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, CoreError>;
