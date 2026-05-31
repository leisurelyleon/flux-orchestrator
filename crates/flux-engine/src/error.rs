//! Engine error type.

/// Errors raised while orchestrating jobs.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error("bus error: {0}")]
    Bus(#[from] flux_bus::BusError),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Convenience result alias.
pub type EngineResult<T> = std::result::Result<T, EngineError>;
