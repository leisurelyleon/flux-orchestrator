//! The EventBus trait and its message types.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A position within a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Offset(pub u64);

/// A message on the bus: an idempotency key plus an opaque payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    pub key: String,
    pub payload: Vec<u8>,
}

impl Envelope {
    pub fn new(key: impl Into<String>, payload: Vec<u8>) -> Self {
        Self { key: key.into(), payload }
    }
}

/// A polled message together with the offset needed to acknowledge it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub offset: Offset,
    pub envelope: Envelope,
}

/// Errors raised by a bus backend.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("bus backend error: {0}")]
    Backend(String),
}

/// Convenience result alias.
pub type BusResult<T> = std::result::Result<T, BusError>;

/// An at-least-once message bus. Implementors deliver messages that must be
/// acknowledged; unacknowledged messages may be redelivered.
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publishes an envelope to a topic, returning its offset.
    async fn publish(&self, topic: &str, envelope: Envelope) -> BusResult<Offset>;

    /// Polls the next available delivery, if any.
    async fn poll(&self, topic: &str) -> BusResult<Option<Delivery>>;

    /// Acknowledges successful processing of a delivery.
    async fn ack(&self, topic: &str, offset: Offset) -> BusResult<()>;

    /// Negatively acknowledges a delivery, making it available for redelivery.
    async fn nack(&self, topic: &str, offset: Offset) -> BusResult<()>;
}
