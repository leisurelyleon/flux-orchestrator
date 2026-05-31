//! The EventBus abstraction for `flux`, with a complete in-memory backend and an
//! optional, feature-gated Kafka backend.

pub mod bus;
pub mod in_memory;

#[cfg(feature = "kafka")]
pub mod kafka;

pub use bus::{BusError, BusResult, Delivery, Envelope, EventBus, Offset};
pub use in_memory::InMemoryBus;

#[cfg(feature = "kafka")]
pub use kafka::KafkaBus;
