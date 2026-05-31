//! An in-memory EventBus. Real and complete — it powers the entire test suite
//! and the local demo with no external broker.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::bus::{BusResult, Delivery, Envelope, EventBus, Offset};

#[derive(Default)]
struct TopicState {
    next_offset: u64,
    pending: VecDeque<Delivery>,
    in_flight: HashMap<u64, Envelope>,
}

/// A thread-safe, in-memory at-least-once bus.
#[derive(Default)]
pub struct InMemoryBus {
    topics: Mutex<HashMap<String, TopicState>>,
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of messages awaiting delivery on a topic.
    pub fn pending_count(&self, topic: &str) -> usize {
        let guard = self.topics.lock().expect("topics lock poisoned");
        guard.get(topic).map_or(0, |state| state.pending.len())
    }

    /// Number of delivered-but-unacknowledged messages on a topic.
    pub fn in_flight_count(&self, topic: &str) -> usize {
        let guard = self.topics.lock().expect("topics lock poisoned");
        guard.get(topic).map_or(0, |state| state.in_flight.len())
    }

    /// Returns all in-flight messages to the pending queue, simulating a
    /// consumer restart. Used by the chaos harness.
    pub fn recover(&self, topic: &str) {
        let mut guard = self.topics.lock().expect("topics lock poisoned");
        if let Some(state) = guard.get_mut(topic) {
            let mut items: Vec<(u64, Envelope)> = state.in_flight.drain().collect();
            items.sort_by_key(|(offset, _)| *offset);
            for (offset, envelope) in items {
                state.pending.push_back(Delivery {
                    offset: Offset(offset),
                    envelope,
                });
            }
        }
    }
}

#[async_trait]
impl EventBus for InMemoryBus {
    async fn publish(&self, topic: &str, envelope: Envelope) -> BusResult<Offset> {
        let mut guard = self.topics.lock().expect("topics lock poisoned");
        let state = guard.entry(topic.to_string()).or_default();
        let offset = state.next_offset;
        state.next_offset += 1;
        state.pending.push_back(Delivery {
            offset: Offset(offset),
            envelope,
        });
        Ok(Offset(offset))
    }

    async fn poll(&self, topic: &str) -> BusResult<Option<Delivery>> {
        let mut guard = self.topics.lock().expect("topics lock poisoned");
        let Some(state) = guard.get_mut(topic) else {
            return Ok(None);
        };
        match state.pending.pop_front() {
            Some(delivery) => {
                state
                    .in_flight
                    .insert(delivery.offset.0, delivery.envelope.clone());
                Ok(Some(delivery))
            }
            None => Ok(None),
        }
    }

    async fn ack(&self, topic: &str, offset: Offset) -> BusResult<()> {
        let mut guard = self.topics.lock().expect("topics lock poisoned");
        if let Some(state) = guard.get_mut(topic) {
            state.in_flight.remove(&offset.0);
        }
        Ok(())
    }

    async fn nack(&self, topic: &str, offset: Offset) -> BusResult<()> {
        let mut guard = self.topics.lock().expect("topics lock poisoned");
        if let Some(state) = guard.get_mut(topic) {
            if let Some(envelope) = state.in_flight.remove(&offset.0) {
                state.pending.push_back(Delivery { offset, envelope });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_then_poll_returns_message() {
        let bus = InMemoryBus::new();
        bus.publish("t", Envelope::new("k", vec![1, 2, 3]))
            .await
            .unwrap();
        let delivery = bus.poll("t").await.unwrap().unwrap();
        assert_eq!(delivery.envelope.key, "k");
        assert_eq!(bus.in_flight_count("t"), 1);
    }

    #[tokio::test]
    async fn ack_removes_in_flight() {
        let bus = InMemoryBus::new();
        let offset = bus.publish("t", Envelope::new("k", vec![])).await.unwrap();
        bus.poll("t").await.unwrap();
        bus.ack("t", offset).await.unwrap();
        assert_eq!(bus.in_flight_count("t"), 0);
    }

    #[tokio::test]
    async fn nack_requeues_for_redelivery() {
        let bus = InMemoryBus::new();
        let offset = bus.publish("t", Envelope::new("k", vec![])).await.unwrap();
        bus.poll("t").await.unwrap();
        bus.nack("t", offset).await.unwrap();
        assert_eq!(bus.pending_count("t"), 1);
        assert_eq!(bus.in_flight_count("t"), 0);
    }

    #[tokio::test]
    async fn recover_returns_in_flight_to_pending() {
        let bus = InMemoryBus::new();
        bus.publish("t", Envelope::new("k", vec![])).await.unwrap();
        bus.poll("t").await.unwrap();
        bus.recover("t");
        assert_eq!(bus.pending_count("t"), 1);
        assert_eq!(bus.in_flight_count("t"), 0);
    }

    #[tokio::test]
    async fn poll_empty_topic_returns_none() {
        let bus = InMemoryBus::new();
        assert!(bus.poll("nope").await.unwrap().is_none());
    }
}
