//! A Kafka-backed EventBus. Compiled only with the `kafka` feature.
//!
//! This consumer uses auto-commit, so `ack`/`nack` are intentional no-ops: the
//! *effectively-once* guarantee is provided by the dedup store in `flux-engine`,
//! not by the bus. That keeps the correctness guarantee independent of which
//! backend is in use.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::bus::{BusError, BusResult, Delivery, Envelope, EventBus, Offset};

/// A Kafka-backed bus over a single topic subscription.
pub struct KafkaBus {
    producer: FutureProducer,
    consumer: StreamConsumer,
}

impl KafkaBus {
    /// Connects a producer and a subscribed consumer to the given brokers.
    pub fn connect(brokers: &str, group_id: &str, topic: &str) -> BusResult<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|e| BusError::Backend(e.to_string()))?;

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| BusError::Backend(e.to_string()))?;

        consumer.subscribe(&[topic]).map_err(|e| BusError::Backend(e.to_string()))?;

        Ok(Self { producer, consumer })
    }
}

#[async_trait]
impl EventBus for KafkaBus {
    async fn publish(&self, topic: &str, envelope: Envelope) -> BusResult<Offset> {
        let record = FutureRecord::to(topic).key(&envelope.key).payload(&envelope.payload);
        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((_partition, offset)) => Ok(Offset(offset as u64)),
            Err((err, _msg)) => Err(BusError::Backend(err.to_string())),
        }
    }

    async fn poll(&self, _topic: &str) -> BusResult<Option<Delivery>> {
        match tokio::time::timeout(Duration::from_millis(500), self.consumer.recv()).await {
            Ok(Ok(message)) => {
                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_default();
                let payload = message.payload().map(<[u8]>::to_vec).unwrap_or_default();
                Ok(Some(Delivery {
                    offset: Offset(message.offset() as u64),
                    envelope: Envelope::new(key, payload),
                }))
            }
            Ok(Err(err)) => Err(BusError::Backend(err.to_string())),
            Err(_timeout) => Ok(None),
        }
    }

    async fn ack(&self, _topic: &str, _offset: Offset) -> BusResult<()> {
        Ok(()) // auto-commit; dedup store provides the real guarantee
    }

    async fn nack(&self, _topic: &str, _offset: Offset) -> BusResult<()> {
        Ok(()) // Kafka redelivers on consumer restart
    }
}
