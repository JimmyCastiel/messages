use thiserror::Error;

use rdkafka::{
    config::ClientConfig,
    producer::{
        FutureProducer,
        FutureRecord
    }
};

use std::{env, time::Duration};

pub type KafkaFutureProducer = FutureProducer;

#[derive(Error, Debug)]
pub enum ProducerError {
    #[error("Couldn't create producer")]
    ProducerCreationError,
    #[error("Message delivery failed")]
    MessageDeliveryError,
}

#[async_trait]
pub trait Producer {
    async fn send_message(&self, key: &str, message: &str) -> Result<(), ProducerError>;
}

#[derive(Debug, Clone)]
pub struct KafkaProducer<P: Send + Sync + 'static> {
    pub(self) producer: P,
    topic: String,
}

impl KafkaProducer<FutureProducer> {
    pub fn new() -> Result<Self, ProducerError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &env::var("KAFKA_BROKERS")
            .expect("KAFKA_BROKERS not set"))
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|_| ProducerError::ProducerCreationError)?;

        let topic = env::var("KAFKA_TOPIC")
            .expect("KAFKA_TOPIC not set");

        Ok(KafkaProducer
         { producer, topic })
    }
}

#[async_trait]
impl Producer for &KafkaProducer<FutureProducer> {
    async fn send_message(&self, key: &str, message: &str) -> Result<(), ProducerError> {
        self.producer
            .send(
                FutureRecord::to(&self.topic)
                    .key(key)
                    .payload(message),
                Duration::from_secs(0),
            )
            .await
            .map(|_| ())
            .map_err(|_| ProducerError::MessageDeliveryError)
    }
}

#[cfg(test)]
mod test;