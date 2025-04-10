use thiserror::Error;

use rdkafka::{config::ClientConfig, producer::{FutureProducer, FutureRecord}};

use std::{env, time::Duration};

#[derive(Error, Debug)]
pub(crate) enum KafkaError {
    #[error("Couldn't create producer")]
    ProducerCreationError,
    #[error("Message delivery failed")]
    MessageDeliveryError,
}

pub(crate) struct KafkaClient {
    pub(self) producer: FutureProducer,
    topic: String,
}

impl KafkaClient {
    pub async fn send_message(&self, key: &str, message: &str) -> Result<(), KafkaError> {
        self.producer
            .send(
                FutureRecord::to(&self.topic).key(key).payload(message),
                Duration::from_secs(0),
            )
            .await
            .map(|_| ())
            .map_err(|_| KafkaError::MessageDeliveryError)
    }

    pub fn new() -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &env::var("KAFKA_BROKERS").expect("KAFKA_BROKERS not set"))
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|_| KafkaError::ProducerCreationError)?;

        let topic = env::var("KAFKA_TOPIC").expect("KAFKA_TOPIC not set");

        Ok(KafkaClient { producer, topic })
    }
}

#[cfg(test)]
mod test;