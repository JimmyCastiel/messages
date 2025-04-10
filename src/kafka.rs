use env;

use rdkafka::{config::ClientConfig, producer::{FutureProducer, FutureRecord}};
use thiserror::Error;
use std::time::Duration;

#[derive(Error, Debug)]
pub(crate) enum KafkaError {
    #[error("Couldn't create producer")]
    ProducerCreationError,
    #[error("Message delivery failed")]
    MessageDeliveryError,
}

pub(crate) struct KafkaClient {
    producer: FutureProducer,
    topic: String,
}

impl KafkaClient {
    pub async fn send_message(&self, key: &str, message: &str) -> Result<(), KafkaError> {
        let record = FutureRecord::to(&self.topic)
            .key(key)
            .payload(message);

        let response = self.producer.send(record, Duration::from_secs(0)).await;

        match response {
            Ok(_) => Ok(()),
            _ => Err(KafkaError::MessageDeliveryError),
        }
    }

    pub fn new() -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set(
                "bootstrap.servers",
                env::var("KAFKA_BROKERS")
                    .expect("KAFKA_BROKERS environment variable is not set")
                    .parse::<String>()
                    .unwrap(),
            )
            .set("message.timeout.ms", "5000")
            .create()
            .map_err(|_| KafkaError::ProducerCreationError)?;

        let topic = env::var("KAFKA_TOPIC")
            .expect("KAFKA_TOPIC environment variable is not set")
            .parse::<String>()
            .unwrap();

        Ok(KafkaClient { producer, topic })
    }
}
