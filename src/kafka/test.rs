#[cfg(test)]
mod tests {
    use crate::kafka::KafkaClient;
    use std::env;

    #[tokio::test]
    async fn test_kafka_client_new_success() {
        unsafe {
            // Set the environment variables for testing
            env::set_var("KAFKA_BROKERS", "localhost:9092");
            env::set_var("KAFKA_TOPIC", "test_topic");
        }

        let client = KafkaClient::new();
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_kafka_client_new_failure_missing_env() {
        unsafe {
            // Remove the environment variables to simulate failure
            env::remove_var("KAFKA_BROKERS");
            env::remove_var("KAFKA_TOPIC");
        }

        let client = KafkaClient::new();
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_send_message_failure() {
        unsafe {
            // Set the environment variables for testing
            env::set_var("KAFKA_BROKERS", "localhost:9092");
            env::set_var("KAFKA_TOPIC", "test_topic");
        }

        let client = KafkaClient::new().unwrap();
        let result = client.send_message("key", "message").await;
        assert!(result.is_err()); // This will fail unless a real Kafka broker is running
    }
}
