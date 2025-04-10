#[cfg(test)]
mod tests {
    use crate::kafka::{
        Producer,
        ProducerError
    };
    use async_trait::async_trait;
    use std::sync::{
        Arc,
        Mutex
    };

    // Mock implementation of the Producer trait
    struct MockProducer {
        sent_messages: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl MockProducer {
        fn new() -> Self {
            Self {
                sent_messages: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Producer for MockProducer {
        async fn send_message(&self, key: &str, message: &str) -> Result<(), ProducerError> {
            let mut messages = self.sent_messages.lock().unwrap();
            messages.push((key.to_string(), message.to_string()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_send_message_success() {
        let mock_producer = MockProducer::new();

        let result = mock_producer.send_message("key", "message").await;
        assert!(result.is_ok());

        let sent_messages = mock_producer.sent_messages.lock().unwrap();
        assert_eq!(sent_messages.len(), 1);
        assert_eq!(sent_messages[0], ("key".to_string(), "message".to_string()));
    }

    // Mock implementation for failure scenario
    struct FailingMockProducer;

    #[async_trait]
    impl Producer for FailingMockProducer {
        async fn send_message(&self, _key: &str, _message: &str) -> Result<(), ProducerError> {
            Err(ProducerError::MessageDeliveryError)
        }
    }

    #[tokio::test]
    async fn test_send_message_failure() {
        let failing_producer = FailingMockProducer;

        let result = failing_producer.send_message("key", "message").await;
        assert!(result.is_err());
    }
}
