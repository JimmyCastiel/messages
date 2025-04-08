use env;

use rdkafka::{config::ClientConfig, producer::FutureProducer};

pub(crate) fn init_producer() -> FutureProducer {
    ClientConfig::new()
        .set(
            "bootstrap.servers",
            env::var("KAFKA_BROKERS")
                .unwrap()
                .parse::<String>()
                .unwrap(),
        )
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}
