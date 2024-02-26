use std::time::Duration;

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};

use convoy::{
    codec::Json,
    consumer::{MessageConsumer, WorkerPoolConfig},
    integration::kafka::{KafkaConsumer, KafkaProducer},
    message::RawMessage,
    producer::MessageProducer,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter};

use convoy::message::RawHeaders;

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::Layer::default())
        .try_init()
        .expect("failed to install tracing subscriber");

    tracing::info!("kafka example start");

    let brokers = std::env::var("BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".to_owned());
    let group_id = std::env::var("GROUP_ID").unwrap_or_else(|_| "test_group_id".to_owned());
    let topic = std::env::var("TOPIC").unwrap_or_else(|_| "test_topic".to_owned());

    tracing::info!("Brokers: {brokers}, group_id: {group_id}, topic: {topic:?}");

    let consumer = setup_consumer(&brokers, &group_id, &topic);
    let producer = setup_producer(&brokers);

    // wrap producer
    let producer = KafkaProducer::new(producer, topic);
    let producer = MessageProducer::builder(producer, Json).build();

    tokio::spawn(producer_loop(producer));

    // wrap consumer
    let consumer = KafkaConsumer::new(consumer);

    MessageConsumer::new(Json)
        .fallback_handler(my_message_handler)
        .listen(consumer, WorkerPoolConfig::fixed(10))
        .await
        .expect("unexpected consumer finish");
}

fn setup_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topics");

    consumer
}

fn setup_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error")
}

async fn my_message_handler(message: RawMessage) {
    tracing::info!(
        "Received raw message, payload: {:?}, headers: {:?}",
        message.payload,
        message.headers
    );
}

async fn producer_loop(producer: MessageProducer<KafkaProducer, Json>) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let random = rand::random();

        producer
            .produce_raw(
                "static".to_owned(),
                vec![random],
                RawHeaders::default(),
                Default::default(),
            )
            .await
            .expect("Failed to produce");
    }
}
