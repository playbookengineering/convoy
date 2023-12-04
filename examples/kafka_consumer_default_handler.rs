use std::time::Duration;

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};

use convoy::{
    codec::Json,
    consumer::{MessageConsumer, WorkerPoolConfig},
    integration::kafka::{
        extractor::{Offset, Partition, Topic},
        KafkaConsumer,
    },
    message::RawMessage,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

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
    let topics = std::env::var("TOPICS").unwrap_or_else(|_| "test_topic".to_owned());
    let topics = topics.as_str().split(',').collect::<Vec<_>>();

    tracing::info!("Brokers: {brokers}, group_id: {group_id}, topics: {topics:?}");

    let consumer = setup_consumer(&brokers, &group_id, &topics);
    let producer = setup_producer(&brokers);

    let topics = topics.into_iter().map(|t| t.to_owned()).collect();
    tokio::spawn(producer_loop(producer, topics));

    // wrap consumer
    let consumer = KafkaConsumer::new(consumer);

    MessageConsumer::new(Json)
        .fallback_handler(default_kafka_handler)
        .listen(consumer, WorkerPoolConfig::fixed(10))
        .await
        .expect("unexpected consumer finish");
}

fn setup_consumer(brokers: &str, group_id: &str, topics: &[&str]) -> StreamConsumer {
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
        .subscribe(topics)
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

async fn default_kafka_handler(
    message: RawMessage,
    Topic(topic): Topic,
    Partition(partition): Partition,
    Offset(offset): Offset,
) {
    tracing::info!(
        "Received message of {} bytes from {}:{}:{}",
        message.payload.len(),
        topic,
        partition,
        offset
    );
}

async fn producer_loop(producer: FutureProducer, topics: Vec<String>) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        for topic in topics.iter() {
            let record = FutureRecord::to(topic).payload(&[42_u8]).key("key");

            producer
                .send(record, Duration::from_secs(10))
                .await
                .expect("Failed to produce");
        }
    }
}
