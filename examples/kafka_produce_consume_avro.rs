use std::{
    fmt::Display,
    time::{Duration, SystemTime},
};

use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, StreamConsumer},
    producer::FutureProducer,
    ClientConfig,
};

use convoy::{
    codec::{Avro, AvroRegistry},
    consumer::{MessageConsumer, WorkerPoolConfig},
    integration::kafka::{KafkaConsumer, KafkaProducer},
    producer::MessageProducer,
};
use tracing::metadata::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter};

use convoy::message::{Message, RawHeaders};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct MyMessage {
    pub metadata: Metadata,
    pub body: Entity,
}

#[derive(Debug)]
pub struct Metadata {
    pub created_at_millis: i64,
}

impl Metadata {
    const CREATED_AT_MILLIS_HEADER_NAME: &'static str = "created-at";
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Entity {
    pub id: i32,
    pub first_name: String,
    pub second_name: String,
}

impl Message for MyMessage {
    const KIND: &'static str = "my-message.v1"; // unique message descriptor

    type Body = Entity;
    type Headers = Metadata;

    // instructs framework how to build our message type
    fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self {
        Self {
            metadata: headers,
            body,
        }
    }

    // instructs framework how to destruct our message type
    fn into_body_and_headers(self) -> (Self::Body, Self::Headers) {
        (self.body, self.metadata)
    }

    fn key(&self) -> String {
        self.body.id.to_string()
    }
}

#[derive(Debug)]
pub enum MetadataError {
    HeaderNotFound,
    HeaderParse,
}

impl Display for MetadataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetadataError::HeaderNotFound => "header not found",
            MetadataError::HeaderParse => "header parse error",
        };

        write!(f, "{s}")
    }
}

impl std::error::Error for MetadataError {}

impl TryFrom<RawHeaders> for Metadata {
    type Error = MetadataError;

    fn try_from(mut raw_headers: RawHeaders) -> Result<Self, Self::Error> {
        let created_at_millis = raw_headers
            .remove(Self::CREATED_AT_MILLIS_HEADER_NAME)
            .ok_or(MetadataError::HeaderNotFound)?;

        let created_at_millis = created_at_millis
            .parse()
            .map_err(|_| MetadataError::HeaderParse)?;

        Ok(Self { created_at_millis })
    }
}

impl From<Metadata> for RawHeaders {
    fn from(metadata: Metadata) -> Self {
        [(
            Metadata::CREATED_AT_MILLIS_HEADER_NAME.to_string(),
            metadata.created_at_millis.to_string(),
        )]
        .into_iter()
        .collect()
    }
}

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
    let registry =
        std::env::var("AVRO_REGISTRY").unwrap_or_else(|_| "http:/127.0.0.1:8081".to_owned());

    tracing::info!(
        "Brokers: {brokers}, group_id: {group_id}, topic: {topic:?}, registry: {registry:?}"
    );

    let consumer = setup_consumer(&brokers, &group_id, &topic);
    let producer = setup_producer(&brokers);

    // wrap producer
    let registry = AvroRegistry::new(registry.clone());
    let avro = Avro::new(registry, "test_topic");

    let producer = KafkaProducer::new(producer, topic);
    let producer = MessageProducer::builder(producer, avro.clone()).build();

    tokio::spawn(producer_loop(producer));

    // wrap consumer
    let consumer = KafkaConsumer::new(consumer);

    MessageConsumer::new(avro)
        .message_handler(my_message_handler)
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

async fn my_message_handler(message: MyMessage) {
    tracing::info!("Received message: {message:?}");
}

async fn producer_loop(producer: MessageProducer<KafkaProducer, Avro<AvroRegistry>>) {
    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let id = rand::random();

        let body = Entity {
            id,
            first_name: "Joe".to_owned(),
            second_name: "Doe".to_owned(),
        };

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis();

        let metadata = Metadata {
            created_at_millis: now as i64,
        };

        let message = MyMessage { metadata, body };

        producer
            .produce(message, Default::default())
            .await
            .expect("Failed to produce");
    }
}
