use std::{fmt::Display, time::Duration};

use async_trait::async_trait;
use rdkafka::{
    client::DefaultClientContext,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
    ClientContext,
};

use crate::{
    message::RawHeaders,
    producer::{Producer, RoutableOptions},
};

#[derive(Debug, Clone, Default)]
pub struct KafkaProducerOptions {
    topic_override: Option<String>,
    additional_headers: RawHeaders,
}

impl KafkaProducerOptions {
    pub fn override_topic(self, topic: String) -> Self {
        Self {
            topic_override: Some(topic),
            ..self
        }
    }

    pub fn add_header(mut self, key: impl Display, value: impl Display) -> Self {
        let key = key.to_string();
        let value = value.to_string();

        self.additional_headers.insert(key, value);

        self
    }
}

impl RoutableOptions for KafkaProducerOptions {
    /// In case of kafka route is a topic name
    fn route(mut self, target: Option<String>) -> Self {
        self.topic_override = target;
        self
    }
}

#[derive(Clone)]
pub struct KafkaProducer<C = DefaultClientContext>
where
    C: ClientContext + 'static,
{
    producer: FutureProducer<C>,
    topic: String,
}

impl<C: ClientContext + 'static> KafkaProducer<C> {
    pub fn new(producer: FutureProducer<C>, topic: String) -> Self {
        Self { producer, topic }
    }
}

#[async_trait]
impl<C: ClientContext + 'static> Producer for KafkaProducer<C> {
    type Options = KafkaProducerOptions;

    type Error = rdkafka::error::KafkaError;

    async fn send(
        &self,
        key: String,
        mut headers: RawHeaders,
        payload: Vec<u8>,
        options: Self::Options,
    ) -> Result<(), Self::Error> {
        let KafkaProducerOptions {
            topic_override,
            additional_headers,
        } = options;

        headers.extend(additional_headers);

        let topic = topic_override.as_deref().unwrap_or(self.topic.as_str());

        let headers_len = headers.len();
        let headers = headers.into_iter().fold(
            OwnedHeaders::new_with_capacity(headers_len),
            |headers, (key, value)| {
                headers.insert(rdkafka::message::Header {
                    key: &key,
                    value: Some(&value),
                })
            },
        );

        let record = FutureRecord::to(topic)
            .key(&key)
            .headers(headers)
            .payload(&payload);

        self.producer
            .send(record, Duration::from_secs(10))
            .await
            .map(|_| ())
            .map_err(|err| err.0)
    }

    fn make_span(
        &self,
        key: &str,
        _headers: &RawHeaders,
        _payload: &[u8],
        options: &Self::Options,
    ) -> tracing::Span {
        let topic = options
            .topic_override
            .as_deref()
            .unwrap_or(self.topic.as_str());

        tracing::info_span!(
            "producer",
            otel.name = %format!("{} send", topic).as_str(),
            otel.kind = "PRODUCER",
            otel.status_code = tracing::field::Empty,
            messaging.system = "kafka",
            messaging.destination = %topic,
            messaging.destination_kind = "topic",
            messaging.kafka.message_key = key,
        )
    }
}
