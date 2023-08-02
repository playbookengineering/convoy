use std::{
    fmt::Display,
    mem::{self, ManuallyDrop},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    message::{BorrowedMessage, Headers, Message as _Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    ClientContext,
};

use crate::{
    message_bus::{IncomingMessage, MessageBus},
    producer::Producer,
    RawHeaders,
};

pub struct RdKafkaOwnedMessage<C>
where
    C: ConsumerContext + 'static,
{
    consumer: ManuallyDrop<Arc<StreamConsumer<C>>>,
    message: ManuallyDrop<BorrowedMessage<'static>>,
}

impl<C: ConsumerContext> RdKafkaOwnedMessage<C> {
    // Constructs new `RdkafkaOwnedMessage`
    //
    // SAFETY: `message` must originate from `consumer`
    unsafe fn new<'a>(consumer: &'a Arc<StreamConsumer<C>>, message: BorrowedMessage<'a>) -> Self {
        let consumer = Arc::clone(consumer);

        // SAFETY: since we have `consumer` for 'static we can extend
        // message lifetime
        let message = mem::transmute::<_, BorrowedMessage<'static>>(message);

        Self {
            consumer: ManuallyDrop::new(consumer),
            message: ManuallyDrop::new(message),
        }
    }

    pub fn message(&self) -> &BorrowedMessage<'_> {
        &self.message
    }

    pub fn commit(&self) -> Result<(), rdkafka::error::KafkaError> {
        self.consumer
            .commit_message(&self.message, CommitMode::Async)
    }
}

impl<C: ConsumerContext> Drop for RdKafkaOwnedMessage<C> {
    fn drop(&mut self) {
        // SAFETY: By preserving order (message first, consumer second)
        // we guarantee that `message` still points to valid memory
        // allocated by rdkafka
        unsafe {
            ManuallyDrop::drop(&mut self.message);
            ManuallyDrop::drop(&mut self.consumer);
        }
    }
}

pub struct KafkaConsumer<C>
where
    C: ConsumerContext + 'static,
{
    consumer: Arc<StreamConsumer<C>>,
}

impl<C> KafkaConsumer<C>
where
    C: ConsumerContext + 'static,
{
    pub fn new(consumer: StreamConsumer<C>) -> Self {
        Self {
            consumer: Arc::new(consumer),
        }
    }
}

#[async_trait]
impl<C: ConsumerContext + 'static> MessageBus for KafkaConsumer<C> {
    type IncomingMessage = RdKafkaOwnedMessage<C>;
    type Error = rdkafka::error::KafkaError;

    async fn recv(&self) -> Result<Self::IncomingMessage, Self::Error> {
        self.consumer
            .recv()
            .await
            .map(|m| unsafe { RdKafkaOwnedMessage::<C>::new(&self.consumer, m) })
    }

    async fn ack(&self, message: &Self::IncomingMessage) -> Result<(), Self::Error> {
        message.commit()
    }

    async fn nack(&self, _: &Self::IncomingMessage) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn reject(&self, message: &Self::IncomingMessage) -> Result<(), Self::Error> {
        message.commit()
    }
}

impl<C: ConsumerContext + 'static> IncomingMessage for RdKafkaOwnedMessage<C> {
    fn headers(&self) -> RawHeaders {
        self.message()
            .headers()
            .map(|headers| {
                headers
                    .iter()
                    .filter_map(|header| {
                        let value = header.value?;
                        let value = std::str::from_utf8(value).ok()?;

                        let key = header.key.to_string();
                        let value = value.to_string();

                        Some((key, value))
                    })
                    .collect::<RawHeaders>()
            })
            .unwrap_or_default()
    }

    fn payload(&self) -> &[u8] {
        self.message().payload().unwrap_or_default()
    }

    fn key(&self) -> Option<&str> {
        self.message.key().and_then(|x| std::str::from_utf8(x).ok())
    }
}

#[derive(Debug, Default)]
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

#[derive(Clone)]
pub struct KafkaProducer<C: ClientContext + 'static> {
    producer: Arc<FutureProducer<C>>,
    topic: String,
}

impl<C: ClientContext + 'static> KafkaProducer<C> {
    pub fn new(producer: FutureProducer<C>, topic: String) -> Self {
        Self {
            producer: Arc::new(producer),
            topic,
        }
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
            .payload(&payload)
            .headers(headers);

        self.producer
            .send(record, Duration::from_secs(10))
            .await
            .map(|_| ())
            .map_err(|err| err.0)
    }
}