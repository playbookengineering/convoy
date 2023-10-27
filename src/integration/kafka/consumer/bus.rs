use std::{
    cell::Cell,
    mem::{self, ManuallyDrop},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use futures_lite::{Stream, StreamExt};
use rdkafka::{
    consumer::{
        CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, MessageStream,
        StreamConsumer,
    },
    error::KafkaError,
    message::{BorrowedMessage, Headers, Message as _Message},
};

use crate::{
    consumer::{IncomingMessage, MessageBus},
    message::RawHeaders,
};
pub struct RdKafkaMessageStream<C>
where
    C: ConsumerContext + 'static,
{
    consumer: ManuallyDrop<Arc<StreamConsumer<C>>>,
    stream: ManuallyDrop<MessageStream<'static>>,
}

impl<C: ConsumerContext> RdKafkaMessageStream<C> {
    /// Constructs new `RdKafkaMessageStream`
    ///
    /// SAFETY: `stream` must originate from `consumer`
    unsafe fn new<'a>(consumer: &'a Arc<StreamConsumer<C>>, stream: MessageStream<'a>) -> Self {
        let consumer = Arc::clone(consumer);

        let stream = mem::transmute::<_, MessageStream<'static>>(stream);

        Self {
            consumer: ManuallyDrop::new(consumer),
            stream: ManuallyDrop::new(stream),
        }
    }
}

impl<C: ConsumerContext> Drop for RdKafkaMessageStream<C> {
    fn drop(&mut self) {
        // SAFETY: By preserving order (stream first, consumer second)
        // we guarantee that `message` still points to valid memory
        // allocated by rdkafka
        unsafe {
            ManuallyDrop::drop(&mut self.stream);
            ManuallyDrop::drop(&mut self.consumer);
        }
    }
}

impl<C: ConsumerContext> Stream for RdKafkaMessageStream<C> {
    type Item = Result<RdKafkaOwnedMessage<C>, KafkaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream
            .poll_next(cx)
            .map_ok(|message| unsafe { RdKafkaOwnedMessage::new(&self.consumer, message) })
    }
}

pub struct RdKafkaOwnedMessage<C>
where
    C: ConsumerContext + 'static,
{
    consumer: ManuallyDrop<Arc<StreamConsumer<C>>>,
    message: ManuallyDrop<BorrowedMessage<'static>>,
}

impl<C: ConsumerContext> RdKafkaOwnedMessage<C> {
    /// Constructs new `RdkafkaOwnedMessage`
    ///
    /// SAFETY: `message` must originate from `consumer`
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
            .commit_message(&self.message, CommitMode::Async)?;

        Ok(())
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

pub struct KafkaConsumer<C = DefaultConsumerContext>
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
    type Stream = RdKafkaMessageStream<C>;

    async fn into_stream(self) -> Result<Self::Stream, Self::Error> {
        let stream = self.consumer.stream();
        let stream = unsafe { RdKafkaMessageStream::new(&self.consumer, stream) };

        Ok(stream)
    }
}

#[async_trait]
impl<C: ConsumerContext + 'static> IncomingMessage for RdKafkaOwnedMessage<C> {
    type Error = KafkaError;

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

    fn key(&self) -> Option<&[u8]> {
        self.message.key()
    }

    async fn ack(&self) -> Result<(), Self::Error> {
        self.commit()
    }

    async fn nack(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn reject(&self) -> Result<(), Self::Error> {
        self.commit()
    }

    fn make_span(&self) -> tracing::Span {
        let msg = self.message();

        // https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/messaging/#apache-kafka
        tracing::info_span!(
            "consumer",
            otel.name = %format!("{} receive", msg.topic()).as_str(),
            otel.kind = "CONSUMER",
            otel.status_code = tracing::field::Empty,
            messaging.system = "kafka",
            messaging.operation = "receive",
            messaging.message.payload_size_bytes = msg.payload_len(),
            messaging.kafka.source.partition = msg.partition(),
            messaging.kafka.message.key = msg.key().and_then(|k| std::str::from_utf8(k).ok()).unwrap_or_default(),
            messaging.kafka.message.offset = msg.offset(),
            convoy.kind = tracing::field::Empty,
        )
    }

    fn worker_hint(&self) -> Option<usize> {
        Some(self.message().partition() as usize)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TopicPartition {
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Offset {
    offset: i64,
    committed: Cell<bool>,
}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}
