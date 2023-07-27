use std::{
    mem::{self, ManuallyDrop},
    sync::Arc,
};

use async_trait::async_trait;
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    message::{BorrowedMessage, Headers, Message as _Message},
};

use crate::{
    message_bus::{IncomingMessage, MessageBus},
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
