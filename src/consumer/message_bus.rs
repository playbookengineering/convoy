use std::error::Error;

use async_trait::async_trait;
use futures_lite::Stream;

use crate::message::RawHeaders;

#[async_trait]
pub trait MessageBus: Send + Sync + 'static {
    type IncomingMessage: IncomingMessage<Error = Self::Error>;
    type Error: Error + Send + Sync + 'static;
    type Stream: Stream<Item = Result<Self::IncomingMessage, Self::Error>> + Unpin;

    /// Returns stream of messages
    async fn into_stream(self) -> Result<Self::Stream, Self::Error>;
}

#[async_trait]
pub trait IncomingMessage: Send + Sync + 'static {
    type Error: Error + Send + Sync + 'static;

    fn headers(&self) -> RawHeaders;

    fn payload(&self) -> &[u8];

    fn key(&self) -> Option<&[u8]>;

    fn make_span(&self) -> tracing::Span;

    /// Acknowledge message and dequeue
    async fn ack(&self) -> Result<(), Self::Error>;

    /// Negative acknowledgement, don't dequeue
    async fn nack(&self) -> Result<(), Self::Error>;

    /// Reject message and dequeue
    async fn reject(&self) -> Result<(), Self::Error>;
}
