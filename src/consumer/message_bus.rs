use std::error::Error;

use async_trait::async_trait;

use crate::message::RawHeaders;

#[async_trait]
pub trait MessageBus: Send + Sync + 'static {
    type IncomingMessage: IncomingMessage;
    type Error: Error + Send + Sync + 'static;

    /// Receive one message from the bus
    async fn recv(&self) -> Result<Self::IncomingMessage, Self::Error>;

    /// Acknowledge message and dequeue
    async fn ack(&self, message: &Self::IncomingMessage) -> Result<(), Self::Error>;

    /// Negative acknowledgement, don't dequeue
    async fn nack(&self, message: &Self::IncomingMessage) -> Result<(), Self::Error>;

    /// Reject message and dequeue
    async fn reject(&self, message: &Self::IncomingMessage) -> Result<(), Self::Error>;
}

pub trait IncomingMessage: Send + Sync + 'static {
    fn headers(&self) -> RawHeaders;

    fn payload(&self) -> &[u8];

    fn key(&self) -> Option<&str>;

    fn make_span(&self) -> tracing::Span;
}
