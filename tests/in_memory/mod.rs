use std::{
    convert::Infallible,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use convoy::{
    consumer::{IncomingMessage, MessageBus},
    message::RawHeaders,
    producer::Producer,
};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex,
};

pub fn make_queue() -> (InMemoryProducer, InMemoryMessageBus) {
    let (tx, rx) = channel(16);

    let producer = InMemoryProducer(tx);
    let message_bus = InMemoryMessageBus {
        rx: Mutex::new(rx),
        ack: Default::default(),
        nack: Default::default(),
        reject: Default::default(),
    };

    (producer, message_bus)
}

pub struct InMemoryMessageBus {
    rx: Mutex<Receiver<InMemoryMessage>>,
    ack: AtomicUsize,
    nack: AtomicUsize,
    reject: AtomicUsize,
}

#[allow(unused)]
impl InMemoryMessageBus {
    pub fn acks(&self) -> usize {
        self.ack.load(Ordering::Relaxed)
    }

    pub fn nacks(&self) -> usize {
        self.nack.load(Ordering::Relaxed)
    }

    pub fn rejects(&self) -> usize {
        self.reject.load(Ordering::Relaxed)
    }

    pub fn into_receiver(self) -> Receiver<InMemoryMessage> {
        self.rx.into_inner()
    }
}

#[async_trait::async_trait]
impl MessageBus for InMemoryMessageBus {
    type IncomingMessage = InMemoryMessage;
    type Error = Infallible;

    async fn recv(&self) -> Result<Self::IncomingMessage, Self::Error> {
        let mut rx = self.rx.lock().await;
        Ok(rx.recv().await.unwrap())
    }

    async fn ack(&self, _message: &Self::IncomingMessage) -> Result<(), Self::Error> {
        self.ack.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn nack(&self, _message: &Self::IncomingMessage) -> Result<(), Self::Error> {
        self.nack.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn reject(&self, _message: &Self::IncomingMessage) -> Result<(), Self::Error> {
        self.reject.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

pub struct InMemoryMessage {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub headers: RawHeaders,
}

impl IncomingMessage for InMemoryMessage {
    fn headers(&self) -> RawHeaders {
        self.headers.clone()
    }

    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn make_span(&self) -> tracing::Span {
        tracing::info_span!("msg receive")
    }
}

#[derive(Clone)]
pub struct InMemoryProducer(Sender<InMemoryMessage>);

impl InMemoryProducer {
    pub fn into_sender(self) -> Sender<InMemoryMessage> {
        self.0
    }
}

#[async_trait]
impl Producer for InMemoryProducer {
    type Options = ();

    type Error = Infallible;

    async fn send(
        &self,
        key: String,
        headers: RawHeaders,
        payload: Vec<u8>,
        _: Self::Options,
    ) -> Result<(), Self::Error> {
        let message = InMemoryMessage {
            key: Some(key),
            payload,
            headers,
        };

        self.0.send(message).await.unwrap();

        Ok(())
    }

    fn make_span(
        &self,
        _key: &str,
        _headers: &RawHeaders,
        _payload: &[u8],
        _options: &Self::Options,
    ) -> tracing::Span {
        tracing::info_span!("msg produce")
    }
}
