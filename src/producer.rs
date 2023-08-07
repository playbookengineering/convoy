mod hook;

use std::{error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::{
    codec::Codec,
    message::{Message, RawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER},
    utils::InstrumentWithContext,
};

use self::hook::Hooks;

pub use self::hook::Hook;

pub struct MessageProducerBuilder<P: Producer, C: Codec> {
    producer: P,
    codec: C,
    hooks: Hooks,
}

impl<P: Producer, C: Codec> MessageProducerBuilder<P, C> {
    pub fn new(producer: P, codec: C) -> Self {
        Self {
            producer,
            codec,
            hooks: Hooks::default(),
        }
    }

    pub fn hook<H>(mut self, hook: H) -> Self
    where
        H: Hook,
    {
        self.hooks = self.hooks.push(hook);
        self
    }

    pub fn build(self) -> MessageProducer<P, C> {
        let this = Arc::new(self);
        MessageProducer(this)
    }
}

pub struct MessageProducer<P: Producer, C: Codec>(Arc<MessageProducerBuilder<P, C>>);

impl<P: Producer, C: Codec> Clone for MessageProducer<P, C> {
    fn clone(&self) -> Self {
        let incref = Arc::clone(&self.0);
        Self(incref)
    }
}

impl<P: Producer, C: Codec> MessageProducer<P, C> {
    pub fn builder(producer: P, codec: C) -> MessageProducerBuilder<P, C> {
        MessageProducerBuilder::new(producer, codec)
    }

    pub async fn produce<M: Message>(
        &self,
        message: M,
        options: P::Options,
    ) -> Result<(), ProducerError> {
        self.0.hooks.before_send();

        let result = self.produce_impl(message, options).await;
        self.0.hooks.after_send(&result);
        result
    }

    async fn produce_impl<M: Message>(
        &self,
        message: M,
        options: P::Options,
    ) -> Result<(), ProducerError> {
        let this = &self.0;
        let key = message.key();

        let (body, headers) = message.into_body_and_headers();

        let payload = this
            .codec
            .encode(body)
            .map_err(|err| ProducerError::EncodeError(err.into()))?;

        let mut headers: RawHeaders = headers.into();

        headers.insert(KIND_HEADER.to_owned(), M::KIND.to_owned());
        headers.insert(CONTENT_TYPE_HEADER.to_owned(), C::CONTENT_TYPE.to_owned());

        let span = this.producer.make_span(&key, &headers, &payload, &options);

        if cfg!(feature = "opentelemetry") {
            inject_otel_context(&span, &mut headers);
        }

        this.producer
            .send(key, headers, payload, options)
            .instrument_cx(span)
            .await
            .map_err(|err| ProducerError::SendError(err.into()))
    }
}

#[cfg(feature = "opentelemetry")]
#[inline(always)]
fn inject_otel_context(span: &tracing::Span, headers: &mut RawHeaders) {
    use opentelemetry::global::get_text_map_propagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let context = span.context();
    get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, headers);
    });
}

#[cfg(not(feature = "opentelemetry"))]
#[inline(always)]
fn inject_otel_context(_: &tracing::Span, _: &mut RawHeaders) {}

#[derive(Debug, thiserror::Error)]
pub enum ProducerError {
    #[error("Failed to produce message: {0}")]
    SendError(Box<dyn Error + Send + Sync>),

    #[error("Failed to encode message: {0}")]
    EncodeError(Box<dyn Error + Send + Sync>),
}

#[async_trait]
pub trait Producer: Send + Sync + 'static {
    type Options: Default + Send;
    type Error: Error + Send + Sync + 'static;

    async fn send(
        &self,
        key: String,
        headers: RawHeaders,
        payload: Vec<u8>,
        options: Self::Options,
    ) -> Result<(), Self::Error>;

    fn make_span(
        &self,
        key: &str,
        headers: &RawHeaders,
        payload: &[u8],
        options: &Self::Options,
    ) -> tracing::Span;
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

    use crate::{
        codec::Json,
        test::{Meta, TestMessage, TestMessageBody},
    };

    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("error occurred")]
    pub struct TestProducerError;

    #[derive(Debug)]
    struct TestWireMessage {
        key: String,
        headers: RawHeaders,
        payload: Vec<u8>,
    }

    struct TestProducer {
        sender: UnboundedSender<TestWireMessage>,
        is_ok: bool,
    }

    fn message_producer_with_receiver(
        is_ok: bool,
    ) -> (
        MessageProducerBuilder<TestProducer, Json>,
        UnboundedReceiver<TestWireMessage>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();

        let producer = TestProducer { sender: tx, is_ok };
        let producer = MessageProducer::builder(producer, Json);

        (producer, rx)
    }

    #[async_trait]
    impl Producer for TestProducer {
        type Options = ();
        type Error = TestProducerError;

        async fn send(
            &self,
            key: String,
            headers: RawHeaders,
            payload: Vec<u8>,
            _: Self::Options,
        ) -> Result<(), Self::Error> {
            if !self.is_ok {
                return Err(TestProducerError);
            }

            self.sender
                .send(TestWireMessage {
                    key,
                    headers,
                    payload,
                })
                .unwrap();

            Ok(())
        }

        fn make_span(
            &self,
            _key: &str,
            _headers: &RawHeaders,
            _payload: &[u8],
            _options: &Self::Options,
        ) -> tracing::Span {
            tracing::info_span!(
                "producer",
                otel.name = "test send",
                otel.kind = "PRODUCER",
                otel.status_code = tracing::field::Empty,
                messaging.system = "kafka",
                messaging.destination = "memory",
                messaging.destination_kind = "topic",
            )
        }
    }

    #[tokio::test]
    async fn produce_message() {
        let expected_key = "qwerty";
        let expected_body = TestMessageBody {
            id: expected_key.to_owned(),
            data: "asdfg".to_string(),
        };

        let msg = TestMessage(expected_body.clone(), Meta);

        let (producer, mut rx) = message_producer_with_receiver(true);
        let producer = producer.build();

        producer.produce(msg, ()).await.unwrap();

        let received = rx.recv().await.unwrap();

        assert_eq!(received.key, expected_key);
        assert_eq!(
            received.headers.get(KIND_HEADER).unwrap(),
            TestMessage::KIND
        );

        assert_eq!(
            received.headers.get(CONTENT_TYPE_HEADER).unwrap(),
            Json::CONTENT_TYPE
        );

        let body: TestMessageBody = serde_json::from_slice(&received.payload).unwrap();

        assert_eq!(body, expected_body);
    }

    #[tokio::test]
    async fn produce_message_with_hooks() {
        #[derive(Default)]
        struct TestHook {
            counter_before: AtomicUsize,
            counter_after_ok: AtomicUsize,
            counter_after_err: AtomicUsize,
        }

        impl Hook for TestHook {
            fn before_send(&self) {
                self.counter_before.fetch_add(1, Ordering::Relaxed);
            }

            fn after_send(&self, res: &Result<(), ProducerError>) {
                let counter = if res.is_ok() {
                    &self.counter_after_ok
                } else {
                    &self.counter_after_err
                };

                counter.fetch_add(1, Ordering::Relaxed);
            }
        }

        let expected_key = "qwerty";
        let expected_body = TestMessageBody {
            id: expected_key.to_owned(),
            data: "asdfg".to_string(),
        };

        let msg = TestMessage(expected_body.clone(), Meta);

        let test_hook = Arc::new(TestHook::default());

        let (producer, _rx) = message_producer_with_receiver(true);
        let producer = producer.hook(Arc::clone(&test_hook)).build();

        producer.produce(msg.clone(), ()).await.unwrap();

        assert_eq!(test_hook.counter_before.load(Ordering::Relaxed), 1);
        assert_eq!(test_hook.counter_after_ok.load(Ordering::Relaxed), 1);

        let (producer, _rx) = message_producer_with_receiver(false);
        let producer = producer.hook(Arc::clone(&test_hook)).build();
        assert!(producer.produce(msg, ()).await.is_err());

        assert_eq!(test_hook.counter_before.load(Ordering::Relaxed), 2);
        assert_eq!(test_hook.counter_after_ok.load(Ordering::Relaxed), 1);
        assert_eq!(test_hook.counter_after_err.load(Ordering::Relaxed), 1);
    }
}
