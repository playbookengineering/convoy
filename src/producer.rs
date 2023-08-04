use std::{error::Error, fmt::Debug, sync::Arc};

use async_trait::async_trait;

use crate::{
    codec::Codec,
    message::{Message, RawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER},
    utils::InstrumentWithContext,
};

pub struct MessageProducer<P: Producer, C: Codec>(Arc<MessageProducerInner<P, C>>);

struct MessageProducerInner<P, C> {
    producer: P,
    codec: C,
}

impl<P: Producer, C: Codec> Clone for MessageProducer<P, C> {
    fn clone(&self) -> Self {
        let incref = Arc::clone(&self.0);
        Self(incref)
    }
}

impl<P: Producer, C: Codec> MessageProducer<P, C> {
    pub fn new(producer: P, codec: C) -> Self {
        let inner = MessageProducerInner { producer, codec };

        Self(Arc::new(inner))
    }

    pub async fn produce<M: Message>(
        &self,
        message: M,
        options: P::Options,
    ) -> Result<(), ProducerError<P, C>> {
        let this = &self.0;
        let key = message.key().to_owned();

        let (body, headers) = message.into_body_and_headers();

        let payload = this
            .codec
            .encode(body)
            .map_err(ProducerError::EncodeError)?;

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
            .map_err(ProducerError::SendError)
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

#[derive(thiserror::Error)]
pub enum ProducerError<P: Producer, C: Codec> {
    #[error("Failed to produce message: {0}")]
    SendError(P::Error),

    #[error("Failed to encode message: {0}")]
    EncodeError(C::EncodeError),
}

impl<P: Producer, C: Codec> Debug for ProducerError<P, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerError::SendError(s) => Debug::fmt(&s, f),
            ProducerError::EncodeError(e) => Debug::fmt(&e, f),
        }
    }
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
    use std::convert::Infallible;

    use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

    use crate::{
        codec::Json,
        test::{Meta, TestMessage, TestMessageBody},
    };

    use super::*;

    #[derive(Debug)]
    struct TestWireMessage {
        key: String,
        headers: RawHeaders,
        payload: Vec<u8>,
    }

    struct TestProducer {
        sender: UnboundedSender<TestWireMessage>,
    }

    fn message_producer_with_receiver() -> (
        MessageProducer<TestProducer, Json>,
        UnboundedReceiver<TestWireMessage>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();

        let producer = TestProducer { sender: tx };
        let producer = MessageProducer::new(producer, Json);

        (producer, rx)
    }

    #[async_trait]
    impl Producer for TestProducer {
        type Options = ();
        type Error = Infallible;

        async fn send(
            &self,
            key: String,
            headers: RawHeaders,
            payload: Vec<u8>,
            _: Self::Options,
        ) -> Result<(), Self::Error> {
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

        let (producer, mut rx) = message_producer_with_receiver();

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
}
