use std::error::Error;

use async_trait::async_trait;

use crate::{
    codec::Codec,
    message::{CONTENT_TYPE_HEADER, KIND_HEADER},
    utils::InstrumentWithContext,
    Message, RawHeaders,
};

pub struct MessageProducer<P: Producer, C: Codec> {
    producer: P,
    codec: C,
}

impl<P: Producer, C: Codec> MessageProducer<P, C> {
    pub fn new(producer: P, codec: C) -> Self {
        Self { producer, codec }
    }

    pub async fn produce<M: Message>(
        &self,
        message: M,
        options: P::Options,
    ) -> Result<(), ProducerError<P::Error, C::EncodeError>> {
        let key = message.key().to_owned();

        let (body, headers) = message.into_body_and_headers();

        let payload = self
            .codec
            .encode(body)
            .map_err(ProducerError::EncodeError)?;

        let mut headers: RawHeaders = headers.into();

        headers.insert(KIND_HEADER.to_owned(), M::KIND.to_owned());
        headers.insert(CONTENT_TYPE_HEADER.to_owned(), C::CONTENT_TYPE.to_owned());

        let span = self.producer.make_span(&key, &headers, &payload, &options);

        if cfg!(feature = "opentelemetry") {
            inject_otel_context(&span, &mut headers);
        }

        self.producer
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

#[derive(Debug, thiserror::Error)]
pub enum ProducerError<P: Error, C: Error> {
    #[error("Failed to produce message: {0}")]
    SendError(P),

    #[error("Failed to encode message: {0}")]
    EncodeError(C),
}

#[async_trait]
pub trait Producer: Send + Sync + Sized + 'static {
    type Options: Default + Send;
    type Error: Error;

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
            received.headers.get("x-convoy-kind").unwrap(),
            TestMessage::KIND
        );
        assert_eq!(
            received.headers.get("x-convoy-content-type").unwrap(),
            Json::CONTENT_TYPE
        );

        let body: TestMessageBody = serde_json::from_slice(&received.payload).unwrap();

        assert_eq!(body, expected_body);
    }
}
