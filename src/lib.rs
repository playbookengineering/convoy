pub mod codec;
pub mod consumer;
pub mod integration;
pub mod message;
pub mod producer;
pub mod utils;

#[cfg(test)]
pub(crate) mod test {
    use std::{
        convert::Infallible,
        pin::Pin,
        task::{Context, Poll},
    };

    use async_trait::async_trait;
    use futures_lite::Stream;
    use serde::{Deserialize, Serialize};

    use crate::message::FromMessageParts;
    use crate::{
        codec::{Codec, Json},
        consumer::{IncomingMessage, MessageBus},
        message::{Message, RawHeaders, CONTENT_TYPE_HEADER, KIND_HEADER},
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TestMessage(pub TestMessageBody, pub Meta);

    impl TestMessage {
        pub fn new(num: usize) -> Self {
            Self(
                TestMessageBody {
                    id: format!("test{num}"),
                    data: format!("test payload {num}"),
                },
                Meta,
            )
        }
    }

    impl From<TestMessageBody> for TestMessage {
        fn from(value: TestMessageBody) -> Self {
            Self(value, Meta)
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    pub struct Meta;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
    pub struct TestMessageBody {
        pub id: String,
        pub data: String,
    }

    impl Message for TestMessage {
        const KIND: &'static str = "my-message.v1";

        type Body = TestMessageBody;
        type Headers = Meta;

        fn key(&self) -> String {
            self.0.id.clone()
        }
    }

    impl FromMessageParts for TestMessage {
        fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self {
            Self(body, headers)
        }
    }

    impl From<TestMessage> for (TestMessageBody, Meta) {
        fn from(value: TestMessage) -> Self {
            (value.0, value.1)
        }
    }

    impl From<RawHeaders> for Meta {
        fn from(_: RawHeaders) -> Self {
            Self
        }
    }

    impl From<Meta> for RawHeaders {
        fn from(_: Meta) -> Self {
            RawHeaders::default()
        }
    }

    #[derive(Clone)]
    pub struct TestIncomingMessage {
        pub key: Option<String>,
        pub payload: Vec<u8>,
        pub headers: RawHeaders,
    }

    impl TestIncomingMessage {
        pub fn create_raw_json(message: TestMessage) -> Self {
            let key = Some(message.key());

            let mut headers = RawHeaders::default();
            headers.insert(KIND_HEADER.to_owned(), TestMessage::KIND.to_owned());
            headers.insert(
                CONTENT_TYPE_HEADER.to_owned(),
                Json::CONTENT_TYPE.to_owned(),
            );

            let payload = serde_json::to_vec(&message.0).unwrap();

            Self {
                key,
                payload,
                headers,
            }
        }
    }

    #[async_trait]
    impl IncomingMessage for TestIncomingMessage {
        type Error = Infallible;

        fn headers(&self) -> RawHeaders {
            self.headers.clone()
        }

        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn key(&self) -> Option<&[u8]> {
            self.key.as_ref().map(|k| k.as_bytes())
        }

        fn make_span(&self) -> tracing::Span {
            tracing::info_span!(
                "consumer",
                otel.name = "test message receive",
                otel.kind = "CONSUMER",
                otel.status_code = tracing::field::Empty,
                messaging.system = "test",
                messaging.operation = "receive",
                messaging.message.payload_size_bytes = self.payload.len(),
                messaging.test.message.key = self
                    .key()
                    .and_then(|k| std::str::from_utf8(k).ok())
                    .unwrap_or_default(),
                convoy.kind = tracing::field::Empty,
            )
        }

        async fn ack(&self) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn nack(&self) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn reject(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    pub struct TestMessageBusStream;

    impl Stream for TestMessageBusStream {
        type Item = Result<TestIncomingMessage, Infallible>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            unimplemented!("not needed in test scenario")
        }
    }

    pub struct TestMessageBus;

    #[async_trait::async_trait]
    impl MessageBus for TestMessageBus {
        type IncomingMessage = TestIncomingMessage;
        type Error = Infallible;
        type Stream = TestMessageBusStream;

        async fn into_stream(self) -> Result<Self::Stream, Self::Error> {
            Ok(TestMessageBusStream)
        }
    }
}
