# convoy

# Introduction

Convoy is a message bus framework designed with ergonomics and simplicty in mind.

# High level features

Mesage Consumer:

- Route messages to handlers
- Detect message kind and body content type basing on associated metadata
    * _TODO_: for now only `Json` is supported, but other formats shall be implemented in the future.
- Declaratively parse inbound messages using `TryExtract` trait
- Parallelize consumption preserving your message bus idempotence guarantees

Message Producer:

- Send messages to defined destination
- Serialize message body and headers in a bus-agnostic way

# Integrations

Both `producer` and `consumer` integrates with Apache Kafka (this library ships with [`rust-rdkafka`] integration).

OpenTelemetry (aka. [OTel]) context propagation support is gated by
`opentelemetry` feature. Span/event creation is handled by [`tracing`],
OTel layer is supplied by [`tracing-opentelemetry`].

💡 It is up to user to initialize OTel library. If you are new to [OTel], please check
[`tracing-opentelemetry` examples]

# Concepts

## Message

User experience is built around `Message` trait. `Message` defines schema for exchanged piece of information.

```rust
use convoy::message::{Message, RawHeaders};
use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub struct MyMessage {
    pub metadata: Metadata,
    pub body: Entity,
}

#[derive(Debug)]
pub struct Metadata {
    pub created_at_millis: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Entity {
    pub id: u32,
    pub first_name: String,
    pub second_name: String,
}

impl Message for MyMessage {
    // Provide unique message descriptor.
    // `KIND` is passed along headers by producer.
    // This makes routing possible.
    const KIND: &'static str = "my-message.v1";

    // Message body
    // Requires Serialize + DeserializeOwned bounds,
    type Body = Entity;

    // Message headers
    // Requires impls bounds are:
    // * TryFrom<RawHeaders>
    // * TryFrom<RawHeaders>::Error: Error
    // * Into<RawHeaders>
    type Headers = Metadata;

    // Instructs framework how to build our message type
    fn from_body_and_headers(body: Self::Body, headers: Self::Headers) -> Self {
        Self {
            metadata: headers,
            body,
        }
    }

    // Instructs framework how to destruct our message type
    fn into_body_and_headers(self) -> (Self::Body, Self::Headers) {
        (self.body, self.metadata)
    }

    // Identity key
    fn key(&self) -> String {
        self.body.id.to_string()
    }
}

(...)
```

can be found in following example: `examples/kafka_message.rs`

💡 It is a good idea to roll your own messages crate

## Message bus

A message bus is a communication system that allows different software components or services to exchange
information by sending messages to a channel or hub. It acts as an intermediary, facilitating
asynchronous communication between various parts of a software system,
enabling decoupled and scalable interactions between them.
After successful subscription towards message bus, application is capable of
consuming a data stream of interest.

`convoy` requires providing integration layer for implementing message bus support
of your choice; traits `MessageBus` and `IncomingMessage` have to be implemented
and you're ready to go!

# License

This project is licensed under the MIT License

[`rust-rdkafka`]: https://crates.io/crates/rdkafka
[OTel]: https://opentelemetry.io/
[`tracing`]: https://crates.io/crates/tracing
[`tracing-opentelemetry`]: https://crates.io/crates/tracing-opentelemetry
[`tracing-opentelemetry` examples]: https://github.com/tokio-rs/tracing-opentelemetry/tree/v0.1.x/examples