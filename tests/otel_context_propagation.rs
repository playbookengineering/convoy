#![cfg(feature = "opentelemetry")]

mod in_memory;
mod otel;
mod schema;

use crate::in_memory::{InMemoryMessageBus, PRODUCE_SPAN_NAME, RECEIVE_SPAN_NAME};
use crate::schema::{Headers, Model, ModelContainer};

use convoy::consumer::IncomingMessage;
use convoy::{
    codec::Json,
    consumer::{Extension, MessageConsumer, WorkerPoolConfig},
    message::Message,
    producer::MessageProducer,
};
use fake::{Fake, Faker};
use opentelemetry::{
    baggage::BaggageExt, sdk::export::trace::SpanData, trace::SpanId, Context, Key,
};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

const TRACEPARENT: &str = "traceparent";
const BAGGAGE_HEADER: &str = "baggage";
const BAGGAGE_KEY: &str = "meaning-of-life";
const BAGGAGE_VAL: i64 = 42;
const ROOT_SPAN_NAME: &str = "root_span";

type Prod = MessageProducer<in_memory::InMemoryProducer, Json>;

// trace_creator is a bridge between our and external system.
// this actor simulates processing incoming payload and passing
// to our system
async fn trace_creator(bus: InMemoryMessageBus, producer: Prod) {
    let mut receiver = bus.into_receiver();

    if let Some(message) = receiver.recv().await {
        let body: Model = serde_json::from_slice(message.payload()).unwrap();

        let container = ModelContainer::from_body_and_headers(body, Headers);

        let span = tracing::info_span!(ROOT_SPAN_NAME);
        span.set_parent(Context::new().with_baggage([Key::new(BAGGAGE_KEY).i64(BAGGAGE_VAL)]));

        async {
            if let Err(err) = producer.produce(container, ()).await {
                tracing::info!("failed to produce: {err}");
            }

            tracing::info!("produced message");
        }
        .instrument(span)
        .await
    }
}

// `trace_consumer` is an actor in our system
// this actor simulates doing some actions on received message
// and forwarding it further
async fn trace_consumer(message: ModelContainer, producer: Extension<Prod>) {
    if let Err(err) = producer.produce(message, ()).await {
        tracing::info!("failed to produce: {err}");
    }

    tracing::info!("processed message");
}

#[tokio::test]
async fn otel_context_propagation() {
    let exporter = otel::init();

    // entry -> [ bus1 -> producer1 ] -> [ bus2 -> producer2 ] -> [ bus3 -> producer3 ] -> out
    let (entry, bus1) = in_memory::make_queue();
    let (producer1, bus2) = in_memory::make_queue();
    let producer1 = MessageProducer::builder(producer1, Json).build();

    // [ bus1 -> producer1 ]
    let trace_creator = trace_creator(bus1, producer1);

    let (producer2, bus3) = in_memory::make_queue();
    let producer2 = MessageProducer::builder(producer2.clone(), Json).build();

    // [ bus2 -> producer2 ]
    let consumer2 = MessageConsumer::new(Json)
        .extension(producer2)
        .message_handler(trace_consumer)
        .listen(bus2, WorkerPoolConfig::fixed(3));

    let (producer3, out) = in_memory::make_queue();
    let producer3 = MessageProducer::builder(producer3.clone(), Json).build();

    // [ bus3 -> producer3 ]
    let consumer3 = MessageConsumer::new(Json)
        .extension(producer3)
        .message_handler(trace_consumer)
        .listen(bus3, WorkerPoolConfig::fixed(3));

    let entry = entry.into_sender();
    let mut out = out.into_receiver();

    // spin them up (pun intended)
    tokio::spawn(trace_creator);
    tokio::spawn(consumer2);
    tokio::spawn(consumer3);

    let model_in: Model = Faker.fake();
    let raw_msg = model_in.marshal();

    // send to the system
    entry.send(raw_msg).await.unwrap();

    // wait for last msg
    let last = out.recv().await.unwrap();

    let baggage = last
        .headers()
        .get(BAGGAGE_HEADER)
        .expect("baggage not exported");

    assert_eq!(*baggage, format!("{BAGGAGE_KEY}={BAGGAGE_VAL}"));

    let traceparent = last
        .headers()
        .get(TRACEPARENT)
        .expect("traceparent not exported");

    assert_eq!(*baggage, format!("{BAGGAGE_KEY}={BAGGAGE_VAL}"));

    // flush exporter pipeline
    opentelemetry::global::shutdown_tracer_provider();

    let spans = exporter.spans();
    let spans = extract_sorted_spans(spans, ROOT_SPAN_NAME);
    let expected_trace_id = spans[0].span_context.trace_id();

    assert!(
        spans
            .iter()
            .all(|span| span.span_context.trace_id() == expected_trace_id),
        "Trace id is not propagated correctly (multiple ids detected)"
    );

    let names = spans.iter().map(|x| x.name.as_ref()).collect::<Vec<_>>();

    assert_eq!(
        names,
        [
            ROOT_SPAN_NAME,
            PRODUCE_SPAN_NAME,
            RECEIVE_SPAN_NAME,
            PRODUCE_SPAN_NAME,
            RECEIVE_SPAN_NAME,
            PRODUCE_SPAN_NAME
        ]
    );

    let last_span = spans.last().unwrap();

    assert!(traceparent.contains(&last_span.span_context.trace_id().to_string()));
    assert!(traceparent.contains(&last_span.span_context.span_id().to_string()));
}

#[track_caller]
fn extract_sorted_spans(mut spans: Vec<SpanData>, root_name: &str) -> Vec<SpanData> {
    let mut sorted = Vec::new();

    let root_idx = spans
        .iter()
        .position(|span| span.parent_span_id == SpanId::INVALID && span.name == root_name)
        .expect("Can't find root");

    let root = spans.remove(root_idx);
    let mut parent_id = root.span_context.span_id();
    sorted.push(root);

    loop {
        let Some(child_idx) = spans
            .iter()
            .position(|span| span.parent_span_id == parent_id)
        else {
            break;
        };

        let child_span = spans.remove(child_idx);
        parent_id = child_span.span_context.span_id();
        sorted.push(child_span)
    }

    sorted
}
