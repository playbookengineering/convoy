#[cfg(feature = "opentelemetry")]
mod in_memory;
#[cfg(feature = "opentelemetry")]
mod otel;
#[cfg(feature = "opentelemetry")]
mod schema;

#[cfg(feature = "opentelemetry")]
mod otel_test {
    use convoy::{
        codec::Json,
        consumer::{Extension, Hook, MessageConsumer, ProcessContext, WorkerPoolConfig},
        producer::MessageProducer,
        utils::InstrumentWithContext,
    };
    use fake::{Fake, Faker};
    use opentelemetry::{baggage::BaggageExt, Context, Key};
    use tokio::sync::mpsc::{self, UnboundedSender};
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use crate::{
        in_memory, otel,
        schema::{Model, ModelContainer},
    };

    type Prod = MessageProducer<in_memory::InMemoryProducer, Json>;

    struct TraceParentSender(UnboundedSender<String>);

    impl Hook for TraceParentSender {
        fn on_processing_start(&self, ctx: &mut ProcessContext<'_>) {
            let headers = ctx.headers();

            if let Some(traceparent) = headers.get("traceparent") {
                let _ = self.0.send(traceparent.to_string());
            }
        }
    }

    async fn trace_creator(message: ModelContainer, producer: Extension<Prod>) {
        let span = tracing::info_span!("new span");
        span.set_parent(Context::new().with_baggage([Key::new("meaning-of-life").i64(42)]));

        async {
            if let Err(err) = producer.produce(message, ()).await {
                tracing::info!("failed to produce: {err}");
            }
        }
        .instrument_cx(span)
        .await
    }

    async fn trace_consumer(message: ModelContainer, producer: Extension<Prod>) {
        if let Err(err) = producer.produce(message, ()).await {
            tracing::info!("failed to produce: {err}");
        }
    }

    #[tokio::test]
    async fn otel_context_propagation() {
        otel::init();

        let (trace_tx, mut trace_rx) = mpsc::unbounded_channel::<String>();

        // entry -> [ bus1 -> producer1 ] -> [ bus2 -> producer2 ] -> [ bus3 -> producer3 ] -> out
        let (entry, bus1) = in_memory::make_queue();
        let (producer1, bus2) = in_memory::make_queue();
        let producer1 = MessageProducer::new(producer1, Json);

        // [ bus1 -> producer1 ]
        let consumer1 = MessageConsumer::default()
            .extension(producer1)
            .message_handler(trace_creator)
            .listen(WorkerPoolConfig::fixed(3), bus1);

        let (producer2, bus3) = in_memory::make_queue();
        let producer2 = MessageProducer::new(producer2.clone(), Json);

        // [ bus2 -> producer2 ]
        let consumer2 = MessageConsumer::default()
            .extension(producer2)
            .hook(TraceParentSender(trace_tx.clone()))
            .message_handler(trace_consumer)
            .listen(WorkerPoolConfig::fixed(3), bus2);

        let (producer3, out) = in_memory::make_queue();
        let producer3 = MessageProducer::new(producer3.clone(), Json);

        // [ bus3 -> producer3 ]
        let consumer3 = MessageConsumer::default()
            .extension(producer3)
            .hook(TraceParentSender(trace_tx))
            .message_handler(trace_consumer)
            .listen(WorkerPoolConfig::fixed(3), bus3);

        let entry = entry.into_sender();
        let mut out = out.into_receiver();

        // spin them up (pun intended)
        tokio::spawn(consumer1);
        tokio::spawn(consumer2);
        tokio::spawn(consumer3);

        let model_in: Model = Faker.fake();
        let raw_msg = model_in.marshal();

        entry.send(raw_msg).await.unwrap();
        let recv = out.recv().await.unwrap();

        let first_traceparent = trace_rx.recv().await.unwrap();
        let second_traceparent = trace_rx.recv().await.unwrap();
        let last_traceparent = recv.headers.get("traceparent").unwrap();

        let first_trace_id = extract_trace_id_from_traceparent(&first_traceparent);
        let second_trace_id = extract_trace_id_from_traceparent(&second_traceparent);
        let last_trace_id = extract_trace_id_from_traceparent(last_traceparent);

        assert_eq!(first_trace_id, second_trace_id);
        assert_eq!(second_trace_id, last_trace_id);
    }

    fn extract_trace_id_from_traceparent(traceparent: &str) -> &str {
        traceparent.split('-').next().unwrap()
    }
}
