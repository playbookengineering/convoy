use std::{
    env,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use futures_lite::future::Boxed;
use opentelemetry::{
    global::{set_text_map_propagator, set_tracer_provider},
    sdk::{
        export::trace::{ExportResult, SpanData, SpanExporter},
        propagation::{BaggagePropagator, TextMapCompositePropagator, TraceContextPropagator},
        trace::TracerProvider,
    },
    trace::TracerProvider as _,
};

use tracing::metadata::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Default, Clone)]
pub struct Exporter {
    spans: Arc<Mutex<Vec<SpanData>>>,
}

impl Exporter {
    pub fn spans(&self) -> Vec<SpanData> {
        self.spans.lock().unwrap().clone()
    }
}

impl Debug for Exporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Exporter").finish()
    }
}

impl SpanExporter for Exporter {
    fn export(
        &mut self,
        batch: Vec<SpanData>,
    ) -> Boxed<opentelemetry::sdk::export::trace::ExportResult> {
        self.spans.lock().unwrap().extend(batch);

        Box::pin(async move { ExportResult::Ok(()) })
    }
}

pub fn init() -> Exporter {
    let tracecontext = TraceContextPropagator::new();
    let baggage = BaggagePropagator::new();

    let propagator =
        TextMapCompositePropagator::new(vec![Box::new(tracecontext), Box::new(baggage)]);

    set_text_map_propagator(propagator);

    let disable_color = matches!(env::var("NO_COLOR"), Ok(s) if !s.is_empty());

    let exporter = Exporter::default();

    let provider = TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();

    let tracer = provider.tracer("start");

    // needed for propagation
    set_tracer_provider(provider);

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(telemetry)
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .with(tracing_subscriber::fmt::layer().with_ansi(!disable_color))
        .init();

    exporter
}
