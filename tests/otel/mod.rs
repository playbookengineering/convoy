use std::env;

use opentelemetry::{
    global::set_text_map_propagator,
    sdk::propagation::{BaggagePropagator, TextMapCompositePropagator, TraceContextPropagator},
};

use opentelemetry_otlp::WithExportConfig;
use tracing::metadata::LevelFilter;
use tracing_subscriber::{prelude::*, EnvFilter};

pub fn init() {
    let tracecontext = TraceContextPropagator::new();
    let baggage = BaggagePropagator::new();

    let propagator =
        TextMapCompositePropagator::new(vec![Box::new(tracecontext), Box::new(baggage)]);

    set_text_map_propagator(propagator);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
        .install_batch(opentelemetry::runtime::Tokio)
        .unwrap();

    let disable_color = matches!(env::var("NO_COLOR"), Ok(s) if !s.is_empty());

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
}
