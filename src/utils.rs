use tracing::{instrument::Instrumented, Instrument};

#[cfg(feature = "opentelemetry")]
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[cfg(feature = "opentelemetry")]
use opentelemetry::trace::{FutureExt, WithContext};

#[cfg(feature = "opentelemetry")]
pub trait InstrumentWithContext: Instrument {
    fn instrument_cx(self, span: tracing::Span) -> Instrumented<WithContext<Self>>;
}

#[cfg(feature = "opentelemetry")]
impl<T> InstrumentWithContext for T
where
    T: Instrument,
{
    fn instrument_cx(self, span: tracing::Span) -> Instrumented<WithContext<T>> {
        self.with_context(span.context()).instrument(span)
    }
}

#[cfg(not(feature = "opentelemetry"))]
pub trait InstrumentWithContext: Instrument {
    fn instrument_cx(self, span: tracing::Span) -> Instrumented<Self>;
}

#[cfg(not(feature = "opentelemetry"))]
impl<T> InstrumentWithContext for T
where
    T: Instrument,
{
    fn instrument_cx(self, span: tracing::Span) -> Instrumented<T> {
        self.instrument(span)
    }
}
