//! Distributed tracing with OpenTelemetry, off unless an OTLP endpoint is
//! configured (see `config::TracingConfig`).
//!
//! The gateway's trace spans are `tracing` spans carrying the target
//! `TARGET`. One layer turns exactly those into OpenTelemetry spans, and the
//! log layer is told to ignore them. That split is what keeps the two
//! outputs apart:
//!
//! - Logs stay as they were: the same lines with the same fields, whatever
//!   the trace spans record.
//! - Traces do not depend on `RUST_LOG`: a trace-only span is created
//!   whatever the log level.
//! - With tracing off, no layer accepts these spans, so their callsites are
//!   switched off after the first check and cost next to nothing. No
//!   exporter exists, and no `traceparent` is read or written.
//!
//! Nothing here is global. The propagator is stateless, and the tracer
//! provider belongs to a `Telemetry` value, so tests can run the gateway with
//! and without tracing side by side.
//!
//! The spans themselves, and how they tie into callers and upstream
//! requests, are in `spans`.

use std::time::Duration;

use anyhow::Context as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter as OtlpExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanExporter};
use tracing::{Metadata, Subscriber, warn};
use tracing_subscriber::filter::{FilterExt, filter_fn};
use tracing_subscriber::layer::{Filter, Layer};
use tracing_subscriber::registry::LookupSpan;

use crate::config::{TracesEndpoint, TracingConfig};

mod spans;

pub use spans::{
    adopt_caller, attempt, attempt_failed, attempt_ok, batch, finish_call, inject, link_to_batch,
    models_call, systemone_call,
};

/// The target of every span that goes to the tracing backend instead of the
/// logs.
pub const TARGET: &str = "systemone_gateway::trace";

/// Longest shutdown waits for the last spans to reach the collector. A
/// collector that is down must not keep the gateway from stopping.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// A running exporter: what turns the gateway's spans into OTLP requests.
pub struct Telemetry {
    provider: SdkTracerProvider,
}

impl Telemetry {
    /// Starts exporting to `endpoint` in batches, from a thread of its own
    /// so that a slow collector never slows a call. A full queue drops
    /// spans instead of growing.
    pub fn otlp(config: &TracingConfig, endpoint: &TracesEndpoint) -> anyhow::Result<Self> {
        reqwest::Url::parse(&endpoint.url)
            .with_context(|| format!("{} is not a URL: {:?}", endpoint.source, endpoint.url))?;
        // Headers, timeout and compression come from the standard
        // OTEL_EXPORTER_OTLP_* variables, read by the exporter itself.
        let exporter = OtlpExporter::builder()
            .with_http()
            .with_endpoint(&endpoint.url)
            .build()
            .context("could not set up the OTLP trace exporter")?;
        Ok(Self {
            provider: provider(config).with_batch_exporter(exporter).build(),
        })
    }

    /// Exports each span to `exporter` as soon as it ends, on the thread that
    /// ended it. For tests, with an in-memory exporter.
    pub fn with_exporter(config: &TracingConfig, exporter: impl SpanExporter + 'static) -> Self {
        Self {
            provider: provider(config).with_simple_exporter(exporter).build(),
        }
    }

    /// The layer that turns the gateway's trace spans into OpenTelemetry
    /// spans. Add it to the subscriber next to a log layer filtered with
    /// `logs_filter`.
    pub fn layer<S>(&self) -> impl Layer<S> + use<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.provider.tracer("systemone-gateway"))
            // Source locations, thread ids and busy/idle times are noise
            // in a trace of a gateway.
            .with_location(false)
            .with_threads(false)
            .with_tracked_inactivity(false)
            // A span starts when it first needs an id, which lets a call
            // take its parent from the caller's headers after the span was
            // entered. The gateway never reads OpenTelemetry's ambient
            // context either: every parent is a span it names.
            .with_context_activation(false)
            .with_filter(filter_fn(is_trace_span as fn(&Metadata<'_>) -> bool))
    }

    /// Sends what is still queued and stops the exporter. Blocks for at most
    /// a few seconds, so call it where blocking is allowed: `shutdown` does.
    fn flush_and_stop(&self) {
        if let Err(err) = self.provider.shutdown_with_timeout(FLUSH_TIMEOUT) {
            warn!(error = %err, "could not flush the last trace spans");
        }
    }

    /// Sends the spans still queued. Call it after the servers have stopped,
    /// so that the spans of the last calls are in the queue.
    pub async fn shutdown(self) {
        // The exporter thread is joined here, which would block a runtime
        // thread.
        let _ = tokio::task::spawn_blocking(move || self.flush_and_stop()).await;
    }
}

fn provider(config: &TracingConfig) -> opentelemetry_sdk::trace::TracerProviderBuilder {
    SdkTracerProvider::builder()
        .with_resource(
            Resource::builder()
                .with_service_name(config.service_name.clone())
                .build(),
        )
        // A caller that sampled its trace gets the gateway's spans in it, one
        // that did not gets none, and the traces the gateway starts follow
        // the ratio. Every decision is the caller's first.
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
}

fn is_trace_span(meta: &Metadata<'_>) -> bool {
    meta.is_span() && meta.target() == TARGET
}

fn not_trace_span(meta: &Metadata<'_>) -> bool {
    meta.target() != TARGET
}

/// The filter of the log layer: `filter` as before, minus the trace spans.
pub fn logs_filter<S>(filter: impl Filter<S>) -> impl Filter<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    filter.and(filter_fn(not_trace_span as fn(&Metadata<'_>) -> bool))
}
