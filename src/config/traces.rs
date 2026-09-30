//! The `[tracing]` table: whether the gateway exports traces, where to, and
//! how many of them.

use anyhow::{Context, ensure};
use serde::Deserialize;

/// Set in the environment by most OpenTelemetry tooling: a base URL, to which
/// the signal path is added.
const ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// The same for traces only: the full URL, used as it is.
const TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
/// Where OTLP over HTTP takes traces.
const TRACES_PATH: &str = "/v1/traces";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TracingConfig {
    /// Base URL of an OTLP/HTTP collector, such as `http://localhost:4318`.
    /// Unset turns export off, unless the environment names an endpoint (see
    /// `endpoint`). With no endpoint at all no exporter is created, no
    /// header is read or written, and spans cost next to nothing.
    pub otlp_endpoint: Option<String>,
    /// How the gateway shows up in the tracing backend.
    pub service_name: String,
    /// Share of the traces the gateway starts that are kept, from 0 to 1. A
    /// call that arrives with a `traceparent` follows its caller's decision
    /// instead (parent-based sampling).
    pub sample_ratio: f64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            service_name: "systemone-gateway".to_owned(),
            sample_ratio: 1.0,
        }
    }
}

/// The URL traces are posted to, and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TracesEndpoint {
    pub url: String,
    /// The setting that named it, for messages.
    pub source: &'static str,
}

impl TracingConfig {
    /// Where to post traces, or `None` when tracing is off. The file wins
    /// over the environment, like every programmatic setting does in
    /// OpenTelemetry: `otlp_endpoint`, then `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`,
    /// then `OTEL_EXPORTER_OTLP_ENDPOINT`. The file value and the generic
    /// variable are base URLs and get `/v1/traces` added; the traces variable
    /// is a full URL. An empty variable counts as unset.
    pub fn endpoint(&self, env: &dyn Fn(&str) -> Option<String>) -> Option<TracesEndpoint> {
        let set = |name: &str| {
            env(name)
                .map(|v| v.trim().to_owned())
                .filter(|v| !v.is_empty())
        };
        if let Some(base) = &self.otlp_endpoint {
            return Some(TracesEndpoint {
                url: with_traces_path(base),
                source: "tracing.otlp_endpoint",
            });
        }
        if let Some(url) = set(TRACES_ENDPOINT_ENV) {
            return Some(TracesEndpoint {
                url,
                source: TRACES_ENDPOINT_ENV,
            });
        }
        set(ENDPOINT_ENV).map(|base| TracesEndpoint {
            url: with_traces_path(&base),
            source: ENDPOINT_ENV,
        })
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            !self.service_name.trim().is_empty(),
            "tracing.service_name must not be empty"
        );
        ensure!(
            self.sample_ratio.is_finite() && (0.0..=1.0).contains(&self.sample_ratio),
            "tracing.sample_ratio must be a number from 0 to 1"
        );
        if let Some(endpoint) = &self.otlp_endpoint {
            let url =
                reqwest::Url::parse(endpoint).context("tracing.otlp_endpoint is not a URL")?;
            ensure!(
                matches!(url.scheme(), "http" | "https") && url.has_host(),
                "tracing.otlp_endpoint must be an http or https URL"
            );
        }
        Ok(())
    }
}

fn with_traces_path(base: &str) -> String {
    format!("{}{TRACES_PATH}", base.trim().trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        let pairs = pairs.to_vec();
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        }
    }

    fn config(otlp_endpoint: Option<&str>) -> TracingConfig {
        TracingConfig {
            otlp_endpoint: otlp_endpoint.map(str::to_owned),
            ..TracingConfig::default()
        }
    }

    #[test]
    fn nothing_set_means_tracing_is_off() {
        assert_eq!(config(None).endpoint(&env(&[])), None);
        // An empty variable is how a deployment unsets one.
        let empty = env(&[(ENDPOINT_ENV, " "), (TRACES_ENDPOINT_ENV, "")]);
        assert_eq!(config(None).endpoint(&empty), None);
    }

    #[test]
    fn the_file_wins_over_the_environment() {
        let vars = env(&[
            (TRACES_ENDPOINT_ENV, "http://env:4318/v1/traces"),
            (ENDPOINT_ENV, "http://generic:4318"),
        ]);
        let found = config(Some("http://file:4318/")).endpoint(&vars).unwrap();
        assert_eq!(found.url, "http://file:4318/v1/traces");
        assert_eq!(found.source, "tracing.otlp_endpoint");
    }

    #[test]
    fn the_traces_variable_is_a_full_url_and_the_generic_one_a_base() {
        let both = env(&[
            (TRACES_ENDPOINT_ENV, "http://traces:4318/custom/path"),
            (ENDPOINT_ENV, "http://generic:4318"),
        ]);
        let found = config(None).endpoint(&both).unwrap();
        assert_eq!(found.url, "http://traces:4318/custom/path");
        assert_eq!(found.source, TRACES_ENDPOINT_ENV);

        let generic = env(&[(ENDPOINT_ENV, "http://generic:4318/otlp/")]);
        let found = config(None).endpoint(&generic).unwrap();
        assert_eq!(found.url, "http://generic:4318/otlp/v1/traces");
        assert_eq!(found.source, ENDPOINT_ENV);
    }

    #[test]
    fn validation_refuses_what_cannot_work() {
        assert!(config(Some("http://localhost:4318")).validate().is_ok());
        assert!(config(Some("not a url")).validate().is_err());
        assert!(config(Some("ftp://localhost")).validate().is_err());
        for ratio in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            let bad = TracingConfig {
                sample_ratio: ratio,
                ..TracingConfig::default()
            };
            assert!(bad.validate().is_err(), "{ratio}");
        }
        let nameless = TracingConfig {
            service_name: " ".to_owned(),
            ..TracingConfig::default()
        };
        assert!(nameless.validate().is_err());
    }
}
