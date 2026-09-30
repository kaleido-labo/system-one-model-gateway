//! End-to-end tests of the optional shared rate limiter: replicas that keep
//! their pacing in Redis, and a gateway that carries on when Redis is gone.
//!
//! The tests that need a real Redis (or Valkey) run when
//! `SYSTEMONE_GATEWAY_TEST_REDIS_URL` holds its URL, and are skipped
//! otherwise. The others point the gateway at a port where nothing listens.

mod common;

use std::time::Duration;

use common::{MockUpstream, Reply, UPSTREAM_KEY, reply};
use serde_json::json;
use systemone_gateway::{Config, Gateway, hash_key};

const TEST_REDIS_URL: &str = "SYSTEMONE_GATEWAY_TEST_REDIS_URL";
const SERVICE_KEY: &str = "key-ocr";

/// One gateway replica in front of the mock.
struct Replica {
    gateway: Gateway,
    client: reqwest::Client,
}

/// What differs between the gateways a test starts.
#[derive(Default)]
struct Settings<'a> {
    /// Extra lines for the `[[backend]]` block.
    backend: &'a str,
    /// Extra lines for the `[[service]]` block.
    service: &'a str,
    /// The body of a `[cluster]` table; `None` leaves the table out.
    cluster: Option<&'a str>,
    /// What the environment variable named by `redis_url_env` holds.
    redis_url: Option<String>,
}

async fn start(mock: &MockUpstream, settings: Settings<'_>) -> anyhow::Result<Replica> {
    let mut toml = format!(
        "[server]\nlisten = \"127.0.0.1:0\"\nadmin_listen = \"127.0.0.1:0\"\n\
         [[backend]]\nname = \"typesafe\"\nbase_url = \"{}\"\napi_key_env = \"MOCK_KEY\"\n\
         models = [\"jev-*\"]\nbackoff_initial_ms = 20\nbackoff_max_ms = 100\n{}\n\
         [coalescing]\nwindow_ms = 0\n\
         [[service]]\nname = \"ocr\"\nkey_sha256 = [\"{}\"]\n{}\n",
        mock.url,
        settings.backend,
        hex::encode(hash_key(SERVICE_KEY)),
        settings.service,
    );
    if let Some(cluster) = settings.cluster {
        toml.push_str(&format!(
            "[cluster]\nredis_url_env = \"REDIS_URL\"\n{cluster}\n"
        ));
    }
    let config = Config::from_toml(&toml).expect("the test configuration is valid");
    let redis_url = settings.redis_url;
    let gateway = Gateway::start(&config, move |variable| match variable {
        "MOCK_KEY" => Some(UPSTREAM_KEY.to_owned()),
        "REDIS_URL" => redis_url.clone(),
        _ => None,
    })
    .await?;
    Ok(Replica {
        gateway,
        client: reqwest::Client::new(),
    })
}

impl Replica {
    /// A call about `state`, so that calls with different states never merge.
    async fn call(&self, state: &str) -> Reply {
        let body = json!({
            "state": state,
            "model": "jev-latest",
            "questions": {"q": {"type": "noul", "instructions": "Is this a refund request?"}},
        });
        let response = self
            .client
            .post(format!("http://{}/v1/systemone", self.gateway.public_addr))
            .bearer_auth(SERVICE_KEY)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        reply(response).await
    }

    async fn metrics(&self) -> String {
        self.client
            .get(format!("http://{}/metrics", self.gateway.admin_addr))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// The value of a counter without labels.
    async fn counter(&self, name: &str) -> u64 {
        let metrics = self.metrics().await;
        metrics
            .lines()
            .find_map(|line| line.strip_prefix(&format!("systemone_gateway_{name} ")))
            .map_or(0, |value| value.trim().parse().unwrap())
    }
}

/// A URL where nothing listens: the port was free a moment ago.
fn dead_redis_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("redis://127.0.0.1:{port}")
}

/// The test Redis and a key prefix that no other test run uses.
fn real_redis() -> Option<(String, String)> {
    let Ok(url) = std::env::var(TEST_REDIS_URL) else {
        eprintln!("skipped: {TEST_REDIS_URL} is not set");
        return None;
    };
    Some((url, format!("test-{:016x}", fastrand::u64(..))))
}

async fn mock() -> MockUpstream {
    MockUpstream::start("127.0.0.1:0".parse().unwrap(), UPSTREAM_KEY).await
}

/// How far apart the mock saw its first two calls arrive.
fn gap_between_first_two_calls(mock: &MockUpstream) -> Duration {
    let arrivals = mock.state.arrivals();
    assert_eq!(arrivals.len(), 2, "{arrivals:?}");
    arrivals[1].duration_since(arrivals[0])
}

#[tokio::test]
async fn a_configured_redis_url_must_be_in_the_environment() {
    let mock = mock().await;
    let result = start(
        &mock,
        Settings {
            cluster: Some(""),
            redis_url: None,
            ..Settings::default()
        },
    )
    .await;
    let err = result.err().expect("the gateway must not start");
    assert!(err.to_string().contains("REDIS_URL"), "{err:#}");
}

#[tokio::test]
async fn without_a_cluster_table_each_replica_paces_on_its_own() {
    let mock = mock().await;
    // One request a second each, sent at the same moment to two replicas.
    let backend = "requests_per_minute = 60\nburst = 1";
    let a = start(
        &mock,
        Settings {
            backend,
            ..Settings::default()
        },
    )
    .await
    .unwrap();
    let b = start(
        &mock,
        Settings {
            backend,
            ..Settings::default()
        },
    )
    .await
    .unwrap();
    let (first, second) = tokio::join!(a.call("document A"), b.call("document B"));
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(second.status, 200, "{}", second.text);
    // Nothing is shared: both replicas had a free slot.
    assert!(gap_between_first_two_calls(&mock) < Duration::from_millis(500));
    assert_eq!(a.counter("shared_limiter_errors_total").await, 0);
}

#[tokio::test]
async fn calls_succeed_and_are_paced_on_a_share_while_redis_is_down() {
    let mock = mock().await;
    // Two replicas would share 120 a minute: one every half second. On its
    // own, each replica paces at its half, one a second.
    let node = start(
        &mock,
        Settings {
            backend: "requests_per_minute = 120\nburst = 2",
            service: "requests_per_minute = 600",
            cluster: Some("expected_replicas = 2"),
            redis_url: Some(dead_redis_url()),
        },
    )
    .await
    .expect("the gateway starts without Redis");

    let (first, second) = tokio::join!(node.call("document A"), node.call("document B"));
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(second.status, 200, "{}", second.text);
    let gap = gap_between_first_two_calls(&mock);
    assert!(gap >= Duration::from_millis(800), "{gap:?}");
    assert!(gap < Duration::from_millis(1500), "{gap:?}");

    // Every call was answered, and the failures were counted, not thrown.
    assert!(node.counter("shared_limiter_errors_total").await >= 1);
}

#[tokio::test]
async fn replicas_sharing_a_redis_share_the_backend_limit() {
    let Some((url, prefix)) = real_redis() else {
        return;
    };
    let mock = mock().await;
    let cluster = format!("key_prefix = \"{prefix}\"");
    let settings = |url: &str| Settings {
        backend: "requests_per_minute = 60\nburst = 1",
        cluster: Some(&cluster),
        redis_url: Some(url.to_owned()),
        ..Settings::default()
    };
    let a = start(&mock, settings(&url)).await.unwrap();
    let b = start(&mock, settings(&url)).await.unwrap();

    let (first, second) = tokio::join!(a.call("document A"), b.call("document B"));
    assert_eq!(first.status, 200, "{}", first.text);
    assert_eq!(second.status, 200, "{}", second.text);
    // One request a second for both replicas together: the second call
    // waited for the slot the first one left.
    let gap = gap_between_first_two_calls(&mock);
    assert!(gap >= Duration::from_millis(800), "{gap:?}");
    assert_eq!(a.counter("shared_limiter_errors_total").await, 0);
    assert_eq!(b.counter("shared_limiter_errors_total").await, 0);
}

#[tokio::test]
async fn replicas_sharing_a_redis_share_a_services_rate() {
    let Some((url, prefix)) = real_redis() else {
        return;
    };
    let mock = mock().await;
    let cluster = format!("key_prefix = \"{prefix}\"");
    let settings = |url: &str| Settings {
        service: "requests_per_minute = 60\nburst = 1",
        cluster: Some(&cluster),
        redis_url: Some(url.to_owned()),
        ..Settings::default()
    };
    let a = start(&mock, settings(&url)).await.unwrap();
    let b = start(&mock, settings(&url)).await.unwrap();

    assert_eq!(a.call("document A").await.status, 200);
    // The service spent its one request on the other replica.
    let refused = b.call("document B").await;
    assert_eq!(refused.status, 429, "{}", refused.text);
    assert_eq!(refused.body["error"]["type"], "rate_limit_error");
    assert!(refused.header("retry-after-ms").parse::<u64>().unwrap() > 300);
    assert_eq!(mock.state.calls(), 1);
}
