//! A gateway wired to a mock TypeSafe, for the integration tests.

pub mod mock_upstream;

use std::borrow::Borrow;

use axum::http::HeaderMap;
use reqwest::StatusCode;
use serde_json::Value;
use systemone_gateway::{ApiKey, Config, Gateway, hash_key};

pub use mock_upstream::{MockUpstream, Scripted};

/// The key the mock expects from the gateway.
pub const UPSTREAM_KEY: &str = "upstream-secret";

/// Extra TOML lines for each configuration table, and the services.
pub struct Setup {
    pub server: &'static str,
    pub upstream: &'static str,
    pub coalescing: &'static str,
    /// (name, key, extra lines for its [[service]] block)
    pub services: Vec<(&'static str, &'static str, &'static str)>,
    /// The TypeSafe key the gateway presents to the mock.
    pub gateway_key: &'static str,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            server: "",
            upstream: "",
            // Wide enough that calls fired together always land in one batch.
            coalescing: "window_ms = 150",
            services: vec![
                ("ocr", "key-ocr", ""),
                ("fraud", "key-fraud", ""),
                ("expense", "key-expense", ""),
            ],
            gateway_key: UPSTREAM_KEY,
        }
    }
}

pub struct Harness {
    pub mock: MockUpstream,
    pub gateway: Gateway,
    pub client: reqwest::Client,
}

pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub text: String,
    /// The body parsed as JSON, or `Null` if it is not JSON.
    pub body: Value,
}

impl Reply {
    pub fn header(&self, name: &str) -> &str {
        self.headers
            .get(name)
            .unwrap_or_else(|| panic!("no {name} header in {:?}", self.headers))
            .to_str()
            .unwrap()
    }
}

impl Harness {
    pub async fn start(setup: Setup) -> Self {
        let mock = MockUpstream::start("127.0.0.1:0".parse().unwrap(), UPSTREAM_KEY).await;
        let mut toml = format!(
            "[server]\nlisten = \"127.0.0.1:0\"\nadmin_listen = \"127.0.0.1:0\"\n{}\n\
             [upstream]\nbase_url = \"{}\"\nbackoff_initial_ms = 20\nbackoff_max_ms = 100\n{}\n\
             [coalescing]\n{}\n",
            setup.server, mock.url, setup.upstream, setup.coalescing
        );
        for (name, key, extra) in &setup.services {
            toml.push_str(&format!(
                "[[service]]\nname = \"{name}\"\nkey_sha256 = [\"{}\"]\n{extra}\n",
                hex::encode(hash_key(key))
            ));
        }
        let config = Config::from_toml(&toml).expect("the test configuration is valid");
        let gateway = Gateway::start(&config, ApiKey::new(setup.gateway_key))
            .await
            .unwrap();
        Self {
            mock,
            gateway,
            client: reqwest::Client::new(),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.gateway.public_addr)
    }

    /// Takes the body by value or by reference; by value lets a call built
    /// inline live inside `tokio::join!`.
    pub async fn call(&self, key: &str, body: impl Borrow<Value>) -> Reply {
        self.call_raw(key, &body.borrow().to_string()).await
    }

    pub async fn call_raw(&self, key: &str, body: &str) -> Reply {
        let response = self
            .client
            .post(self.url("/v1/systemone"))
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .unwrap();
        reply(response).await
    }

    pub async fn get(&self, key: &str, path: &str) -> Reply {
        let response = self
            .client
            .get(self.url(path))
            .bearer_auth(key)
            .send()
            .await
            .unwrap();
        reply(response).await
    }

    pub async fn admin(&self, path: &str) -> (StatusCode, String) {
        let response = self
            .client
            .get(format!("http://{}{path}", self.gateway.admin_addr))
            .send()
            .await
            .unwrap();
        (response.status(), response.text().await.unwrap())
    }

    pub async fn metrics(&self) -> String {
        self.admin("/metrics").await.1
    }
}

pub async fn reply(response: reqwest::Response) -> Reply {
    let status = response.status();
    let headers = response.headers().clone();
    let text = response.text().await.unwrap();
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Reply {
        status,
        headers,
        text,
        body,
    }
}
