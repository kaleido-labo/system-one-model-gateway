//! A gateway wired to a mock TypeSafe, for the integration tests.

// Each test binary compiles this module and uses a different part of it.
#![allow(dead_code)]

pub mod mock_upstream;

use std::borrow::Borrow;

use axum::http::HeaderMap;
use reqwest::StatusCode;
use serde_json::Value;
use systemone_gateway::{Config, Gateway, Reloaded, hash_key};

// Each test binary uses a different part of what this module re-exports.
#[allow(unused_imports)]
pub use mock_upstream::{MockUpstream, Scripted};

/// The key the mock expects from the gateway.
pub const UPSTREAM_KEY: &str = "upstream-secret";

/// Extra TOML lines for each configuration table, and the services.
pub struct Setup {
    pub server: &'static str,
    /// Extra lines for the System One backend, which serves `jev-*`.
    pub upstream: &'static str,
    /// Extra lines for a chat backend on the same mock, serving `Qwen/*`.
    /// `None` leaves the chat backend out.
    pub chat: Option<&'static str>,
    pub coalescing: &'static str,
    /// Lines of a `[cache]` table. `None` leaves the table out, so the cache
    /// is off.
    pub cache: Option<&'static str>,
    /// (name, key, extra lines for its [[service]] block)
    pub services: Vec<(&'static str, &'static str, &'static str)>,
    /// The key the gateway presents to the mock, on both backends.
    pub gateway_key: &'static str,
    /// The admin token, handed to the gateway through `server.admin_token_env`.
    /// `None` leaves the admin port open.
    pub admin_token: Option<&'static str>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            server: "",
            upstream: "",
            chat: None,
            // Wide enough that calls fired together always land in one batch.
            coalescing: "window_ms = 150",
            cache: None,
            services: vec![
                ("ocr", "key-ocr", ""),
                ("fraud", "key-fraud", ""),
                ("triage", "key-triage", ""),
            ],
            gateway_key: UPSTREAM_KEY,
            admin_token: None,
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

/// The configuration file for `setup`, with the backends on the mock at `url`.
pub fn config_text(setup: &Setup, url: &str) -> String {
    let admin_token_env = if setup.admin_token.is_some() {
        "admin_token_env = \"MOCK_ADMIN_TOKEN\"\n"
    } else {
        ""
    };
    let mut toml = format!(
        "[server]\nlisten = \"127.0.0.1:0\"\nadmin_listen = \"127.0.0.1:0\"\n{admin_token_env}{}\n\
         [[backend]]\nname = \"typesafe\"\nbase_url = \"{url}\"\napi_key_env = \"MOCK_KEY\"\n\
         models = [\"jev-*\"]\nbackoff_initial_ms = 20\nbackoff_max_ms = 100\n{}\n",
        setup.server, setup.upstream
    );
    if let Some(chat) = setup.chat {
        toml.push_str(&format!(
            "[[backend]]\nname = \"hf\"\nprotocol = \"chat\"\nbase_url = \"{url}/v1\"\n\
             api_key_env = \"MOCK_KEY\"\n{}backoff_initial_ms = 20\nbackoff_max_ms = 100\n{chat}\n",
            // A test may list its own models.
            if chat.contains("models =") {
                ""
            } else {
                "models = [\"Qwen/*\"]\n"
            },
        ));
    }
    toml.push_str(&format!("[coalescing]\n{}\n", setup.coalescing));
    if let Some(cache) = setup.cache {
        toml.push_str(&format!("[cache]\n{cache}\n"));
    }
    for (name, key, extra) in &setup.services {
        toml.push_str(&format!(
            "[[service]]\nname = \"{name}\"\nkey_sha256 = [\"{}\"]\n{extra}\n",
            hex::encode(hash_key(key))
        ));
    }
    toml
}

impl Harness {
    pub async fn start(setup: Setup) -> Self {
        let mock = MockUpstream::start("127.0.0.1:0".parse().unwrap(), UPSTREAM_KEY).await;
        let toml = config_text(&setup, &mock.url);
        let config = Config::from_toml(&toml).expect("the test configuration is valid");
        let key = setup.gateway_key;
        let admin_token = setup.admin_token;
        let gateway = Gateway::start(&config, move |variable| match variable {
            "MOCK_KEY" => Some(key.to_owned()),
            "MOCK_ADMIN_TOKEN" => admin_token.map(str::to_owned),
            _ => None,
        })
        .await
        .unwrap();
        Self {
            mock,
            gateway,
            client: reqwest::Client::new(),
        }
    }

    /// The file `setup` would be, for the same mock.
    pub fn config_text(&self, setup: &Setup) -> String {
        config_text(setup, &self.mock.url)
    }

    /// Reloads the running gateway with `setup`, as if the file had been
    /// edited into it.
    pub fn reload(&self, setup: &Setup) -> anyhow::Result<Reloaded> {
        let config = Config::from_toml(&self.config_text(setup))?;
        self.gateway.reload(&config)
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

    /// An admin request with the given `Authorization` header value, if any.
    pub async fn admin_with(
        &self,
        path: &str,
        authorization: Option<&str>,
    ) -> (StatusCode, String) {
        let mut request = self
            .client
            .get(format!("http://{}{path}", self.gateway.admin_addr));
        if let Some(value) = authorization {
            request = request.header("authorization", value);
        }
        let response = request.send().await.unwrap();
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
