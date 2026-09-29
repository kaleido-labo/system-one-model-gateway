//! Runs the mock TypeSafe the integration tests use, so the gateway can be
//! tried locally without a TypeSafe key:
//!
//! ```sh
//! cargo run --example mock_upstream               # 127.0.0.1:9999
//! cargo run --example mock_upstream -- 0.0.0.0:9999
//! ```
//!
//! It expects `Authorization: Bearer mock-typesafe-key` (override with
//! `MOCK_TYPESAFE_KEY`), and every answer carries an `echo` of the question's
//! instructions so you can see which answer went where. It also serves
//! `POST /v1/chat/completions` with logprobs, to stand in for a chat
//! backend: point one at `http://127.0.0.1:9999/v1`.

#[allow(dead_code)]
#[path = "../tests/common/mock_upstream.rs"]
mod mock_upstream;

#[tokio::main]
async fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9999".to_owned());
    let key = std::env::var("MOCK_TYPESAFE_KEY").unwrap_or_else(|_| "mock-typesafe-key".to_owned());
    let mock = mock_upstream::MockUpstream::start(
        addr.parse().expect("an address such as 127.0.0.1:9999"),
        &key,
    )
    .await;
    println!(
        "mock TypeSafe on {url}, mock chat API on {url}/v1 (expects `Authorization: Bearer {key}`)",
        url = mock.url
    );
    let _ = tokio::signal::ctrl_c().await;
}
