//! Reloading the configuration without a restart: what a reload applies, what
//! it refuses, what it leaves running, and the file and signal paths that
//! trigger it.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use common::{Harness, Setup};
use serde_json::{Value, json};
use systemone_gateway::Config;
use tokio::time::sleep;

fn body() -> Value {
    json!({
        "state": {"document": "Order 1042 - refund request"},
        "model": "jev-latest",
        "questions": {"q": {"type": "noul", "instructions": "Is this a refund request?"}},
    })
}

fn services(
    list: &[(&'static str, &'static str)],
) -> Vec<(&'static str, &'static str, &'static str)> {
    list.iter().map(|(name, key)| (*name, *key, "")).collect()
}

fn setup(list: &[(&'static str, &'static str)]) -> Setup {
    Setup {
        services: services(list),
        ..Setup::default()
    }
}

/// The value of one Prometheus sample, by the text that starts its line.
fn sample(metrics: &str, name: &str) -> f64 {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .unwrap_or_else(|| panic!("no {name} in\n{metrics}"))
        .trim()
        .parse()
        .unwrap()
}

const OK: &str = r#"systemone_gateway_config_reloads_total{result="ok"} "#;
const ERROR: &str = r#"systemone_gateway_config_reloads_total{result="error"} "#;

/// A configuration file in a directory of its own, removed when dropped.
struct ConfigFile {
    dir: PathBuf,
}

impl ConfigFile {
    fn new() -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "systemone-gateway-reload-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    fn path(&self) -> PathBuf {
        self.dir.join("gateway.toml")
    }

    fn write(&self, text: &str) {
        // Like a ConfigMap update, the file is replaced, not edited in place.
        let staging = self.dir.join("staging");
        std::fs::write(&staging, text).unwrap();
        std::fs::rename(&staging, self.path()).unwrap();
    }
}

impl Drop for ConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn a_service_added_by_a_reload_can_call_without_a_restart() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    assert_eq!(h.call("key-new", body()).await.status, 401);

    let reloaded = h
        .reload(&setup(&[("ocr", "key-ocr"), ("new", "key-new")]))
        .unwrap();
    assert_eq!(reloaded.services, 2);
    assert!(reloaded.restart_required.is_empty());

    let reply = h.call("key-new", body()).await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    // The service that was there is untouched.
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
    let metrics = h.metrics().await;
    assert_eq!(sample(&metrics, OK), 1.0);
    assert_eq!(sample(&metrics, ERROR), 0.0);
    assert!(
        sample(
            &metrics,
            "systemone_gateway_config_last_reload_timestamp_seconds "
        ) > 1.6e9
    );
}

#[tokio::test]
async fn a_rotated_key_replaces_the_old_one() {
    let h = Harness::start(setup(&[("ocr", "key-old")])).await;
    assert_eq!(h.call("key-old", body()).await.status, 200);

    h.reload(&setup(&[("ocr", "key-new")])).unwrap();
    assert_eq!(h.call("key-old", body()).await.status, 401);
    assert_eq!(h.call("key-new", body()).await.status, 200);
}

#[tokio::test]
async fn a_service_that_was_removed_gets_a_401() {
    let h = Harness::start(setup(&[("ocr", "key-ocr"), ("fraud", "key-fraud")])).await;
    h.reload(&setup(&[("ocr", "key-ocr")])).unwrap();
    assert_eq!(h.call("key-fraud", body()).await.status, 401);
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
}

#[tokio::test]
async fn a_changed_quota_applies_to_the_next_call() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    assert_eq!(h.call("key-ocr", body()).await.status, 200);

    let mut limited = setup(&[]);
    limited.services = vec![("ocr", "key-ocr", "requests_per_minute = 6\nburst = 1")];
    h.reload(&limited).unwrap();
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
    let second = h.call("key-ocr", body()).await;
    assert_eq!(second.status, 429, "{}", second.text);
}

#[tokio::test]
async fn an_invalid_reload_keeps_the_running_configuration_and_is_counted() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    let file = ConfigFile::new();
    let reloader = h.gateway.reloader();

    // Not TOML, a rule broken, a service twice, a key that does not exist and
    // no file at all: each is refused and the old configuration answers.
    file.write("this is [not toml");
    assert!(reloader.reload_file(&file.path()).is_err());
    let good = h.config_text(&setup(&[("ocr", "key-ocr"), ("new", "key-new")]));
    file.write(&good.replace("window_ms = 150", "max_questions = 0"));
    let err = reloader.reload_file(&file.path()).unwrap_err();
    assert!(format!("{err:#}").contains("max_questions"), "{err:#}");
    file.write(&format!(
        "{good}\n[[service]]\nname = \"ocr\"\nkey_sha256 = [\"{}\"]\n",
        "1".repeat(64)
    ));
    assert!(reloader.reload_file(&file.path()).is_err());
    file.write(&good.replace("base_url", "base_uri"));
    assert!(reloader.reload_file(&file.path()).is_err());
    std::fs::remove_file(file.path()).unwrap();
    assert!(reloader.reload_file(&file.path()).is_err());

    assert_eq!(h.call("key-new", body()).await.status, 401);
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
    let metrics = h.metrics().await;
    assert_eq!(sample(&metrics, ERROR), 5.0);
    assert_eq!(sample(&metrics, OK), 0.0);

    // The same path then accepts a good file.
    file.write(&good);
    let reloaded = reloader.reload_file(&file.path()).unwrap();
    assert_eq!(reloaded.services, 2);
    assert_eq!(h.call("key-new", body()).await.status, 200);
    let metrics = h.metrics().await;
    assert_eq!(sample(&metrics, OK), 1.0);
    assert_eq!(sample(&metrics, ERROR), 5.0);
}

#[tokio::test]
async fn a_call_waiting_in_the_merge_window_is_answered_when_its_backend_is_replaced() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    // The window is 150 ms: the reload lands while the call is still in it.
    let (reply, reloaded) = tokio::join!(h.call("key-ocr", body()), async {
        sleep(Duration::from_millis(50)).await;
        h.reload(&Setup {
            upstream: "requests_per_minute = 600",
            ..setup(&[("ocr", "key-ocr")])
        })
        .unwrap()
    });
    assert_eq!(reloaded.backends_built, ["typesafe"]);
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(h.mock.state.calls(), 1);

    // New calls go through the new backend.
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
    assert_eq!(h.mock.state.calls(), 2);
}

#[tokio::test]
async fn a_call_in_flight_upstream_is_answered_when_its_backend_is_replaced() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    h.mock.state.set_delay(Duration::from_millis(600));
    let started = Instant::now();
    let (reply, reloaded) = tokio::join!(h.call("key-ocr", body()), async {
        // Past the window: the batch is upstream and waiting for the mock.
        sleep(Duration::from_millis(350)).await;
        let reloaded = h
            .reload(&Setup {
                upstream: "requests_per_minute = 600",
                ..setup(&[("ocr", "key-ocr")])
            })
            .unwrap();
        (reloaded, started.elapsed())
    });
    let (reloaded, at) = reloaded;
    assert_eq!(reloaded.backends_built, ["typesafe"]);
    assert!(at < Duration::from_millis(550), "the reload took {at:?}");
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.body["answers"]["q"]["echo"],
        "Is this a refund request?"
    );
}

#[tokio::test]
async fn an_unchanged_backend_keeps_its_queue_across_a_reload_of_the_services() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    // Two calls sharing a state merge into one upstream call, and a reload
    // that only touches the services lands between them.
    let (first, _, second) = tokio::join!(
        h.call("key-ocr", body()),
        async {
            sleep(Duration::from_millis(40)).await;
            let reloaded = h
                .reload(&setup(&[("ocr", "key-ocr"), ("new", "key-new")]))
                .unwrap();
            assert_eq!(reloaded.backends_kept, ["typesafe"]);
        },
        async {
            sleep(Duration::from_millis(80)).await;
            h.call("key-new", body()).await
        },
    );
    assert_eq!(first.status, 200);
    assert_eq!(second.status, 200);
    assert_eq!(first.header("x-systemone-gateway-batch-callers"), "2");
    assert_eq!(h.mock.state.calls(), 1);
}

#[tokio::test]
async fn a_removed_backend_takes_no_new_calls() {
    let h = Harness::start(Setup {
        chat: Some("models = [\"Qwen/Qwen2.5-7B-Instruct\"]"),
        ..setup(&[("ocr", "key-ocr")])
    })
    .await;
    let reloaded = h.reload(&setup(&[("ocr", "key-ocr")])).unwrap();
    assert_eq!(reloaded.backends_removed, ["hf"]);
    assert_eq!(reloaded.backends_kept, ["typesafe"]);

    let qwen = json!({
        "state": {"document": "x"},
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "questions": {"q": {"type": "noul", "instructions": "Is this a refund request?"}},
    });
    let reply = h.call("key-ocr", qwen).await;
    assert_eq!(reply.status, 422, "{}", reply.text);
    assert_eq!(h.call("key-ocr", body()).await.status, 200);
}

#[tokio::test]
async fn keys_that_need_a_restart_are_reported_and_the_rest_is_applied() {
    let h = Harness::start(setup(&[("ocr", "key-ocr")])).await;
    let reloaded = h
        .reload(&Setup {
            server: "max_body_bytes = 1000\nlog_format = \"json\"",
            ..setup(&[("ocr", "key-ocr"), ("new", "key-new")])
        })
        .unwrap();
    assert_eq!(
        reloaded.restart_required,
        ["server.max_body_bytes", "server.log_format"]
    );
    assert_eq!(h.call("key-new", body()).await.status, 200);
}

/// Polls `check` until it holds or three seconds pass.
async fn eventually<F: std::future::Future<Output = bool>>(
    what: &str,
    mut check: impl FnMut() -> F,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !check().await {
        assert!(Instant::now() < deadline, "gave up waiting for {what}");
        sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn the_watcher_reloads_a_file_whose_content_changed_and_only_then() {
    let h = &Harness::start(Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr")])
    })
    .await;
    let file = ConfigFile::new();
    file.write(&h.config_text(&Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr")])
    }));
    let (_, loaded) = Config::load_hashed(&file.path()).unwrap();
    let reloader = h.gateway.reloader();
    let watcher = tokio::spawn({
        let path = file.path();
        async move { reloader.watch_file(path, loaded).await }
    });

    // Same content, written again: no reload.
    file.write(&std::fs::read_to_string(file.path()).unwrap());
    sleep(Duration::from_millis(350)).await;
    assert_eq!(sample(&h.metrics().await, OK), 0.0);

    let changed = h.config_text(&Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr"), ("new", "key-new")])
    });
    file.write(&changed);
    eventually("the new service", || async move {
        h.call("key-new", body()).await.status == 200
    })
    .await;
    assert_eq!(sample(&h.metrics().await, OK), 1.0);

    // A broken file is counted once, however many times it is looked at.
    file.write("broken [");
    sleep(Duration::from_millis(450)).await;
    let metrics = h.metrics().await;
    assert_eq!(sample(&metrics, ERROR), 1.0);
    assert_eq!(sample(&metrics, OK), 1.0);
    assert_eq!(h.call("key-new", body()).await.status, 200);

    // Fixing it is picked up.
    file.write(&h.config_text(&Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr")])
    }));
    eventually("the service to go", || async move {
        h.call("key-new", body()).await.status == 401
    })
    .await;
    watcher.abort();
}

#[tokio::test]
async fn the_watcher_waits_while_the_interval_is_off_and_starts_when_a_reload_sets_it() {
    let h = &Harness::start(setup(&[("ocr", "key-ocr")])).await;
    let file = ConfigFile::new();
    file.write(&h.config_text(&setup(&[("ocr", "key-ocr")])));
    let (_, loaded) = Config::load_hashed(&file.path()).unwrap();
    let reloader = h.gateway.reloader();
    let watcher = tokio::spawn({
        let path = file.path();
        async move { reloader.watch_file(path, loaded).await }
    });

    // Interval off: a change is not noticed.
    file.write(&h.config_text(&setup(&[("ocr", "key-ocr"), ("new", "key-new")])));
    sleep(Duration::from_millis(300)).await;
    assert_eq!(h.call("key-new", body()).await.status, 401);

    // A reload (SIGHUP, here) that turns polling on.
    let polling = Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr"), ("new", "key-new")])
    };
    file.write(&h.config_text(&polling));
    h.gateway.reloader().reload_file(&file.path()).unwrap();
    assert_eq!(h.call("key-new", body()).await.status, 200);
    file.write(&h.config_text(&Setup {
        server: "config_reload_interval_ms = 100",
        ..setup(&[("ocr", "key-ocr"), ("later", "key-later")])
    }));
    eventually("the watcher to notice", || async move {
        h.call("key-later", body()).await.status == 200
    })
    .await;
    watcher.abort();
}

/// The real binary, reloaded by a real `SIGHUP`.
#[cfg(unix)]
mod signal {
    use super::*;
    use std::process::{Child, Command, Stdio};

    struct Gateway(Child);

    impl Drop for Gateway {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn config(public: u16, admin: u16, keys: &[&str]) -> String {
        let mut text = format!(
            "[server]\nlisten = \"127.0.0.1:{public}\"\nadmin_listen = \"127.0.0.1:{admin}\"\n\
             [[backend]]\nname = \"typesafe\"\nbase_url = \"http://127.0.0.1:1\"\n"
        );
        for (index, key) in keys.iter().enumerate() {
            text.push_str(&format!(
                "[[service]]\nname = \"s{index}\"\nkey_sha256 = [\"{}\"]\n",
                hex::encode(systemone_gateway::hash_key(key))
            ));
        }
        text
    }

    async fn status(client: &reqwest::Client, public: u16, key: &str) -> u16 {
        client
            .post(format!("http://127.0.0.1:{public}/v1/systemone"))
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16()
    }

    fn hangup(child: &Child) {
        let status = Command::new("kill")
            .args(["-HUP", &child.id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[tokio::test]
    async fn sighup_reloads_the_file_the_gateway_was_started_with() {
        let (public, admin) = (free_port(), free_port());
        let file = ConfigFile::new();
        file.write(&config(public, admin, &["key-a"]));
        let child = Gateway(
            Command::new(env!("CARGO_BIN_EXE_systemone-gateway"))
                .args(["serve", "--config"])
                .arg(file.path())
                .env("TYPESAFE_API_KEY", "unused")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let client = &reqwest::Client::new();
        eventually("the gateway to be ready", || async move {
            client
                .get(format!("http://127.0.0.1:{admin}/readyz"))
                .send()
                .await
                .is_ok_and(|reply| reply.status() == 200)
        })
        .await;
        // Authenticated, but not a valid request: a 422, not a 401.
        assert_eq!(status(client, public, "key-a").await, 422);
        assert_eq!(status(client, public, "key-b").await, 401);

        file.write(&config(public, admin, &["key-b"]));
        hangup(&child.0);
        eventually("the rotated key", || async move {
            status(client, public, "key-b").await != 401
        })
        .await;
        assert_eq!(status(client, public, "key-a").await, 401);

        // A broken file: the gateway survives, keeps the keys and counts it.
        file.write("[[service]");
        hangup(&child.0);
        let metrics_url = &format!("http://127.0.0.1:{admin}/metrics");
        eventually("the error to be counted", || async move {
            let metrics = client
                .get(metrics_url)
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            metrics.contains(&format!("{ERROR}1"))
        })
        .await;
        assert_eq!(status(client, public, "key-b").await, 422);
        assert_eq!(status(client, public, "key-a").await, 401);
        let metrics = client
            .get(metrics_url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains(&format!("{OK}1")), "{metrics}");
    }
}
