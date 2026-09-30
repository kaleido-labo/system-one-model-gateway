# systemone-gateway

A gateway in front of [TypeSafe's System One API](https://docs.typesafe.ai/api.md).
Your services keep calling `POST /v1/systemone` as they would call TypeSafe.
The gateway picks a backend from the requested model, merges calls that share
a state, paces the upstream traffic, and gives each service its own answers and
its own share of the token usage.

```
 ocr-service ──┐                                     ┌─ jev-*  ──> TypeSafe, POST /v1/systemone
 fraud-check ──┼──> auth ─> validate ─> route model ─┤
 classifier  ──┘            batch by state, pace     └─ Qwen/* ──> Hugging Face, POST /v1/chat/completions
      ^                                                                                   │
      └──────────────── answers in TypeSafe's format, usage split per caller <────────────┘
```

## Why

Without a gateway, every service holds its own TypeSafe key, retries on its own,
and runs into the account's shared request limit without knowing what the others
are doing. Behind the gateway, the services share one key and one queue per
backend. When TypeSafe answers 429, the gateway pauses all traffic to it at once,
and a service that calls too often gets a 429 from the gateway before it takes
capacity from the others.

When three services ask about the same document, the document is billed three
times. TypeSafe charges per input token, and the state is often most of them. The
gateway sends calls about the same state upstream once.

The metrics show what each service spends, and the shares of a merged call add
up to exactly what the backend billed.

The provider becomes a configuration choice. A service that wants a Hugging Face
model instead of Jev changes the model name in its request and nothing else. The
answer comes back in the same format.

## Quick start

You need a Rust toolchain (1.96 or later) or Docker.

```sh
cargo build --release
cp config.example.toml gateway.toml
target/release/systemone-gateway gen-key --service ocr-service
```

`gen-key` prints a key for the service and a `[[service]]` block with its hash.
Paste the hash into `gateway.toml` in place of the placeholder, then start the
gateway:

```sh
export TYPESAFE_API_KEY=...   # your TypeSafe key
export HF_TOKEN=...           # needed to start, because the example config has a Hugging Face backend
target/release/systemone-gateway check-config --config gateway.toml
target/release/systemone-gateway serve --config gateway.toml
```

Call it with the key `gen-key` printed:

```sh
export SERVICE_KEY=s1gw_...
curl -s http://localhost:8080/v1/systemone \
  -H "Authorization: Bearer $SERVICE_KEY" \
  -H 'content-type: application/json' \
  -d '{"state": "Order 1042 - refund request, 18.00 EUR", "model": "jev-latest",
       "questions": {"refund": {"type": "noul", "instructions": "Is this a refund request?"}}}'
```

To point an existing service at the gateway, set `TYPESAFE_BASE_URL` to the
gateway's address and `TYPESAFE_API_KEY` to the key it issued to that service.
No TypeSafe account? [Getting started](docs/getting-started.md) runs the same
steps against a bundled mock.

## Documentation

| Page | What is in it |
| --- | --- |
| [Getting started](docs/getting-started.md) | Build, configure, run, and try it locally with the mock. |
| [Architecture](docs/architecture.md) | How a call moves through authentication, validation, routing, batching, pacing, retries and answer splitting. |
| [Configuration](docs/configuration.md) | Every configuration key, with type, default and meaning. See also [`config.example.toml`](config.example.toml). |
| [API](docs/api.md) | Endpoints, headers, error shape and status codes. |
| [Backends](docs/backends.md) | Model routing, System One backends, and chat models answered through logprobs, with their limits. |
| [Operations](docs/operations.md) | Docker, Kubernetes, logs, metrics, tuning, queue deadlines and known limits. |

## Development

```sh
cargo test                   # unit tests, and end-to-end tests against the mock
cargo clippy --all-targets
```

| Path under `src/` | Role |
| --- | --- |
| `http/` | Routers, the `POST /v1/systemone` handler and its use of the answer cache, `GET /v1/models`, admin endpoints and the check of the admin token |
| `wire/` | The System One wire format kept as raw JSON: parsing, merge keys, documented request rules, token estimates |
| `backend/` | Backends, model routing and fallback chains; the circuit breaker (`breaker`); the HTTP client of one backend with retries (`upstream/`); the `chat` protocol with a prompt per question, answers read from logprobs and errors in TypeSafe's shape (`chat/`) |
| `scheduling/` | Opens, joins and seals batches (`coalescer`); builds the merged request and splits the answer and token usage (`batch/`); sends a sealed batch and replays after a rejection (`dispatch`); GCRA pacing, in process or shared through Redis (`limiter/`) |
| `config/` | Configuration root and validation, backend settings, model name patterns, `[cluster]` and `[tracing]` |
| `cache.rs` | The optional answer cache |
| `telemetry/` | OpenTelemetry setup, trace context propagation and spans |
| `services.rs`, `metrics.rs`, `error.rs`, `app.rs` | Keys and quotas, Prometheus, error shape, wiring and shutdown |

[CONTRIBUTING.md](CONTRIBUTING.md) covers the build, the lint commands and the pull request conventions.

## License

MIT. See [LICENSE](LICENSE).
