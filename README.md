# systemone-gateway

A single gateway in front of TypeSafe's System One API (the Jev model). Your
services keep calling `POST /v1/systemone` exactly as they would call
TypeSafe. The gateway takes in those calls, sends the ones that share a state
as one upstream request, keeps the combined traffic inside the account's
limits, and gives each service back its own answers.

```
 ocr-service ──┐                                              ┌──> TypeSafe
 fraud-check ──┼──> auth ─> validate ─> batch by state ─> pace ┤    POST /v1/systemone
 expense-cat ──┘                                              │
      ^                                                       │
      └──────────── answers and usage, split per caller <─────┘
```

## Why

When every service holds its own TypeSafe key, each one retries on its own
and runs into the account's shared 1,200 requests/min without knowing what the
others are doing. And when three services ask about the same receipt, the
receipt is billed three times.

Behind the gateway, the services share one key and one queue, and the metrics
show what each of them spends. Calls about the same state go upstream once.
Jev bills input tokens only, and the state is usually the bulk of them.

## How it works

1. The gateway identifies the calling service by its bearer key; the config
   stores only the key's SHA-256. The service's own quota applies (requests
   per minute, calls in flight, allowed models). Then the request is checked
   against the rules in the [API reference](https://docs.typesafe.ai/api.md):
   question types, required fields, at most 255 options for a Choice, 2 to 10
   levels for a Score. A malformed question fails on its own, before it can
   join a merged call.
2. Calls with the same model, the same state and the same other top-level
   fields go into one batch. The batch stays open for `window_ms` (10 ms by
   default) and for as long as it waits for an upstream slot, so merging does
   the most when traffic is heaviest. In the merged request, question ids
   become `q0`, `q1` and so on, and a question asked by two services is sent
   once. The same state sent with different whitespace still merges. The same
   keys in a different order do not, because the model reads the state as
   text.

   Merging is safe because TypeSafe evaluates every question of a request
   independently against the same state and never shows question ids to the
   model ([primitives](https://docs.typesafe.ai/primitives.md)).
3. The gateway paces upstream calls against requests per minute and tokens
   per second. It retries failed attempts with exponential backoff on 429,
   529, 5xx and network errors, and follows `retry-after-ms` or `retry-after`
   when TypeSafe sends one. A 429 pauses all batches at once instead of
   letting each one find the limit by itself. If TypeSafe rejects a merged
   request (400 or 422), the gateway replays each caller alone, so the error
   reaches only the caller whose question caused it.
4. Each service gets TypeSafe's answers under its own question ids. The merged
   call's `usage` is split between callers: the state evenly, each question to
   whoever asked it. The shares add up to exactly what TypeSafe billed.

When waiting for a slot would take longer than `max_queue_wait_ms`, the
gateway answers 429 with `retry-after` right away. The TypeSafe SDKs retry
429s and honour that header, so the service backs off without any code of
its own.

## Pointing a service at the gateway

The Python and JavaScript SDKs read their base URL and key from the
environment. Setting two variables is enough, and the service's code stays as
it is:

```sh
TYPESAFE_BASE_URL=http://systemone-gateway:8080
TYPESAFE_API_KEY=s1gw_...   # the key the gateway issued to this service
```

Over plain HTTP, the request is the one TypeSafe documents:

```sh
curl -s http://systemone-gateway:8080/v1/systemone \
  -H "Authorization: Bearer $TYPESAFE_API_KEY" \
  -H 'content-type: application/json' \
  -d '{"state": "Parking Saemes - 18,00 EUR", "model": "jev-latest",
       "questions": {"parking": {"type": "noul", "instructions": "Is this a parking receipt?"}}}'
```

## Try it locally, without a TypeSafe key

The repository ships the mock TypeSafe the tests use. Its answers carry an
extra `echo` field with the question's instructions, so you can see which
answer went where. The real API has no such field.

```sh
cargo run --example mock_upstream               # fake TypeSafe on 127.0.0.1:9999
cargo run -- gen-key --service ocr-service      # prints a key and its hash
```

Put the hash in `gateway.toml`:

```toml
[upstream]
base_url = "http://127.0.0.1:9999"

[[service]]
name = "ocr-service"
key_sha256 = ["<hash printed by gen-key>"]
```

```sh
cargo run -- check-config --config gateway.toml
TYPESAFE_API_KEY=mock-typesafe-key cargo run -- serve --config gateway.toml
```

Two services asking about the same receipt at the same moment share one
upstream call. From a local run:

```
ocr-service   HTTP 200  x-typesafe-request-id: req_1  x-systemone-gateway-batch-callers: 2
{"answers":{"is_toll":{...}},"model":"jev-1.13.0","usage":{"input_tokens":30,"output_tokens":10}}

fraud-check   HTTP 200  x-typesafe-request-id: req_1  x-systemone-gateway-batch-callers: 2
{"answers":{"altered":{...}},"model":"jev-1.13.0","usage":{"input_tokens":48,"output_tokens":10}}
```

## Configuration

[`config.example.toml`](config.example.toml) lists every setting with its
default. The file holds no secret, so it can live in git or in a ConfigMap.
The TypeSafe key comes from the variable named by `upstream.api_key_env`
(`TYPESAFE_API_KEY` by default).

| Setting | Default | What it does |
| --- | --- | --- |
| `server.listen` / `server.admin_listen` | `0.0.0.0:8080` / `0.0.0.0:9090` | API port; health and metrics port |
| `server.request_timeout_ms` | `9000` | Longest a call may take, queueing included. Kept under the SDKs' 10 s timeout so the caller gets the gateway's 504 instead of a blind client retry |
| `upstream.requests_per_minute`, `burst`, `tokens_per_second` | `1200`, one second's worth, `250000` | The account's limits, shared by all services |
| `upstream.max_concurrency` | `64` | Upstream calls in flight |
| `upstream.max_retries`, `backoff_*_ms`, `attempt_timeout_ms` | `3`, `200` to `3000`, `5000` | Retry policy |
| `upstream.max_queue_wait_ms` | `2000` | Past this wait, answer 429 with a retry-after |
| `coalescing.window_ms` | `10` | How long a batch waits for company; `0` turns merging off |
| `coalescing.max_questions`, `max_request_tokens`, `max_state_plus_question_tokens` | `128`, `56000`, `28000` | What one merged call may carry; kept under the vendor's 64k and 32k |
| `[[service]] key_sha256` | required | One or more key hashes; two let you rotate a key without downtime |
| `[[service]] requests_per_minute`, `burst`, `max_concurrent` | unset | The service's own share |
| `[[service]] allowed_models` | any | Models the service may ask for |

`systemone-gateway gen-key` creates a key and prints its hash;
`systemone-gateway hash-key` hashes a key read from standard input.

## API

| Endpoint | Port | |
| --- | --- | --- |
| `POST /v1/systemone` | public | Same request and response as TypeSafe |
| `GET /v1/models` | public | TypeSafe's model list, cached for `models_cache_ttl_ms` |
| `GET /healthz`, `GET /readyz` | admin | Liveness; readiness, which turns 503 on shutdown |
| `GET /metrics` | admin | Prometheus / OpenMetrics |

Response headers: `x-systemone-gateway-batch-callers` (how many calls shared
the upstream request), `x-typesafe-request-id` (TypeSafe's id, the same for
every caller of a merged request) and `x-request-id`.

Errors the gateway produces itself have the shape
`{"error": {"type", "message", "param"}}`:

| Status | Type | When |
| --- | --- | --- |
| 400 | `invalid_request_error` | The body is not a JSON object |
| 401 | `authentication_error` | Missing or unknown service key |
| 403 | `permission_error` | Model not in the service's `allowed_models` |
| 422 | `validation_error` | A documented rule is broken; `param` names the field |
| 429 | `rate_limit_error` | Service over its own quota, shared quota booked past `max_queue_wait_ms`, or TypeSafe asked to back off. Always carries `retry-after` and `retry-after-ms` |
| 502 | `upstream_error` | TypeSafe unreachable, refusing the gateway's key, or answering something unreadable |
| 504 | `timeout_error` | No answer within `request_timeout_ms` |

Any other error status comes from TypeSafe and is passed through with its
body, after retries for 429, 529 and 5xx. A 401 from TypeSafe means the
gateway's own key is wrong, so callers get a 502 instead.

## Metrics

All prefixed `systemone_gateway_`:

- `calls_total{service,status}`, `call_duration_seconds{service}`, `questions_total{service}`
- `input_tokens_total{service}`: tokens charged to each service, merged calls split
- `upstream_calls_total{status}`, `upstream_duration_seconds`, `upstream_retries_total`
- `batch_callers`, `batch_questions`, `queue_wait_seconds`: how much merging happens and what it costs in latency
- `deduplicated_questions_total`, `estimated_tokens_saved_total`, `isolated_replays_total`

Logs never contain states or questions, only the service, the request id,
the status and timings. Set `server.log_format = "json"` in production and
use `RUST_LOG` to change the level.

## Docker and Kubernetes

```sh
docker build -t systemone-gateway .
docker run -p 8080:8080 -p 9090:9090 \
  -v "$PWD/gateway.toml:/etc/systemone-gateway/gateway.toml:ro" \
  -e TYPESAFE_API_KEY \
  systemone-gateway
```

The image is distroless and runs as non-root (about 64 MB). In Kubernetes,
point the readiness probe at `/readyz` and the liveness probe at `/healthz`,
both on port 9090. On SIGTERM the gateway stops taking connections and lets
in-flight calls finish, so give `terminationGracePeriodSeconds` a few seconds
more than `request_timeout_ms`.

## Limits

- Pacing, quotas and merging live in memory, so they apply per process. With
  N replicas, give each one 1/N of the account's limits, and expect merging
  only between calls that reach the same replica.
- Token counts are estimates. Jev's tokenizer is not published, so the
  gateway counts 3 bytes per token (`coalescing.bytes_per_token`). The
  estimate sizes merged calls, books the tokens-per-second budget (corrected
  with the real `usage` after each call) and weights the usage split, where
  only ratios matter.
- TypeSafe's limits change without notice
  ([models](https://docs.typesafe.ai/models.md)). The gateway backs off on
  every 429 but does not learn a lower rate. If
  `upstream_calls_total{status="429"}` keeps growing, lower
  `requests_per_minute`.
- There is no answer cache. Two identical calls a minute apart cost two
  upstream requests; only concurrent calls share one.

## Development

```sh
cargo test                   # unit tests, and end-to-end tests against the mock
cargo clippy --all-targets
```

| Module | Role |
| --- | --- |
| `api` | HTTP handlers and routers |
| `coalescer` | Opens, joins and seals batches |
| `dispatch` | Sends a sealed batch, answers each caller, replays after a rejection |
| `batch` | Builds the merged request and splits the answer |
| `upstream` | HTTP client to TypeSafe, retries, `retry-after` parsing |
| `limiter` | GCRA pacing for requests, tokens and per-service quotas |
| `protocol`, `json`, `validate` | Wire format kept as raw JSON, merge keys, documented rules |
| `usage` | Largest-remainder split of token usage |
| `services`, `config`, `metrics`, `error`, `app` | Keys and quotas, configuration, Prometheus, error shape, wiring |
