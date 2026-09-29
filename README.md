# systemone-gateway

A single gateway that speaks TypeSafe's System One API. Your services keep
calling `POST /v1/systemone` exactly as they would call TypeSafe. The gateway
takes in those calls and picks a backend from the requested model: TypeSafe's
Jev, another server with the same API, or a chat model such as one hosted on
Hugging Face. It sends calls that share a state together, keeps the combined
traffic inside each backend's limits, and gives each service back its own
answers.

```
 ocr-service ──┐                                     ┌─ jev-*  ──> TypeSafe, POST /v1/systemone
 fraud-check ──┼──> auth ─> validate ─> route model ─┤
 expense-cat ──┘            batch by state, pace     └─ Qwen/* ──> Hugging Face, POST /v1/chat/completions
      ^                                                                                   │
      └──────────────── answers in TypeSafe's format, usage split per caller <────────────┘
```

## Why

When every service holds its own TypeSafe key, each one retries on its own
and runs into the account's shared 1,200 requests/min without knowing what the
others are doing. And when three services ask about the same receipt, the
receipt is billed three times.

Behind the gateway, the services share one key and one queue per backend,
and the metrics show what each of them spends. Calls about the same state go
upstream once. Jev bills input tokens only, and the state is usually the bulk
of them.

The gateway also makes the provider a configuration choice. A service that
wants a Hugging Face model instead of Jev changes the model name in its
request and nothing else, and the answer comes back in the same format.

## How it works

0. The requested `model` picks the backend (see [Backends](#backends)).
   Everything below happens per backend: each one has its own key, limits
   and queue.
1. The gateway identifies the calling service by its bearer key; the config
   stores only the key's SHA-256. The service's own quota applies (requests
   per minute, calls in flight, allowed models). Then the request is checked
   against the rules in the [API reference](https://docs.typesafe.ai/api.md):
   question types, required fields, at most 255 options for a Choice, 2 to 10
   levels for a Score. A malformed question fails on its own, before it can
   join a merged call.
2. Calls with the same model, the same state and the same other top-level
   fields go into one batch. The batch stays open for `window_ms` (10 ms by
   default) and for as long as it waits for upstream capacity, so merging
   does the most when traffic is heaviest. In the merged request, question ids
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
   when the backend sends one. A 429 pauses all batches at once instead of
   letting each one find the limit by itself. When the pause ends, the
   batches leave one slot apart. If the backend rejects a merged request (400
   or 422), the gateway replays each caller alone, so the error reaches only
   the caller whose question caused it.
4. Each service gets its answers under its own question ids. The merged
   call's `usage` is split between callers: the state evenly, each question to
   whoever asked it. The shares add up to exactly what the backend billed.

A call waits at most `max_queue_wait_ms` for upstream capacity (a request
slot, a free connection, the token budget), on top of the merge window. When
the gateway can tell up front that the wait will be longer, it answers 429
with `retry-after` straight away. When it only finds out while the call
waits, the 429 goes out as soon as the limit passes. In both cases the call
is refused rather than sent late. The TypeSafe SDKs retry 429s and honour
that header, so the service backs off without any code of its own.

## Backends

Each `[[backend]]` block in the configuration is one model provider. Its
`models` list says which model names it serves: an exact name, a prefix
ending in `*`, or `*` alone for everything. A call goes to the backend with
the most specific match, an exact name before the longest prefix, whatever
the order of the blocks. A model no backend serves gets a 422. A file with
no `[[backend]]` at all sends every model to TypeSafe, with the key in
`TYPESAFE_API_KEY`.

```toml
[[backend]]
name = "typesafe"
api_key_env = "TYPESAFE_API_KEY"
models = ["jev-*"]

[[backend]]
name = "huggingface"
protocol = "chat"
base_url = "https://router.huggingface.co/v1"
api_key_env = "HF_TOKEN"
models = ["Qwen/*", "meta-llama/*"]
requests_per_minute = 300
```

With this file, a service moves a question from Jev to Qwen by changing
`"model": "jev-latest"` to `"model": "Qwen/Qwen2.5-7B-Instruct"`. The
answer keeps TypeSafe's format either way, so the service's code stays the
same. The `x-systemone-gateway-backend` response header says which backend
answered.

### `protocol = "systemone"`

This is the default. The gateway forwards calls to `POST /v1/systemone`,
merged calls included. It fits TypeSafe and any server with the same API.
Some open models published on Hugging Face ship one: Jev-Vision's `serve.py`
listens on `/v1/systemone`, for instance. For those, point `base_url` at the
server and leave out `api_key_env` if it takes no key.

### `protocol = "chat"`

This one is for a generative model behind an OpenAI-compatible chat
completions API: the [Hugging Face
router](https://huggingface.co/docs/inference-providers/tasks/chat-completion),
a Hugging Face Inference Endpoint, TGI or vLLM. `base_url` is the URL that
`chat/completions` hangs off, such as `https://router.huggingface.co/v1`.

Each question becomes one chat request that asks for a single token, the
label of an answer, with its `logprobs`:

| Question | Labels the model picks from | Answer built from the label probabilities |
| --- | --- | --- |
| `noul` | `Yes`, `No` | `noul` = P(Yes) |
| `choice` | `A`, `B`, `C`... one letter per option | `choice`, `probabilities` per option, `confidence` |
| `score` | `0`, `1`... one digit per level | `score` = sum of level × probability, `legend`, `probabilities`, `confidence` |

The gateway adds up the probabilities of tokens that spell the same label
(`Yes`, ` Yes`, `yes`), drops the tokens that are no label, and renormalises
what is left. `confidence` follows the [formula TypeSafe
documents](https://docs.typesafe.ai/confidence.md), `(n × max − 1) / (n − 1)`.
The answers come from the model's token probabilities, never from a
confidence the model writes out, because verbalised confidence is badly
calibrated. `usage` adds up the `prompt_tokens` and `completion_tokens` of
the chat calls.

A chat backend behaves differently from Jev in these ways:

- The Hugging Face router returns at most 5 candidate tokens
  (`top_logprobs`), so a Choice spreads its probability over 5 options at
  most and every other option gets 0. vLLM and TGI accept up to 20; raise
  `top_logprobs` there.
- A Choice has at most 26 options, one letter each. More gets a 422 before
  anything is sent.
- The probabilities are the model's raw ones. Nobody calibrated them the way
  TypeSafe calibrates Jev, so measure them on your own data before you pick
  thresholds.
- Every question is one request that repeats the state, and each one books
  its own slot in `requests_per_minute`. Merging still sends a question two
  services share only once, but it saves no state tokens. A server with
  prefix caching (vLLM, TGI) reads the shared state once anyway, since the
  state comes first in every prompt.
- Use an instruct model that answers straight away. A reasoning model that
  starts with a thinking token has no label in its first token, and the call
  fails with a 502. For vLLM, `request_extras` can turn thinking off with
  `chat_template_kwargs = { enable_thinking = false }`.
- Top-level request fields other than `state`, `model` and `questions` are
  not sent, because a chat API has nothing to put them in.

`upstream_model` sends one fixed model id upstream whatever name the service
used: services ask for `my-judge`, say, and the gateway calls
`org/fine-tuned-model`. `request_extras` adds fields to every chat request.
The gateway's own fields (`model`, `messages`, `max_tokens`, `logprobs`,
`top_logprobs`, `stream`) take precedence over them.

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
answer went where. The real API has no such field. The mock also serves a
chat API with logprobs under `/v1`, which stands in for a Hugging Face model.

```sh
cargo run --example mock_upstream               # fake TypeSafe on 127.0.0.1:9999
cargo run -- gen-key --service ocr-service      # prints a key and its hash
```

Put the hash in `gateway.toml`:

```toml
[[backend]]
name = "typesafe"
base_url = "http://127.0.0.1:9999"
api_key_env = "TYPESAFE_API_KEY"
models = ["jev-*"]

[[backend]]
name = "huggingface"
protocol = "chat"
base_url = "http://127.0.0.1:9999/v1"
api_key_env = "TYPESAFE_API_KEY"
models = ["Qwen/*"]

[[service]]
name = "ocr-service"
key_sha256 = ["<hash printed by gen-key>"]
```

```sh
cargo run -- check-config --config gateway.toml
TYPESAFE_API_KEY=mock-typesafe-key cargo run -- serve --config gateway.toml
```

A call with `"model": "Qwen/Qwen2.5-7B-Instruct"` then goes to the mock chat
API and comes back in TypeSafe's format. From a local run:

```
HTTP/1.1 200 OK
x-typesafe-request-id: chat_2
x-systemone-gateway-backend: huggingface
{"model":"Qwen/Qwen2.5-7B-Instruct","answers":{"toll":{"type":"noul","noul":0.9365558912386708},"category":{"type":"choice","choice":"tolls","probabilities":{"tolls":0.6666666666666666,"fuel":0.33333333333333337,"meals":0.0},"confidence":0.5}},"usage":{"input_tokens":247,"output_tokens":2}}
```

Two services asking about the same receipt at the same moment share one
upstream call. From a local run against the System One backend:

```
ocr-service   HTTP 200  x-typesafe-request-id: req_1  x-systemone-gateway-batch-callers: 2
{"answers":{"is_toll":{...}},"model":"jev-1.13.0","usage":{"input_tokens":30,"output_tokens":10}}

fraud-check   HTTP 200  x-typesafe-request-id: req_1  x-systemone-gateway-batch-callers: 2
{"answers":{"altered":{...}},"model":"jev-1.13.0","usage":{"input_tokens":48,"output_tokens":10}}
```

## Configuration

[`config.example.toml`](config.example.toml) lists every setting with its
default. The file holds no secret, so it can live in git or in a ConfigMap.
Each backend's key comes from the variable named by its `api_key_env`.
`check-config` prints which backend serves which models, and the gateway
refuses to start while a named variable is missing.

| Setting | Default | What it does |
| --- | --- | --- |
| `server.listen` / `server.admin_listen` | `0.0.0.0:8080` / `0.0.0.0:9090` | API port; health and metrics port |
| `server.request_timeout_ms` | `9000` | Longest a call may take, queueing included. Kept under the SDKs' 10 s timeout so the caller gets the gateway's 504 instead of a blind client retry |
| `[[backend]] name` | required | Shows up in metrics, logs and the `x-systemone-gateway-backend` header |
| `[[backend]] protocol` | `systemone` | `systemone` or `chat` (see [Backends](#backends)) |
| `[[backend]] base_url` | TypeSafe for `systemone`, required for `chat` | Where the backend listens |
| `[[backend]] api_key_env` | unset: no key sent | Variable holding the backend's key |
| `[[backend]] models` | `["*"]` | Model names and `prefix*` patterns the backend serves |
| `[[backend]] upstream_model`, `top_logprobs`, `request_extras` | unset, `5`, none | `chat` only: model id sent upstream, candidate tokens asked for, extra request fields |
| `[[backend]] requests_per_minute`, `burst`, `tokens_per_second` | `1200`, one second's worth, `250000` | The backend's limits, shared by all services; on a chat backend, each question counts as one request |
| `[[backend]] max_concurrency` | `64` | Batches in flight to the backend |
| `[[backend]] max_retries`, `backoff_*_ms`, `attempt_timeout_ms` | `3`, `200` to `3000`, `5000` | Retry policy |
| `[[backend]] max_queue_wait_ms` | `2000` | Longest wait for backend capacity on top of the merge window; past it, a 429 with a retry-after |
| `coalescing.window_ms` | `10` | How long a batch waits for company; `0` turns merging off |
| `coalescing.max_questions`, `max_request_tokens`, `max_state_plus_question_tokens` | `128`, `56000`, `28000` | What one merged call may carry; kept under TypeSafe's 64k and 32k |
| `[[service]] key_sha256` | required | One or more key hashes; two let you rotate a key without downtime |
| `[[service]] requests_per_minute`, `burst`, `max_concurrent` | unset | The service's own share |
| `[[service]] allowed_models` | any | Models the service may ask for; `prefix*` patterns work here too |

`systemone-gateway gen-key` creates a key and prints its hash;
`systemone-gateway hash-key` hashes a key read from standard input.

Configurations written for earlier versions used a single `[upstream]`
table. The gateway now refuses it with a message: rename it to `[[backend]]`
and add `name = "typesafe"` and `api_key_env = "TYPESAFE_API_KEY"`.

## API

| Endpoint | Port | |
| --- | --- | --- |
| `POST /v1/systemone` | public | Same request and response as TypeSafe, whatever the backend |
| `GET /v1/models` | public | Every backend's models: a System One backend's own list, cached for `models_cache_ttl_ms`, and the exact names a chat backend lists in `models` |
| `GET /healthz`, `GET /readyz` | admin | Liveness; readiness, which turns 503 on shutdown |
| `GET /metrics` | admin | Prometheus / OpenMetrics |

Response headers: `x-systemone-gateway-backend` (the backend that
answered), `x-systemone-gateway-batch-callers` (how many calls shared the
upstream request), `x-typesafe-request-id` (the backend's request id, the
same for every caller of a merged request; for a chat backend, the id of the
first question's call) and `x-request-id`.

Errors the gateway produces itself have the shape
`{"error": {"type", "message", "param"}}`:

| Status | Type | When |
| --- | --- | --- |
| 400 | `invalid_request_error` | The body is not a JSON object |
| 401 | `authentication_error` | Missing or unknown service key |
| 403 | `permission_error` | Model not in the service's `allowed_models` |
| 422 | `validation_error` | A documented rule is broken, no backend serves the model, or a chat backend cannot express the question; `param` names the field |
| 429 | `rate_limit_error` | Service over its own quota, backend quota booked past `max_queue_wait_ms`, or the backend asked to back off. Always carries `retry-after` and `retry-after-ms` |
| 502 | `upstream_error` | Backend unreachable, refusing the gateway's key, or answering something unreadable, such as a chat model whose first token is no label |
| 504 | `timeout_error` | No answer within `request_timeout_ms` |

Any other error status comes from the backend and is passed through with its
body, after retries for 429, 529 and 5xx. A chat backend's error body is in
its own format, not TypeSafe's. A 401 from a backend means the gateway's own
key is wrong, so callers get a 502 instead.

## Metrics

All prefixed `systemone_gateway_`:

- `calls_total{service,status}`, `call_duration_seconds{service}`, `questions_total{service}`
- `input_tokens_total{service,backend}`: tokens charged to each service, merged calls split
- `upstream_calls_total{backend,status}`, `upstream_duration_seconds{backend}`, `upstream_retries_total{backend}`: HTTP calls, so one per question on a chat backend
- `batch_callers`, `batch_questions`, `queue_wait_seconds`: how much merging happens and what it costs in latency
- `deduplicated_questions_total`, `estimated_tokens_saved_total` (System One backends only), `isolated_replays_total`

Logs never contain states or questions, only the service, the backend, the
request id, the status and timings. Set `server.log_format = "json"` in
production and use `RUST_LOG` to change the level.

## Docker and Kubernetes

```sh
docker build -t systemone-gateway .
docker run -p 8080:8080 -p 9090:9090 \
  -v "$PWD/gateway.toml:/etc/systemone-gateway/gateway.toml:ro" \
  -e TYPESAFE_API_KEY -e HF_TOKEN \
  systemone-gateway
```

The image is distroless and runs as non-root (about 64 MB). In Kubernetes,
point the readiness probe at `/readyz` and the liveness probe at `/healthz`,
both on port 9090. On SIGTERM the gateway stops taking connections and lets
in-flight calls finish, so give `terminationGracePeriodSeconds` a few seconds
more than `request_timeout_ms`.

## Limits

- Pacing, quotas and merging live in memory, so they apply per process. With
  N replicas, give each one 1/N of each backend's limits, and expect merging
  only between calls that reach the same replica.
- A chat backend only approximates System One, with the limits listed under
  [`protocol = "chat"`](#protocol--chat).
- Token counts are estimates. Jev's tokenizer is not published, so the
  gateway counts 3 bytes per token (`coalescing.bytes_per_token`). The
  estimate sizes merged calls, books the tokens-per-second budget (corrected
  with the real `usage` after each call) and weights the usage split, where
  only ratios matter.
- TypeSafe's limits change without notice
  ([models](https://docs.typesafe.ai/models.md)). The gateway backs off on
  every 429 but does not learn a lower rate. If
  `upstream_calls_total{status="429"}` keeps growing, lower that backend's
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
| `backend`, `pattern` | Backends, routing a model to one, model name patterns |
| `chat` | The `chat` protocol: a prompt per question, answers read from logprobs |
| `coalescer` | Opens, joins and seals batches |
| `dispatch` | Sends a sealed batch, answers each caller, replays after a rejection |
| `batch` | Builds the merged request and splits the answer |
| `upstream` | HTTP client of one backend, retries, `retry-after` parsing |
| `limiter` | GCRA pacing for requests, tokens and per-service quotas |
| `protocol`, `json`, `validate` | Wire format kept as raw JSON, merge keys, documented rules |
| `usage` | Largest-remainder split of token usage |
| `services`, `config`, `metrics`, `error`, `app` | Keys and quotas, configuration, Prometheus, error shape, wiring |
