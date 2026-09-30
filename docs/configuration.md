# Configuration reference

The gateway reads one TOML file. [`config.example.toml`](../config.example.toml)
is a complete, commented starting point; this page lists every key.

The file holds no secret. Services are identified by the SHA-256 of their key,
and each backend's key and the Redis URL come from environment variables that
the file names. It can live in git or in a Kubernetes ConfigMap.

## Loading and checking

| Command | What it does |
| --- | --- |
| `systemone-gateway serve [--config PATH]` | Runs the gateway. |
| `systemone-gateway check-config [--config PATH]` | Validates the file and prints what it sets up. It does not read the backends' environment variables, so it passes on a machine that has no keys. |
| `systemone-gateway gen-key [--service NAME]` | Makes a service key and prints it with a ready-to-paste `[[service]]` block. The key is not stored anywhere. |
| `systemone-gateway hash-key` | Prints the SHA-256 of a key read from the first line of standard input. |

The path comes from `--config`, then the `SYSTEMONE_GATEWAY_CONFIG`
environment variable, then `gateway.toml` in the current directory.

Unknown keys are rejected, so a typo stops the gateway at startup instead of
being ignored. A backend's key variable, or the admin token's, that is unset or
empty also stops `serve`, with a message naming the variable, and so does the
Redis URL variable when `[cluster]` is configured.

Environment variables the gateway reads:

| Variable | Used for |
| --- | --- |
| `SYSTEMONE_GATEWAY_CONFIG` | Path of the configuration file. |
| The variable named by a backend's `api_key_env` | That backend's API key. |
| The variable named by `server.admin_token_env` | The bearer token that `/metrics` requires. |
| The variable named by `cluster.redis_url_env` | The Redis URL, only when `[cluster]` is configured. |
| `TYPESAFE_API_KEY` | The TypeSafe key, but only when the file has no `[[backend]]` block (see below). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Base URL of an OTLP/HTTP collector, used only when `tracing.otlp_endpoint` is not set. See [`[tracing]`](#tracing). |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | The same for traces only, as a full URL. It wins over the variable above. |
| `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT` | Read by the OTLP exporter itself, for example to send an API key to a hosted collector. Their `OTEL_EXPORTER_OTLP_TRACES_*` forms work too. |
| `RUST_LOG` | Log level and filters, in the [`tracing-subscriber` syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). Defaults to `info`. |

## File layout

```toml
[server]          # listeners, timeouts, logging
[[backend]]       # one block per model provider; repeat as needed
[coalescing]      # merging of calls that share a state
[cache]           # optional answer cache, off by default
[tracing]         # OpenTelemetry traces, off unless an endpoint is set
[[service]]       # one block per calling service; at least one is required
[cluster]         # optional: replicas share their pacing through Redis
```

Every table except `[[service]]` may be left out. With no `[[backend]]` block,
the gateway uses one backend named `typesafe` that serves every model at
`https://api.typesafe.ai`, with its key in `TYPESAFE_API_KEY`.

Versions before `[[backend]]` existed used an `[upstream]` table. The gateway
refuses it with a message: rename it to `[[backend]]` and add
`name = "typesafe"` and `api_key_env = "TYPESAFE_API_KEY"`.

## `[server]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `listen` | socket address | `0.0.0.0:8080` | Public API: `POST /v1/systemone` and `GET /v1/models`. |
| `admin_listen` | socket address | `0.0.0.0:9090` | `/healthz`, `/readyz` and `/metrics`. Keep this port off the public network. |
| `admin_token_env` | string | unset | Name of the environment variable that holds a bearer token for `/metrics`. Unset leaves `/metrics` open. When set, `/metrics` answers 401 unless the request carries `Authorization: Bearer <token>`. `/healthz` and `/readyz` never ask for it, because Kubernetes probes cannot easily send a header. An empty name is refused. |
| `request_timeout_ms` | integer, > 0 | `9000` | Longest a call may take from arrival to answer, queueing included. Past it the caller gets a 504. The TypeSafe SDKs time out after 10 s by default, so the default stays under that: callers get the gateway's 504 instead of a client-side timeout that the SDK would retry blindly. |
| `max_body_bytes` | integer, > 0 | `2097152` | Largest request body. Larger requests get a 413. |
| `log_format` | `"text"` or `"json"` | `"text"` | Log line format. Use `"json"` in production. |

## `[[backend]]`

One block per model provider. See [Backends](backends.md) for what each
protocol does. Keys marked "chat only" or "System One only" are rejected on the
other protocol, except where noted.

### Identity and routing

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Appears in metrics, logs and the `x-systemone-gateway-backend` response header. Letters, digits, `-`, `_` and `.`. Unique. |
| `protocol` | `"systemone"` or `"chat"` | `"systemone"` | How the gateway talks to the backend. |
| `base_url` | URL (`http` or `https`) | `https://api.typesafe.ai` for `systemone`; required for `chat` | For `systemone`, the gateway calls `<base_url>/v1/systemone` and `<base_url>/v1/models`. For `chat`, it is the URL that `chat/completions` hangs off, such as `https://router.huggingface.co/v1`. A path in the URL is kept. |
| `api_key_env` | string | unset | Name of the environment variable that holds the backend's key, sent as `Authorization: Bearer <key>`. Unset sends no key, for a server on a private network. An empty name is refused. |
| `models` | list of strings | `["*"]` | Model names this backend serves. An entry is an exact name, a prefix ending in `*` (`"jev-*"`), or `"*"` alone. `*` is allowed only at the end. Must not be empty. |

A call goes to the backend with the most specific match: an exact name first,
then the longest prefix, whatever the order of the blocks. The same entry
cannot appear in two backends. A model that no backend serves gets a 422.

### Chat backends only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `upstream_model` | string | unset | Model id sent upstream in place of the name the service asked for. Unset forwards the service's name unchanged. Refused on a `systemone` backend. |
| `top_logprobs` | integer, 1 to 20 | `5` | Candidate tokens requested with each answer. The Hugging Face router accepts at most 5; vLLM and TGI accept up to 20. The key is ignored on a `systemone` backend. |
| `request_extras` | table | empty | Extra fields merged into every chat request. The gateway's own fields (`model`, `messages`, `max_tokens`, `logprobs`, `top_logprobs`, `stream`) win. Refused on a `systemone` backend. |

`request_extras` can be an inline table (`request_extras = { temperature = 0 }`)
or a sub-table (`[backend.request_extras]`) placed right after its backend.

### System One backends only

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `models_cache_ttl_ms` | integer | `300000` | How long the backend's own `GET /v1/models` answer is kept in memory. A chat backend ignores it, because it lists the exact names in `models` without calling upstream. |

### Answer cache

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `cache` | boolean | `true` | Lets the [answer cache](#cache) keep and serve this backend's answers. It does nothing while `[cache] enabled` is `false`. Set it to `false` for a backend whose answers should differ from one call to the next, such as a chat model sampled at a temperature above zero (`request_extras = { temperature = 0.7 }`). |

### Circuit breaker and fallback

See [Fallback and the circuit breaker](backends.md#fallback-and-the-circuit-breaker)
for how they work together.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `circuit_breaker_failures` | integer | `5` | Failed upstream calls in a row that open the backend's breaker. A call fails when its final outcome, after the retries, is a network error or timeout, a 5xx (529 included), or a 401 (the backend refusing the gateway's key). A 429 or a client error (400, 403, 422...) is not a failure, because the backend answered. `0` turns the breaker off. |
| `circuit_breaker_cooldown_ms` | integer, > 0 | `30000` | How long an open breaker turns calls away before it lets one trial call through. Ignored when `circuit_breaker_failures = 0`. |
| `fallback` | list of backend names | empty | Backends that take over a call when this one is unavailable, in order of preference. Each name must be another `[[backend]]`, listed once, and the lists must not lead back to where they started. The fallback does not need to list the model in its `models`. |

### Limits and pacing

These limits describe the backend and are shared by every service. They are
enforced per gateway process, so with N replicas give each one 1/N of them.
With [`[cluster]`](#cluster) the replicas draw on one budget instead: give
every replica the full limits. `max_concurrency` stays per process either way.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `requests_per_minute` | integer, > 0 | `1200` | Upstream requests per minute. On a chat backend each question is one request. |
| `burst` | integer | `requests_per_minute / 60`, at least 1 | Requests that may leave back to back before pacing starts. |
| `tokens_per_second` | integer, > 0 | `250000` | Estimated input tokens per second. One second's worth may go at once. The estimate is corrected with the real `usage` after each answer. |
| `max_concurrency` | integer, > 0 | `64` | Batches in flight at once. A chat batch counts once, however many questions it holds. |

The defaults are the gateway's own starting values, not a promise from any
vendor. TypeSafe's limits change without notice (see
[Operations](operations.md#tuning)); set these to what your account allows.

### Adaptive rate

Off by default. With `adaptive_rate = true`, `requests_per_minute` becomes a
ceiling: the gateway lowers the backend's request rate when the backend answers
429, and raises it again, step by step, while the backend stays quiet. See
[Pace](architecture.md#adaptive-rate) for how it decides and
[Operations](operations.md#tuning) for when to use it. `tokens_per_second` and
the services' own rates do not adapt.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `adaptive_rate` | boolean | `false` | Turns the adaptive rate on for this backend. |
| `adaptive_min_requests_per_minute` | integer, 1 to `requests_per_minute` | a tenth of `requests_per_minute`, at least 1 | The floor: the rate never goes below it, however many 429s come. |
| `adaptive_decrease` | number, above 0 and below 1 | `0.7` | What one congestion episode multiplies the rate by. `0.5` halves it. |
| `adaptive_increase_per_minute` | integer, 1 to `requests_per_minute` | a twentieth of `requests_per_minute`, at least 1 | Requests per minute added back at each recovery step. |
| `adaptive_recovery_ms` | integer, > 0 | `10000` | How long without a 429 before each recovery step. It is also the length of an episode: 429s less than this far apart after a decrease lower the rate once, not again. |

The three limits are checked even while `adaptive_rate` is off, so a typo shows
up before anyone turns it on. `burst` is not scaled with the rate: it stays the
number of requests that may go back to back. A chat backend adapts like any
other, in questions per minute.

```toml
[[backend]]
name = "typesafe"
requests_per_minute = 1200        # the ceiling
adaptive_rate = true
adaptive_min_requests_per_minute = 120
adaptive_decrease = 0.7           # 1200 -> 840 -> 588 -> ... down to 120
adaptive_increase_per_minute = 60 # 10 s of quiet: +60 per minute, up to 1200
adaptive_recovery_ms = 10000
```

With [`[cluster]`](#cluster), each replica adapts its own rate. See
[Operations](operations.md#adaptive-rate-and-replicas).

### Timeouts and retries

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `connect_timeout_ms` | integer | `2000` | Time allowed to open a connection. |
| `attempt_timeout_ms` | integer, > 0 | `5000` | Time allowed for one attempt. A retry gets a fresh allowance. Each attempt is also cut off at the call's deadline. |
| `max_retries` | integer | `3` | Retries after the first attempt, for 429, 500, 502, 503, 504, 529 and network errors. `0` turns retries off. |
| `backoff_initial_ms` | integer | `200` | Wait before the first retry. It doubles with each retry, loses up to 25% to jitter, and is capped by `backoff_max_ms`. |
| `backoff_max_ms` | integer | `3000` | Longest backoff between retries. |
| `max_queue_wait_ms` | integer | `2000` | Longest a call waits for upstream capacity (a request slot, a free connection, the token budget), on top of the merge window. Past it the caller gets a 429 with `retry-after`. Must be smaller than `server.request_timeout_ms`. |

A `retry-after-ms` or `retry-after` header from the backend replaces the
computed backoff. Values above 60 seconds are capped at 60 seconds.

## `[coalescing]`

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `window_ms` | integer | `10` | How long a new batch stays open for other calls with the same model, state and top-level fields. It then stays open while it waits for upstream capacity. `0` turns merging off. |
| `max_questions` | integer, > 0 | `128` | Most distinct questions in one merged call. |
| `max_request_tokens` | integer, > 0 | `56000` | Estimated tokens one merged call may carry. TypeSafe's limit is 64k; the gateway stays under it because the estimate is approximate. |
| `max_state_plus_question_tokens` | integer, > 0 | `28000` | Estimated tokens of the state plus the longest question in a merged call. TypeSafe's limit is 32k. |
| `bytes_per_token` | number, > 0 | `3.0` | Bytes of minified JSON counted as one token. TypeSafe does not publish Jev's tokenizer, so this is a conservative guess. It sizes merged calls, books the tokens-per-second budget and weights the usage split. |

These settings apply to every backend. `max_request_tokens` and
`max_state_plus_question_tokens` mirror System One's limits. A chat backend
sends one request per question, but it still forms batches under the same
limits, so they cap the size of a batch there too.

## `[cache]`

An in-memory cache of answers, off by default. It is one table for the whole
gateway, not one per backend, because the memory bound and the time to live are
what an operator reasons about for the process. A backend that must not be
cached opts out with `cache = false` in its own block.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `enabled` | boolean | `false` | Turns the cache on. Cached answers trade freshness for fewer upstream requests, so the operator chooses that. |
| `ttl_ms` | integer, > 0 | `300000` | How long an answer may be served after the backend gave it. A read does not extend it. |
| `max_entries` | integer, > 0 | `10000` | Most answers held at once, one per question. When the cache is full, the oldest answer makes room. |

An answer is kept per question, under the model, the state, the extra top-level
fields and the question itself, so it is only served for exactly the same call.
Answers are small (a few hundred bytes), so the default bound is a few
megabytes. The cache lives in the process: a restart empties it, and with
several replicas each one has its own. See [Architecture](architecture.md#answer-cache)
for how a call uses it and [Operations](operations.md#metrics) for what to watch.

## `[tracing]`

Distributed traces, exported over OTLP. Off unless an endpoint is set. See
[Traces](operations.md#traces) for what the gateway records.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `otlp_endpoint` | URL (`http` or `https`) | unset | Base URL of an OTLP/HTTP collector, such as `http://localhost:4318`. The gateway posts to `<otlp_endpoint>/v1/traces`. Unset turns tracing off, unless the environment names an endpoint (below). The collector must speak OTLP over HTTP with protobuf, usually on port 4318: gRPC (port 4317) is not supported. |
| `service_name` | string | `"systemone-gateway"` | The `service.name` of the exported spans, which is how the gateway appears in the tracing backend. |
| `sample_ratio` | number, 0 to 1 | `1.0` | Share of the traces the gateway starts that are kept. A call that arrives with a `traceparent` follows its caller's sampling decision instead, so this applies only to calls that arrive without one. |

The endpoint is taken from the first of these that is set:

1. `tracing.otlp_endpoint` in the file, a base URL to which `/v1/traces` is added.
2. The `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` variable, a full URL used as it is.
3. The `OTEL_EXPORTER_OTLP_ENDPOINT` variable, a base URL like the file's.

The file wins, as a setting made in code does everywhere in OpenTelemetry. An
empty variable counts as unset. With none of the three set, no exporter is
created, the gateway neither reads nor sends a `traceparent`, and the spans cost
next to nothing. `check-config` prints the endpoint `serve` would use.

The file has no key for headers, timeouts or compression. Use
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT` and the like, which
the exporter reads itself.

## `[[service]]`

One block per calling service. At least one is required.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `name` | string | required | Appears in metrics and logs. Letters, digits, `-`, `_` and `.`. Unique. |
| `key_sha256` | list of strings | required | SHA-256 of each key the service may present, in hex (64 characters). Give two to rotate a key without downtime. A hash cannot belong to two services. |
| `requests_per_minute` | integer, > 0 | unset | The service's own share of requests per minute. Unset means it is bound only by the backends' limits. Over it, the service gets a 429. |
| `burst` | integer | `requests_per_minute / 6`, at least 1 | Requests the service may send back to back. Ten seconds' worth of its rate by default. Has no effect without `requests_per_minute`. |
| `max_concurrent` | integer, > 0 | unset | Calls the service may have in flight at once. Over it, the service gets a 429. |
| `allowed_models` | list of strings | unset | Models the service may ask for, as exact names or `prefix*` patterns. Unset allows any model. Anything else gets a 403. Must not be empty when set. |

The service's limits are checked once the request has been validated and
routed. A call that passes spends one request of the service's budget, even if
it is later refused upstream.

With [`[cluster]`](#cluster), `requests_per_minute` is a limit of the whole
cluster: a call that one replica admitted counts against the service on every
replica. `max_concurrent` counts the calls in flight in this process, so each
replica allows that many.

## `[cluster]`

Optional. Without it, every limit lives in memory, per process, and Redis is
never contacted. With it, the replicas of a gateway keep these limits in Redis
and draw on one budget:

- each backend's `requests_per_minute` and `tokens_per_second`;
- each service's `requests_per_minute`;
- the pause that follows a 429 from a backend, so every replica pauses, not
  only the one that got the 429.

Set these to the limits of the whole cluster, the same on every replica. What
stays per process, because it describes this process: `max_concurrency`,
`max_concurrent`, and merging. See [Operations](operations.md#sharing-the-limits-between-replicas)
for how it behaves, and what happens when Redis is down.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `redis_url_env` | string | required | Name of the environment variable that holds the Redis URL, such as `redis://:password@redis.internal:6379/0`. A variable, because the URL can carry a password. The gateway refuses to start when it is unset or empty, or does not hold a `redis://` URL. `rediss://` (TLS) is not supported. |
| `key_prefix` | string | `"systemone-gateway"` | Put in front of every key the gateway writes, so that several gateways or environments can share one Redis. Letters, digits, `-`, `_`, `.` and `:`. |
| `redis_timeout_ms` | integer, > 0 | `200` | Longest the gateway waits for Redis on one booking. Past it, that booking is paced on this replica's own share (see `expected_replicas`) and the call goes on. |
| `expected_replicas` | integer, > 0 | `1` | How many replicas share the limits. Used only while Redis cannot be reached: each replica then paces with the limits above divided by this number. Set it to the number of replicas you run. |

The gateway needs Redis 5 or later, or Valkey, as a single endpoint: Redis
Cluster and Sentinel are not supported. It does not connect at startup,
so a Redis that comes up after the gateway does not stop it from starting.

```toml
[cluster]
redis_url_env = "REDIS_URL"
key_prefix = "systemone-gateway"
expected_replicas = 3
```

## Validation summary

`check-config` and `serve` both refuse a file where:

- a name is empty, contains another character, or is used twice (backends and
  services are checked separately);
- a `base_url` is not an `http` or `https` URL;
- a `chat` backend has no `base_url`, or its `top_logprobs` is outside 1 to 20;
- `upstream_model` or `request_extras` is set on a `systemone` backend;
- a model pattern is empty or has a `*` anywhere but the end;
- two backends list the same model entry;
- a rate, a timeout or a size that must be positive is zero, `cache.ttl_ms` and
  `cache.max_entries` included;
- `server.admin_token_env` is an empty name;
- `tracing.otlp_endpoint` is not an `http` or `https` URL, `tracing.service_name`
  is empty, or `tracing.sample_ratio` is outside 0 to 1;
- a backend's `max_queue_wait_ms` is not smaller than `server.request_timeout_ms`;
- `circuit_breaker_cooldown_ms` is zero while the breaker is on;
- a `fallback` names a backend that does not exist, names the backend itself,
  lists a name twice, or leads back to a backend already in its chain;
- a service has no `key_sha256`, a hash that is not 64 hexadecimal characters,
  or a hash shared with another service;
- no `[[service]]` block exists, because nobody could call the gateway;
- `[cluster]` has an empty `redis_url_env`, a `key_prefix` that is empty or
  uses another character, or a zero `redis_timeout_ms` or `expected_replicas`.

`serve` also refuses to start when the variable named by `cluster.redis_url_env`
is empty, or holds something other than a `redis://` URL.
