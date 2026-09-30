# Configuration reference

The gateway reads one TOML file. [`config.example.toml`](../config.example.toml)
is a complete, commented starting point; this page lists every key.

The file holds no secret. Services are identified by the SHA-256 of their key,
and each backend's key comes from an environment variable that the file names.
It can live in git or in a Kubernetes ConfigMap.

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
empty also stops `serve`, with a message naming the variable.

Environment variables the gateway reads:

| Variable | Used for |
| --- | --- |
| `SYSTEMONE_GATEWAY_CONFIG` | Path of the configuration file. |
| The variable named by a backend's `api_key_env` | That backend's API key. |
| The variable named by `server.admin_token_env` | The bearer token that `/metrics` requires. |
| `TYPESAFE_API_KEY` | The TypeSafe key, but only when the file has no `[[backend]]` block (see below). |
| `RUST_LOG` | Log level and filters, in the [`tracing-subscriber` syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html). Defaults to `info`. |

## File layout

```toml
[server]          # listeners, timeouts, logging
[[backend]]       # one block per model provider; repeat as needed
[coalescing]      # merging of calls that share a state
[[service]]       # one block per calling service; at least one is required
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

### Limits and pacing

These limits describe the backend and are shared by every service. They are
enforced per gateway process, so with N replicas give each one 1/N of them.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `requests_per_minute` | integer, > 0 | `1200` | Upstream requests per minute. On a chat backend each question is one request. |
| `burst` | integer | `requests_per_minute / 60`, at least 1 | Requests that may leave back to back before pacing starts. |
| `tokens_per_second` | integer, > 0 | `250000` | Estimated input tokens per second. One second's worth may go at once. The estimate is corrected with the real `usage` after each answer. |
| `max_concurrency` | integer, > 0 | `64` | Batches in flight at once. A chat batch counts once, however many questions it holds. |

The defaults are the gateway's own starting values, not a promise from any
vendor. TypeSafe's limits change without notice (see
[Operations](operations.md#tuning)); set these to what your account allows.

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

## Validation summary

`check-config` and `serve` both refuse a file where:

- a name is empty, contains another character, or is used twice (backends and
  services are checked separately);
- a `base_url` is not an `http` or `https` URL;
- a `chat` backend has no `base_url`, or its `top_logprobs` is outside 1 to 20;
- `upstream_model` or `request_extras` is set on a `systemone` backend;
- a model pattern is empty or has a `*` anywhere but the end;
- two backends list the same model entry;
- a rate, a timeout or a size that must be positive is zero;
- `server.admin_token_env` is an empty name;
- a backend's `max_queue_wait_ms` is not smaller than `server.request_timeout_ms`;
- a service has no `key_sha256`, a hash that is not 64 hexadecimal characters,
  or a hash shared with another service;
- no `[[service]]` block exists, because nobody could call the gateway.
