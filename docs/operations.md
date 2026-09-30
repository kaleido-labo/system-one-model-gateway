# Operations

Running, watching and tuning the gateway.

- [Docker](#docker)
- [Kubernetes](#kubernetes)
- [Logs](#logs)
- [Metrics](#metrics)
- [Tuning](#tuning)
- [Timeouts and queue deadlines](#timeouts-and-queue-deadlines)
- [Known limits](#known-limits)

## Docker

```sh
docker build -t systemone-gateway .
docker run -p 8080:8080 -p 9090:9090 \
  -v "$PWD/gateway.toml:/etc/systemone-gateway/gateway.toml:ro" \
  -e TYPESAFE_API_KEY -e HF_TOKEN \
  systemone-gateway
```

- The image is a two-stage build. The runtime stage is distroless, runs as a
  non-root user and weighs about 64 MB.
- It runs `systemone-gateway serve` and reads the configuration from
  `/etc/systemone-gateway/gateway.toml` (set by `SYSTEMONE_GATEWAY_CONFIG`).
  Mount your file there.
- Pass each backend's key as the environment variable named by its
  `api_key_env`. The gateway refuses to start when one is missing.
- The image exposes 8080 (API) and 9090 (health and metrics). To run another
  command, such as `check-config`, put it after the image name:
  `docker run --rm -v ... systemone-gateway check-config`.
- The gateway serves plain HTTP and its admin port has no authentication.
  Terminate TLS in front of the public port, and keep port 9090 off the public
  network.

## Kubernetes

- Put the configuration in a ConfigMap mounted at
  `/etc/systemone-gateway/gateway.toml`. It holds no secret.
- Put each backend's key in a Secret and expose it as the environment variable
  that `api_key_env` names.
- Point the liveness probe at `GET /healthz` and the readiness probe at
  `GET /readyz`, both on port 9090.
- On SIGTERM the gateway stops accepting connections and lets in-flight calls
  finish. Set `terminationGracePeriodSeconds` a few seconds above
  `server.request_timeout_ms`.
- Scrape `GET /metrics` on port 9090.

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
```

Pacing, quotas and merging live in memory, so they apply per process. With N
replicas, give each replica 1/N of each backend's `requests_per_minute` and
`tokens_per_second`, and expect merging only between calls that reach the same
replica. If merging matters, run fewer and larger replicas, or route calls for
the same state to the same replica.

## Logs

Logs go to standard output. `server.log_format = "json"` gives one JSON object
per line, which is what you want in production; `"text"` is easier to read in a
terminal. Colours appear only when the output is a terminal. `RUST_LOG` sets
the level and filters, and defaults to `info`:

```sh
RUST_LOG=info                          # default
RUST_LOG=info,systemone_gateway=debug  # adds one line per upstream call
```

Logs never contain states or questions, because they can hold personal or
financial data. They carry the service, the backend, the request id, the status
and timings.

| Level | Line | When |
| --- | --- | --- |
| info | `gateway listening` | Startup, with the addresses and the number of services and backends. |
| info | `finished processing request` | Every call on the public port, with `method`, `path`, `request_id`, `service`, `status` and `latency`. `service` is missing when the call was refused before authentication. |
| warn | `retrying the upstream call` | A retry, with `backend`, `attempt`, `delay_ms` and `failure`. |
| warn | `upstream call failed` | A call failed after its retries. |
| warn | `the backend rejected a merged call; replaying each call on its own` | A 400 or 422 on a merged call. |
| error | `the backend refused the gateway's API key` | A backend answered 401. Fix the key: every call to that backend fails with a 502 until you do. |
| error | `the answer could not be split` | A backend's response was unreadable or had no answer for a question. |
| debug | `upstream call answered` | One per upstream call, with the callers and questions it carried. |

The `x-request-id` of a response is the `request_id` of its log line.

## Metrics

`GET /metrics` on the admin port serves Prometheus metrics in OpenMetrics text.
Every name starts with `systemone_gateway_`.

### Per call (service side)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `calls_total` | counter | `service`, `status` | Calls received, by response status. A call refused before authentication has `service="-"`. A body refused for its size (413) is not counted. |
| `call_duration_seconds` | histogram | `service` | Time from receiving a call to answering it, queueing included. |
| `questions_total` | counter | `service` | Questions asked. Counted once a call has been accepted for batching: calls refused earlier (validation, 403, service limits, a booked-out backend) add nothing. |
| `input_tokens_total` | counter | `service`, `backend` | Input tokens charged to each service. Merged calls are split between their callers, and the shares add up to the backend's bill. |

### Per upstream call (backend side)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `upstream_calls_total` | counter | `backend`, `status` | HTTP calls to a backend, by final status after retries. `status="0"` means the backend could not be reached. A chat backend makes one call per question. |
| `upstream_duration_seconds` | histogram | `backend` | Time spent in a backend's HTTP calls, retries included. |
| `upstream_retries_total` | counter | `backend` | Attempts retried after a 429, 500, 502, 503, 504, 529 or network error. |

### Merging

| Metric | Type | Meaning |
| --- | --- | --- |
| `batch_callers` | histogram | Service calls answered by one batch. Mostly 1 means little merging. |
| `batch_questions` | histogram | Distinct questions in one batch. |
| `queue_wait_seconds` | histogram | Time from a batch's first call arriving to the batch going upstream. The cost of merging and pacing in latency. |
| `deduplicated_questions_total` | counter | Questions left out because an identical one was already in the same upstream call. |
| `estimated_tokens_saved_total` | counter | Estimated input tokens not billed thanks to merging. System One backends only, because a chat backend repeats the state for every question. It is an estimate. |
| `isolated_replays_total` | counter | Calls replayed alone after the merged call they were in was rejected. |

The three merging histograms have no `backend` label: they aggregate all
backends.

### Useful queries

```promql
# Share of calls answered with 429, per service
sum by (service) (rate(systemone_gateway_calls_total{status="429"}[5m]))
  / sum by (service) (rate(systemone_gateway_calls_total[5m]))

# Backend 429s and retries
sum by (backend) (rate(systemone_gateway_upstream_calls_total{status="429"}[5m]))
sum by (backend) (rate(systemone_gateway_upstream_retries_total[5m]))

# How many calls share one upstream call
rate(systemone_gateway_batch_callers_sum[5m]) / rate(systemone_gateway_batch_callers_count[5m])

# 95th percentile of the time a batch waits before going upstream
histogram_quantile(0.95, sum by (le) (rate(systemone_gateway_queue_wait_seconds_bucket[5m])))

# Input tokens per service
sum by (service, backend) (rate(systemone_gateway_input_tokens_total[1h]))
```

## Tuning

Start from the limits your providers give you, then adjust with the metrics.

**Backend limits.** `requests_per_minute` and `tokens_per_second` should match
what the account may use, divided by the number of replicas. The defaults
(1200 and 250000) are starting points, and a provider can change its limits
without notice. If `upstream_calls_total{status="429"}` keeps growing, the
limit is set too high: lower that backend's `requests_per_minute`. The gateway
backs off on every 429 but does not learn a lower rate by itself.

**Concurrency.** `max_concurrency` bounds the batches in flight to a backend.
The number you need is about the batches per second times the upstream latency
in seconds. Raise it if `queue_wait_seconds` is high while the request limit
is not reached. Keep it above that product, or batches queue for a connection
instead of a request slot.

**Merging.**

- Raising `window_ms` gives calls more time to meet, at the price of latency
  for every call. The window is a floor: under load, a batch waits for its
  request slot and keeps collecting calls anyway.
- `batch_callers` averaging 1 means services rarely ask about the same state at
  the same moment; the window is then pure latency, and `window_ms = 0` is
  reasonable.
- Keep `max_request_tokens` and `max_state_plus_question_tokens` below the
  provider's limits. If TypeSafe rejects large merged calls as too big, lower
  those two, or lower `bytes_per_token` so that the gateway counts more tokens
  per byte.

**Per-service quotas.** Set `requests_per_minute` and `max_concurrent` on each
service so that one noisy service cannot use up the shared budget. The sum of
the services' rates can exceed the backend's limit: they are caps, not
reservations.

**Chat backends.** Every question counts as a request, so set
`requests_per_minute` in questions per minute, and expect a call with 10
questions to book 10 slots.

## Timeouts and queue deadlines

A call can wait in three places, and the settings are nested:

```
SDK timeout (10 s by default, in the caller)
  > server.request_timeout_ms (9000)      the gateway's own deadline: 504
      > backend.max_queue_wait_ms (2000)  wait for capacity, on top of the window: 429
```

- `max_queue_wait_ms` must be smaller than `request_timeout_ms`; the gateway
  refuses the configuration otherwise, because queued calls would time out
  before they were sent.
- `request_timeout_ms` should stay under the TypeSafe SDKs' 10 s default
  timeout. The caller then gets the gateway's 504, which it handles, instead of
  giving up on its own and retrying blindly.
- A call that leaves after waiting `max_queue_wait_ms` still needs time for
  the backend to answer. With the defaults, 2 s of queueing leaves 7 s before
  the 504, and one attempt may take up to `attempt_timeout_ms` (5 s), so a
  retry after a slow attempt may not fit. Lower `max_queue_wait_ms` to shed
  load earlier and leave more room; raise it, along with `request_timeout_ms`,
  to absorb longer bursts.
- When a call is shed, the 429 carries `retry-after` and `retry-after-ms`. The
  TypeSafe SDKs honour them, so services back off without code of their own.
- A merged call is bound by its most patient caller's deadline: a caller whose
  deadline passes first gets its 504 while the others still get answers.

## Known limits

- **Per process.** Pacing, quotas and merging live in memory. See
  [Kubernetes](#kubernetes) for running several replicas.
- **Chat backends approximate System One.** See the
  [limits of chat backends](backends.md#limits-compared-to-a-system-one-backend).
- **Token counts are estimates.** Jev's tokenizer is not published, so the
  gateway counts `bytes_per_token` bytes per token. The estimate sizes merged
  calls, books the tokens-per-second budget (corrected with the real `usage`
  after each call) and weights the usage split, where only ratios matter.
- **Provider limits move.** TypeSafe's limits change without notice
  ([models](https://docs.typesafe.ai/models.md)), and the gateway does not learn
  them.
- **No answer cache.** Two identical calls a minute apart cost two upstream
  requests. Only calls that are in the gateway at the same time share one.
- **No TLS and no admin authentication.** Put the gateway behind a proxy or
  mesh that terminates TLS, and keep the admin port private.
