# Operations

Running, watching and tuning the gateway.

- [Docker](#docker)
- [Kubernetes](#kubernetes)
- [Logs](#logs)
- [Metrics](#metrics)
- [Traces](#traces)
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
  `api_key_env`, and the admin token as the one named by
  `server.admin_token_env` if you configure it. The gateway refuses to start
  when one is missing.
- The image exposes 8080 (API) and 9090 (health and metrics). To run another
  command, such as `check-config`, put it after the image name:
  `docker run --rm -v ... systemone-gateway check-config`.
- The gateway serves plain HTTP. Terminate TLS in front of the public port,
  and keep port 9090 off the public network. Its `/metrics` is open unless you
  configure `server.admin_token_env`, and `/healthz` and `/readyz` are always
  open.

## Kubernetes

- Put the configuration in a ConfigMap mounted at
  `/etc/systemone-gateway/gateway.toml`. It holds no secret.
- Put each backend's key in a Secret and expose it as the environment variable
  that `api_key_env` names. Do the same for the admin token, if you configure
  `server.admin_token_env`.
- Point the liveness probe at `GET /healthz` and the readiness probe at
  `GET /readyz`, both on port 9090.
- On SIGTERM the gateway stops accepting connections and lets in-flight calls
  finish. Set `terminationGracePeriodSeconds` a few seconds above
  `server.request_timeout_ms` (five more when [tracing](#traces) is on, for the
  last spans).
- Scrape `GET /metrics` on port 9090. With `server.admin_token_env` configured,
  `/metrics` needs `Authorization: Bearer <token>`: give the scraper the same
  token, as in the Prometheus example below. The probes need no header, because
  `/healthz` and `/readyz` stay open even then.

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 9090 }
readinessProbe:
  httpGet: { path: /readyz, port: 9090 }
```

A Prometheus scrape job for a gateway with an admin token. The token sits in a
file that Prometheus can read, mounted from the same Kubernetes Secret:

```yaml
scrape_configs:
  - job_name: systemone-gateway
    metrics_path: /metrics
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/gateway-admin-token
    static_configs:
      - targets: ["systemone-gateway:9090"]
```

Leave `authorization` out when the gateway has no admin token.

Without a `[cluster]` table, pacing, quotas and merging live in memory, so they
apply per process. With N replicas, give each replica 1/N of each backend's
`requests_per_minute` and `tokens_per_second`, and expect merging only between
calls that reach the same replica. If merging matters, run fewer and larger
replicas, or route calls for the same state to the same replica. To stop
dividing the limits by hand, share them through Redis.

### Sharing the limits between replicas

With [`[cluster]`](configuration.md#cluster), the replicas keep these limits in
Redis and draw on one budget: each backend's `requests_per_minute` and
`tokens_per_second`, each service's `requests_per_minute`, and the pause after a
backend's 429, so that every replica pauses. Give every replica the full limits
of your account instead of 1/N of them. Without `[cluster]`, the gateway never
contacts Redis and behaves as described above.

```toml
[cluster]
redis_url_env = "REDIS_URL"
expected_replicas = 3
```

- Put the Redis URL, which can carry a password, in a Secret and expose it as
  the environment variable that `redis_url_env` names.
- Redis 5 or later, or Valkey, as one endpoint and without TLS: Redis Cluster
  and Sentinel are not supported. One Redis can serve several gateways: set a
  different `key_prefix` for each.
- Every booking is one round trip to Redis. A call makes a handful: one for a
  service with a rate of its own, one when its batch opens, two for the token
  budget (the booking, then its correction with the real usage) and one before
  each upstream attempt. A chat backend adds one per extra question. Keep Redis
  close to the gateway, in the same zone. `redis_timeout_ms` bounds each of
  them.
- The limits are kept as GCRA state (two numbers per limit, under
  `<key_prefix>:backend:<name>:requests`, `:tokens` and
  `<key_prefix>:service:<name>:requests`), updated by one atomic script that
  reads Redis's clock, so replicas with drifting clocks still agree. A key
  expires once its limiter is idle, so Redis keeps nothing for long.
- These stay per process, because they describe this process: `max_concurrency`
  (connections to a backend), each service's `max_concurrent`, and merging.

**When Redis is down.** The gateway never fails a call because Redis does. A
booking that cannot reach Redis within `redis_timeout_ms` is made on this
replica's own share of the limit instead: the limits divided by
`expected_replicas`, which defaults to 1, so set it to the number of replicas
you run or every replica will use the whole limit while Redis is out. The
gateway logs one warning when an outage starts (not one per call), counts every
failed booking in `shared_limiter_errors_total`, leaves Redis alone for a
second before trying again, and logs once more when Redis answers. Shared
pacing resumes by itself. A pause after a 429 that this replica knows of still
holds during the outage. Bookings made during an outage are not charged to
Redis afterwards, so the cluster can briefly go over its limit at the changeover.
Alert on `rate(systemone_gateway_shared_limiter_errors_total[5m]) > 0`.

**Merging stays per replica.** Only calls that reach the same replica can
share a batch. To maximise merging, send calls about the same state to the
same replica, with consistent hashing in the load balancer on a header that
identifies the state. The gateway ignores the header, so your services can
send whatever names the document or the case, as long as every service sends
the same value for the same state. For example, with `x-document-id`:

```yaml
# ingress-nginx, on the Ingress
metadata:
  annotations:
    nginx.ingress.kubernetes.io/upstream-hash-by: "$http_x_document_id"
---
# Istio
kind: DestinationRule
spec:
  trafficPolicy:
    loadBalancer:
      consistentHash:
        httpHeaderName: x-document-id
```

Hashing keeps a state on one replica while the set of replicas is stable.
When replicas come and go, some states move, and a state that is asked about
very often loads one replica more than the others.

### Adaptive rate and replicas

With [`adaptive_rate`](configuration.md#adaptive-rate), every replica adapts its
own rate. Redis keeps times, not rates: the script takes the rate on every
booking, so a replica lowers its rate by booking with a longer spacing, and
nothing is stored or agreed.

- **The pause is shared, the 429 is not.** After a 429, the pause is in Redis
  and every replica waits it out. But only the replica that received the 429
  counts it: the others see a pause and hold, and do not lower their rate. A
  replica lowers its rate only when a 429 reaches it.
- **The cluster's pace is a blend.** Each booking moves the shared arrival time
  by the booking replica's own spacing, so the cluster runs at a rate between
  the lowest and the highest of the replicas' rates, weighted by how many
  bookings each makes. When one replica out of three has lowered its rate, the
  cluster slows by less than that.
- **It converges, not at once.** If the provider keeps refusing, the replicas
  that still pace at the ceiling take a 429 of their own within moments of the
  pause ending, lower their rate, and the blend comes down. The price of not
  coordinating is a few extra 429s, each retried by the gateway. Replicas also
  recover on their own clocks, so they climb back out of step and meet at the
  ceiling. That is acceptable when the replicas carry similar traffic, which is
  what a load balancer gives you. It is not a guarantee that the cluster
  is under the provider's new limit after the first episode.
- **Redis down.** The local share, which holds the limit divided by
  `expected_replicas`, follows the adapted rate divided the same way.
- **Read the gauge per replica.** `requests_per_minute_limit` is each
  replica's own rate, so it differs between replicas exactly when they have
  not converged: `min by (backend) (...)` is where the cluster is heading.

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
| info | `exporting traces` | Startup, only when [tracing](#traces) is on: the collector URL, where the setting came from, the service name and the sample ratio. |
| info | `gateway listening` | Startup, with the addresses and the number of services and backends. |
| info | `finished processing request` | Every call on the public port, with `method`, `path`, `request_id`, `service`, `status` and `latency`. `service` is missing when the call was refused before authentication. |
| warn | `retrying the upstream call` | A retry, with `backend`, `attempt`, `delay_ms` and `failure`. |
| warn | `upstream call failed` | A call failed after its retries. |
| warn | `the backend rejected a merged call; replaying each call on its own` | A 400 or 422 on a merged call. |
| warn | `circuit breaker opened: the backend failed every call lately` | A backend failed `circuit_breaker_failures` calls in a row. Its calls go to its fallbacks, or get a 503, for `cooldown_ms`. |
| warn | `circuit breaker opened again: the trial call failed` | The trial call after a cool-down failed. |
| info | `circuit breaker closed: the trial call was answered` | The backend answered the trial call. |
| info | `lowered the request rate after a 429; it rises again while the backend stays quiet` | [Adaptive rate](configuration.md#adaptive-rate): a 429 started an episode. With `backend`, `from_rpm`, `to_rpm` and `min_rpm`. One line per decrease, not per 429. |
| info | `the request rate is back at the configured requests_per_minute` | The adaptive rate has recovered to its ceiling. Each recovery step on the way is logged only at debug (`raised the request rate`). |
| warn | `the shared rate limiter's Redis does not answer; every replica paces with its own share of the limits until it does` | The first booking of an outage that could not reach Redis, with the `reason`. Logged once per outage, not per call. Only with `[cluster]`. |
| info | `the shared rate limiter's Redis answers again; pacing is shared again` | The end of that outage. |
| error | `the backend refused the gateway's API key` | A backend answered 401. Fix the key: every call to that backend fails with a 502 until you do. |
| error | `the answer could not be split` | A backend's response was unreadable or had no answer for a question. |
| debug | `upstream call answered` | One per upstream call, with the callers and questions it carried. |

The `x-request-id` of a response is the `request_id` of its log line.

When [tracing](#traces) is on and the collector cannot be reached, the exporter
logs its own lines: a `warn` saying that the OTLP export exhausted its retries,
and an `error` from `opentelemetry_sdk` with the cause. Calls are not affected.

## Metrics

`GET /metrics` on the admin port serves Prometheus metrics in OpenMetrics text.
Every name starts with `systemone_gateway_`.

### Per call (service side)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `calls_total` | counter | `service`, `status` | Calls received, by response status. A call refused before authentication has `service="-"`. A body refused for its size (413) is not counted. |
| `call_duration_seconds` | histogram | `service` | Time from receiving a call to answering it, queueing included. |
| `questions_total` | counter | `service` | Questions asked. Counted once a call has been accepted for batching or answered from the answer cache: calls refused earlier (validation, 403, service limits, a booked-out backend) add nothing. Cached questions count like any other. |
| `input_tokens_total` | counter | `service`, `backend` | Input tokens charged to each service. Merged calls are split between their callers, and the shares add up to the backend's bill. |

### Per upstream call (backend side)

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `upstream_calls_total` | counter | `backend`, `status` | HTTP calls to a backend, by final status after retries. `status="0"` means the backend could not be reached. A chat backend makes one call per question. |
| `upstream_duration_seconds` | histogram | `backend` | Time spent in a backend's HTTP calls, retries included. |
| `upstream_retries_total` | counter | `backend` | Attempts retried after a 429, 500, 502, 503, 504, 529 or network error. |

### Request rate

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `requests_per_minute_limit` | gauge | `backend` | The requests per minute the gateway paces the backend at now. Exported for every backend from the start: it is `requests_per_minute` and never moves unless `adaptive_rate` is on, and then it follows the adapted rate between the floor and `requests_per_minute`. Per replica. |
| `rate_decreases_total` | counter | `backend` | Times the adaptive rate lowered the rate: one per episode of 429s, not one per 429. Appears after the first decrease. |

### Circuit breaker and fallback

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `circuit_state` | gauge | `backend` | The backend's circuit breaker: `0` closed, `1` half-open (one trial call allowed), `2` open (calls turned away). Every backend is exported from the start, at `0` when the breaker is off. An open breaker turns half-open when the first call arrives after the cool-down, so the gauge stays at `2` until then. |
| `fallback_calls_total` | counter | `from`, `to` | Calls served by a fallback backend (`to`) instead of the backend their model routes to (`from`). Counted when the gateway picks the fallback, whether its answer is a success or not. |

### Merging

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `batch_callers` | histogram | `backend` | Service calls answered by one batch. Mostly 1 means little merging. |
| `batch_questions` | histogram | `backend` | Distinct questions in one batch. |
| `queue_wait_seconds` | histogram | `backend` | Time from a batch's first call arriving to the batch going upstream. The cost of merging and pacing in latency. |
| `deduplicated_questions_total` | counter | `backend` | Questions left out because an identical one was already in the same upstream call. |
| `estimated_tokens_saved_total` | counter | `backend` | Estimated input tokens not billed thanks to merging. System One backends only, because a chat backend repeats the state for every question. It is an estimate. |
| `isolated_replays_total` | counter | `backend` | Calls replayed alone after the merged call they were in was rejected. |

These carry a `backend` label because backends do not merge alike: a chat
backend repeats the state for every question, and each backend has its own
limits, so one average over all of them hides which one merges and which one
queues. Sum over `backend` for the gateway as a whole.

### Answer cache

`cache_entries` is always exported, at 0 while the cache is off. The two
counters appear once the cache has looked a question up.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `cache_hits_total` | counter | `backend` | Questions answered from the cache. Counted per question, so a call with 3 cached questions adds 3. |
| `cache_misses_total` | counter | `backend` | Questions looked up and not found, or found expired. Counted per question. A call sent with `cache-control: no-cache` looks nothing up, so it counts as neither. |
| `cache_entries` | gauge | | Answers held now. Expired ones are included until a lookup or a write drops them, so it can sit above the live count on a quiet gateway; it never exceeds `max_entries`. |

A call answered from the cache is still a call: it shows up in `calls_total`,
`call_duration_seconds` and `questions_total` of its service. It adds nothing to
`input_tokens_total`, `upstream_calls_total` or the merging metrics, because
nothing went upstream.

### Shared limiter

| Metric | Type | Meaning |
| --- | --- | --- |
| `shared_limiter_errors_total` | counter | Bookings on the shared rate limiter that could not reach Redis within `redis_timeout_ms`, and were paced on this replica's own share instead. Stays at 0 without `[cluster]`. While it grows, the limits are not shared. After a failure, Redis is left alone for a second, so a long outage counts about one error a second per replica, not one per call. |

### Useful queries

```promql
# Share of calls answered with 429, per service
sum by (service) (rate(systemone_gateway_calls_total{status="429"}[5m]))
  / sum by (service) (rate(systemone_gateway_calls_total[5m]))

# Backend 429s and retries
sum by (backend) (rate(systemone_gateway_upstream_calls_total{status="429"}[5m]))
sum by (backend) (rate(systemone_gateway_upstream_retries_total[5m]))

# How many calls share one upstream call, per backend
sum by (backend) (rate(systemone_gateway_batch_callers_sum[5m]))
  / sum by (backend) (rate(systemone_gateway_batch_callers_count[5m]))

# 95th percentile of the time a batch waits before going upstream, per backend
histogram_quantile(0.95, sum by (backend, le) (rate(systemone_gateway_queue_wait_seconds_bucket[5m])))

# The rate each backend is paced at now, lowest across replicas
min by (backend) (systemone_gateway_requests_per_minute_limit)

# Episodes of 429s that lowered a rate, per backend
sum by (backend) (increase(systemone_gateway_rate_decreases_total[1h]))

# Backends with an open breaker
max by (backend) (systemone_gateway_circuit_state) == 2

# Calls answered by a fallback, per pair of backends
sum by (from, to) (rate(systemone_gateway_fallback_calls_total[5m]))

# Input tokens per service
sum by (service, backend) (rate(systemone_gateway_input_tokens_total[1h]))

# Share of questions answered from the cache, per backend
sum by (backend) (rate(systemone_gateway_cache_hits_total[5m]))
  / (sum by (backend) (rate(systemone_gateway_cache_hits_total[5m]))
     + sum by (backend) (rate(systemone_gateway_cache_misses_total[5m])))
```

## Traces

Logs and metrics tell you how the gateway behaves. A trace tells one caller
what happened to its own call, including what it shared with others inside a
merged batch. The gateway exports traces with OpenTelemetry, and only when you
ask for it.

### Turning it on

Point the gateway at an OTLP/HTTP collector, in the file or in the environment
(the [configuration reference](configuration.md#tracing) lists the keys and
which one wins):

```toml
[tracing]
otlp_endpoint = "http://otel-collector:4318"   # the gateway posts to /v1/traces
# service_name = "systemone-gateway"
# sample_ratio = 1.0
```

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 systemone-gateway serve
```

`check-config` prints the endpoint the gateway would use, and `serve` logs an
`exporting traces` line at startup. With no endpoint, no exporter exists and the
gateway neither reads nor sends a `traceparent`: a run without tracing behaves
as it did before tracing existed.

The exporter speaks OTLP over HTTP with protobuf, not gRPC, so use the
collector's port 4318. Headers for a hosted collector go in
`OTEL_EXPORTER_OTLP_HEADERS`.

### What a trace shows

```
POST /v1/systemone            the call: from receiving it to answering it
  └─ systemone.batch          the upstream call that carried it
       ├─ upstream.attempt    one per HTTP attempt to the backend
       └─ upstream.attempt    the retry, if there was one
```

`GET /v1/models` makes a `GET /v1/models` span with its `upstream.attempt`
spans directly under it, because a model list is never merged.

A batch serves several callers, and a span has one parent. The gateway makes the
batch span a child of the call that opened the batch, and links the other calls
to it:

- A call that is not merged, which is the usual case, has its whole story in its
  own trace: the call, the batch and the attempts. The `traceparent` sent to the
  backend carries that trace's id, so a backend that traces continues it.
- When calls are merged, the batch and its attempts sit in the trace of the call
  that opened the batch. Every other call has a span link to the batch span, and
  the batch span has a link back to each of them. From any caller's span, you
  reach the batch, its attempts and the other callers in one click; in
  `gateway.batch_callers` you can also read how many calls shared it.
- A caller that joined a batch does not see the attempts in its own trace, only
  the link. That is the price of one upstream call serving everyone: it cannot
  also be a child of each of them.

The time between the start of the batch span and the first attempt is the wait
for the merge window, a request slot, a connection and the token budget. It is
also in `gateway.queue_wait_ms`, the same number the `queue_wait_seconds`
histogram observes. A call that is replayed alone after a merged call was
rejected gets a new batch span, a child of its own call, with
`gateway.batch.replay` set.

| Span | Kind | Attributes |
| --- | --- | --- |
| `POST /v1/systemone`, `GET /v1/models` | server | `http.request.method`, `http.route`, `http.response.status_code`, `gateway.request_id`, `gateway.service`; for a System One call also `gateway.backend`, `gateway.model` and `gateway.batch_callers`. Status `Error` for a 5xx. |
| `systemone.batch` | internal | `gateway.backend`, `gateway.model`, `gateway.batch.callers`, `gateway.batch.questions`, `gateway.batch.deduplicated`, `gateway.queue_wait_ms`. Status `Error` when the backend call failed. |
| `upstream.attempt` | client | `gateway.backend`, `http.request.method`, `gateway.attempt` (from 1), `http.response.status_code`, `error.type`, and `gateway.retry_delay_ms`, the wait before the retry that followed a failed attempt. Status `Error` for a failed attempt. |

`gateway.request_id` is the `x-request-id` of the response and of the log line,
which is how you go from a log line to its trace. A chat backend makes one
`upstream.attempt` per question, all under the batch span.

Spans never carry a state or a question, only the names above. A refused call is
traced too, with its status: a call refused before authentication (401) makes a
span of its own, because the gateway does not yet know who is asking.

### Sampling and propagation

- **Incoming.** The gateway reads W3C `traceparent` and `tracestate` from
  `POST /v1/systemone` and `GET /v1/models`, and makes its span a child of the
  caller's. A missing or malformed header starts a new trace. The header is read
  only after the caller's key has been accepted: a `traceparent` carries a
  sampled flag, and honouring it from anyone on the network would let anyone
  make the gateway record spans.
- **Sampling.** The decision is parent-based. A call that arrives with a
  `traceparent` is kept or dropped as its caller decided, and `sample_ratio`
  applies to the traces the gateway starts itself. A dropped trace costs no
  export, but the gateway still passes the `traceparent` on, with its sampled
  flag off, so the decision holds down the chain.
- **Outgoing.** Every upstream request, to a System One backend or a chat
  backend, carries `traceparent` and `tracestate` for its `upstream.attempt`
  span. TypeSafe and Hugging Face are free to ignore them.

### Shutdown, failures and cost

- Spans leave in batches from a thread of their own, every few seconds. On
  SIGTERM, once the servers have stopped, the gateway sends what is still
  queued, waiting at most 5 seconds. Count them in `terminationGracePeriodSeconds`.
- A collector that is down, slow or full never slows a call. The exporter drops
  spans it cannot send, and logs the failure (see [Logs](#logs)).
- With tracing on, each call costs a few spans. Lower `sample_ratio`, or have the
  callers sample, if that is too much.

## Tuning

Start from the limits your providers give you, then adjust with the metrics.

**Backend limits.** `requests_per_minute` and `tokens_per_second` should match
what the account may use, divided by the number of replicas, or undivided when
the replicas [share their limits through Redis](#sharing-the-limits-between-replicas). The defaults
(1200 and 250000) are starting points, and a provider can change its limits
without notice. If `upstream_calls_total{status="429"}` keeps growing, the
limit is set too high: lower that backend's `requests_per_minute`. By default
the gateway backs off on every 429 but does not learn a lower rate by itself.

**Adaptive rate.** Turn on `adaptive_rate` for a backend whose limit you do not
control, or that moves: the gateway then lowers its pacing when it is refused,
and raises it again while it is not. Set `requests_per_minute` to the most the
account may ever use, because it is the ceiling, and leave the rest at their
defaults to start.

- Watch `requests_per_minute_limit` and `rate_decreases_total`. A rate that
  falls and climbs back every few minutes is the gateway finding the provider's
  limit from above: your ceiling is higher than what the provider allows, and
  each fall costs a few 429s, which are retried. Lower `requests_per_minute` to
  that level to stop the probing, or raise `adaptive_recovery_ms` or lower
  `adaptive_increase_per_minute` to probe less often or less far.
- `adaptive_decrease` sets how hard a 429 is taken: `0.5` backs off fast, `0.9`
  gently. `adaptive_recovery_ms` also sets how long 429s count as one episode, so
  keep it longer than an upstream attempt takes.
- `adaptive_min_requests_per_minute` is the most useful guard. Set it to the
  lowest rate you would still want to serve at: below it, callers get 429s
  from the gateway (`max_queue_wait_ms` runs out) rather than a slower
  backend.
- It reacts to 429s only. A backend that slows down without refusing, or
  answers 5xx when it is overloaded, does not lower the rate; use the circuit
  breaker for that.
- With replicas, read [Adaptive rate and replicas](#adaptive-rate-and-replicas).

**Concurrency.** `max_concurrency` bounds the batches in flight to a backend.
The number you need is about the batches per second times the upstream latency
in seconds. Raise it if a backend's `queue_wait_seconds` is high while the request limit
is not reached. Keep it above that product, or batches queue for a connection
instead of a request slot.

**Merging.**

- Raising `window_ms` gives calls more time to meet, at the price of latency
  for every call. The window is a floor: under load, a batch waits for its
  request slot and keeps collecting calls anyway.
- `batch_callers` averaging 1 means services rarely ask about the same state
  at the same moment; the window is then pure latency, and `window_ms = 0` is
  reasonable. Read it per backend: a backend that few services use can sit at 1
  while another merges well, and `window_ms` applies to all of them.
- Keep `max_request_tokens` and `max_state_plus_question_tokens` below the
  provider's limits. If TypeSafe rejects large merged calls as too big, lower
  those two, or lower `bytes_per_token` so that the gateway counts more tokens
  per byte.

**Answer cache.** Turn it on when `cache_misses_total` shows services asking
the same questions about the same states more than once, a moment apart, which
merging cannot help with because the calls are not in flight together.

- `ttl_ms` is how stale an answer may be. A model behind an alias such as
  `jev-latest` can change under a cached answer, so keep it short enough that
  you accept that; callers that must have a fresh answer send
  `cache-control: no-cache`.
- If `cache_entries` sits at `max_entries`, the oldest answers are pushed out
  before they expire: raise `max_entries` if the hit ratio is lower than the
  repetition you expect. An answer takes a few hundred bytes.
- Give a backend `cache = false` when its answers should differ between calls.
- Each replica has its own cache, so the hit ratio falls as replicas are added,
  like merging.

**Circuit breaker.** The defaults (5 failures in a row, a 30 s cool-down) suit a
backend that is either up or down. The cool-down is how long a service waits
before the first trial call after an outage starts, and how long a dead backend
is left alone. Shorten it if the backend usually recovers within seconds, and
lengthen it if every trial call costs the callers an attempt timeout. Raise
`circuit_breaker_failures` for a backend with low traffic, where a few failures
in a row say little. Alert on `circuit_state == 2`: a service only notices the
outage through 503s, or not at all when a fallback answers.

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

- **Per process.** Merging, `max_concurrency` and each service's
  `max_concurrent` live in memory, and pacing does too unless you configure
  `[cluster]`. See [Kubernetes](#kubernetes) for running several replicas.
- **Shared pacing is best effort.** It needs Redis 5 or later (or Valkey)
  as a single endpoint without TLS, and costs a round trip per booking. While Redis is unreachable,
  each replica paces on its own share (`expected_replicas`), and the bookings it
  makes then are not charged to Redis afterwards.
- **Chat backends approximate System One.** See the
  [limits of chat backends](backends.md#limits-compared-to-a-system-one-backend).
- **Token counts are estimates.** Jev's tokenizer is not published, so the
  gateway counts `bytes_per_token` bytes per token. The estimate sizes merged
  calls, books the tokens-per-second budget (corrected with the real `usage`
  after each call) and weights the usage split, where only ratios matter.
- **Breakers are per process.** Each replica learns about an outage from its own
  failed calls, and recovers on its own trial call.
- **Fallback answers differ.** A chat backend standing in for a System One
  backend is not calibrated like Jev. See
  [Backends](backends.md#chat-backends-are-not-calibrated-like-jev).
- **Provider limits move.** TypeSafe's limits change without notice
  ([models](https://docs.typesafe.ai/models.md)). By default the gateway does
  not learn them: it backs off on every 429 and keeps pacing at
  `requests_per_minute`. With `adaptive_rate` it lowers its pacing after a 429
  and raises it again, but only that. It learns from 429s alone, so a provider
  that slows down without refusing does not move it, and a limit that is raised
  is never discovered, because the rate does not go above `requests_per_minute`.
  The adapted rate lives in memory and starts again from the ceiling after a restart.
  Replicas adapt on their own 429s and converge over a few episodes (see
  [Adaptive rate and replicas](#adaptive-rate-and-replicas)). `tokens_per_second`
  does not adapt.
- **The answer cache is opt-in and per process.** With `[cache]` off, which is
  the default, two identical calls a minute apart cost two upstream requests,
  and only calls in the gateway at the same time share one. With it on, answers
  are served for up to `ttl_ms` (so a model alias that moves is seen late),
  a restart or another replica starts with an empty cache, and two identical
  calls in flight together both go upstream unless they merge into one batch.
  A call that timed out does not fill the cache, even if the backend answered it
  afterwards.
- **No TLS, and only a light guard on the admin port.** Put the gateway behind
  a proxy or mesh that terminates TLS, and keep the admin port private. The
  optional `server.admin_token_env` protects `/metrics` and nothing else: the
  probes stay open, and without TLS the token travels in clear text.
