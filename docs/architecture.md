# Architecture

This page follows one call through the gateway, from the HTTP request to the
answer, and explains why each step exists. For the settings mentioned here, see
the [configuration reference](configuration.md).

## Overview

```
 ocr-service ─┐
 fraud-check ─┼─> [1 receive] ─> [2 authenticate] ─> [3 validate] ─> [4 route]
 triage ─┘                                                          │
                                                                         v
                                   [5 admit: per-service rate and concurrency]
                                                                         │
                                                                         v
                 [5b answer cache, if on: a call fully cached is answered here]
                                                                         │
        one queue per backend                                            v
   ┌─────────────────────────────────────────────────────────────────────────┐
   │ [6 batch: calls with the same model, state and extra fields join one    │
   │    batch that books one request slot]                                   │
   │                               │                                         │
   │                               v                                         │
   │ [7 pace: wait for the request slot, a free connection, the token budget]│
   │                               │                                         │
   │                               v                                         │
   │ [8 send: merge questions, call the backend, retry, back off]            │
   └───────────────────────────────┬─────────────────────────────────────────┘
                                   │                 ┌─ systemone: POST /v1/systemone
                                   ├─────────────────┤
                                   │                 └─ chat: POST /chat/completions, one per question
                                   v
   [9 split: each caller gets its own answers under its own question ids,
             and its share of the token usage]
```

Steps 1 to 5b happen per call, in the caller's request. Steps 6 to 9 happen per
backend: every backend has its own queue, request limit, token budget and
connection limit, so a slow provider never holds up another.

Two things can move a call to another backend. Step 4 skips a backend whose
circuit breaker is open, and step 8 reports every upstream call to that breaker.
When the backend that serves the model is unavailable, the call goes to its
fallback and runs steps 6 to 9 there. See
[Fallback](#fallback-and-the-circuit-breaker).

## 1. Receive

The public server accepts the request, gives it an `x-request-id` (or keeps
the one it came with) and opens a log span. Bodies larger than
`server.max_body_bytes` are refused here with a 413, before anything else. The
gateway never logs a body, because states and questions can carry personal or
financial data.

## 2. Authenticate

The service is identified by its bearer key. The gateway hashes the key with
SHA-256 and looks the hash up among the `key_sha256` values of the `[[service]]`
blocks, so it never holds a usable key. A missing or unknown key gets a 401.

## 3. Validate

The body is parsed just enough to merge and split it. Everything the model
reads stays as raw JSON, so nothing is reinterpreted on the way through.

- Not a JSON object: 400.
- A field that breaks one of [TypeSafe's documented rules](https://docs.typesafe.ai/api.md)
  (see the [API reference](api.md#request)): 422, with `param` naming the field.

Validation is there because of merging. One malformed question would make the
backend reject a merged call for every service in it. Catching the documented
mistakes here keeps that failure with the caller that made it.

While validating, the gateway also computes:

- a **batch key**, a hash of the model, the top-level fields other than
  `state`, `model` and `questions` (sorted by name), and the minified state;
- a **question key** for each question, a hash of its fields (sorted by name,
  minified);
- a token estimate for the state and for each question (see
  [Token estimates](#token-estimates)).

Minifying ignores whitespace, so the same state written with different
whitespace gets the same key. Key order inside the state is kept, because the
model reads the state as text and two orderings may not give the same answers:
the same keys in a different order do not merge.

## 4. Route

The service's `allowed_models` is checked first (403), then the model picks a
backend by the most specific match in `models` (422 when none matches). A chat
backend also refuses here what it cannot express, such as a Choice with more
than 26 options. See [Backends](backends.md#how-a-model-picks-a-backend).

The routed backend's circuit breaker is consulted last, when the call is about
to be queued. If the breaker is open, the call goes to the first fallback that
accepts it, or is answered at once with a 503 when none does. See
[Fallback and the circuit breaker](#fallback-and-the-circuit-breaker).

## 5. Admit

If the service has limits of its own, they apply now: `requests_per_minute` and
`max_concurrent`. A service over either gets a 429 before it can take capacity
from the others. The in-flight slot is held until the answer is sent.

### Answer cache

When `[cache]` is enabled and the routed backend has not opted out, the gateway
looks each question of the call up in the answer cache before the call can join
a batch. The key is the batch key (model, state, extra fields) plus the question
key, so an answer is served only for exactly the same call.

- **Every question is cached and fresh.** The call is answered on the spot. It
  never reaches the batcher and books nothing: no request slot, no connection,
  no tokens. The response is rebuilt from the cached answers under the caller's
  own question ids, and its `usage` is zero, because the backend billed nothing.
- **Some are cached.** Only the missing questions go upstream, through steps 6
  to 9 as usual, and the cached answers are merged into the response under the
  caller's ids. `usage` is what the smaller upstream call cost.
- **None is cached**, or the caller sent `cache-control: no-cache`. The call
  goes upstream as if the cache were off. `no-cache` skips the read only: the
  answers that come back are still kept.

A hit comes after authentication, validation, routing, `allowed_models` and the
service's own limits, so the cache never answers a call the service could not
have made. The call still spends one request of the service's
`requests_per_minute` and holds its `max_concurrent` slot while it lasts: the
service's limits count calls, and only the backend's limits count upstream
requests.

Once an upstream answer comes back in the caller's request, its answers are
stored, each under its own question key. Only a successful answer is stored:
an error from the backend, a 429 from the gateway or a body that cannot be read
leaves the cache as it was. The cache is filled by the request handler, so a
call that already gave up on a 504 does not fill it.

The store is a bounded map in write order. Every entry lives for the same
`ttl_ms`, so the oldest entry is also the one that expires first: the cache
drops the expired entries from the front and, when it is full, the oldest one.
Answers are kept as the raw JSON the backend sent, together with its response
fields other than `answers` and `usage` (such as `model`), which is what lets a
response be rebuilt without a call. Nothing the cache holds is ever logged.

## 6. Batch

The batcher groups calls that share a state. A batch opens with the first call
for a given batch key and **books one upstream request slot**. Every call with
the same key that arrives while the batch is open joins it for free.

A batch stays open for `coalescing.window_ms` (10 ms by default), and then for
as long as it waits for its request slot and for a free connection. So merging
does the most when it matters: when the backend's limit is saturated and calls
queue up anyway. `window_ms = 0` turns merging off.

```
time ──────────────────────────────────────────────────────────────>
 ocr-service  ──●  call A arrives, opens the batch, books a slot
 fraud-check  ────●  call B arrives 4 ms later, same state: joins
 ocr-service  ──────────●  call C, other state: its own batch
                  │
                  └── window ends and the slot is reached: A and B go out as one call
```

A call may join an open batch only if:

- the batch is not sealed yet (a batch is sealed when it gets its connection and is about to be sent);
- the batch would not leave later than the call can wait;
- the merged call stays inside `max_questions`, `max_request_tokens` and
  `max_state_plus_question_tokens`. A question identical to one already in the
  batch costs nothing, because it is sent once.

When a batch is full, the next call opens a new batch for the same key. The
full one still goes out on its own schedule.

### Why merging is safe

TypeSafe evaluates every question of a request independently against the same
state, and never shows question ids to the model
([primitives](https://docs.typesafe.ai/primitives.md)). A question gets the same
answer whether it travels alone or next to another service's, so the gateway
can rename ids freely.

### Queue deadlines

Each call carries a latest send time: its arrival plus `max_queue_wait_ms`, and
never earlier than the end of the merge window. The merge window is the
gateway's own delay, so it never counts against the caller's patience.

- If a new batch cannot get a request slot before that time, the call is
  refused at once with a 429 and a `retry-after` that says when the slot frees.
- If a call is still waiting for a connection at that time, it gets a 429 then.
- The 429 is better than a late send: a late send burns tokens on an answer
  the caller may no longer wait for. The TypeSafe SDKs retry 429s and honour
  `retry-after`, so the service backs off without code of its own.

The call as a whole is also bound by `server.request_timeout_ms`; past it the
caller gets a 504.

## 7. Pace

Three limits are enforced with one mechanism, the generic cell rate algorithm
(GCRA): the backend's requests per minute, the backend's estimated tokens per
second, and each service's requests per minute. Booking never sleeps: it
returns the instant a caller may start, and the caller decides whether to wait,
to join a batch that already holds a slot, or to answer 429 with that instant
as `retry-after`.

Before a batch leaves, it:

1. waits for its request slot;
2. waits for a free connection (`max_concurrency`). Connections are handed out
   in the order batches became ready, and the batch stays open to new calls
   meanwhile;
3. books its estimated tokens against `tokens_per_second`. If the budget is
   spent, callers that cannot wait until it is available get a 429 and the
   others go ahead without them.

Callers that timed out or hung up before this point are dropped from the batch,
so they are not paid for. If nobody is left, the batch gives its request slot
back.

## 8. Send

### Merging

With one caller, the request goes upstream under the caller's own question
ids, and the answer comes back unchanged. With several callers:

- the questions are deduplicated by question key, then renamed `q0`, `q1`, and
  so on;
- the state, the model and the extra top-level fields are taken from the first
  call, since every call in the batch has the same ones.

A **System One backend** receives that merged request. A **chat backend**
receives one chat request per distinct question instead; see
[Backends](backends.md#protocol--chat).

### Retries and backoff

A failed attempt is retried with exponential backoff (`backoff_initial_ms`,
doubling up to `backoff_max_ms`, less up to 25% of jitter) on 429, 500, 502,
503, 504, 529 and network errors, up to `max_retries` times. A `retry-after-ms`
or `retry-after` header from the backend replaces the computed delay. Every
retry takes a request slot like any other call, because it counts against the
account's limit, and stays inside the callers' deadline.

A 429 from the backend pauses the whole backend: nothing starts until the pause
ends. Without this, every batch would find the limit on its own. When the
pause ends, batches leave one slot apart instead of together.

Errors that are not retried, such as a 400 or a 403, go back to the caller as
the backend sent them. A 401 from the backend is turned into a 502, because the
caller's key was fine and the gateway's own key is not.

### Fallback and the circuit breaker

The final outcome of each upstream call, after its retries, is reported to the
backend's circuit breaker. `circuit_breaker_failures` failures in a row (network
errors, 5xx, 529, or a 401 for the gateway's key) open it, and an open breaker
turns calls away for `circuit_breaker_cooldown_ms`. Then one trial call goes
through: an answer closes the breaker, a failure opens it again. A 429 or a
client error counts as an answer, since the backend is up. The count is per
upstream call: a merged call that fails is one failure, however many callers it
carried.

For a call routed to a backend that has `fallback` set, the gateway builds a
list of candidates: the backend, then its fallbacks in order, each followed by
its own. It picks the first whose breaker lets the call through, and that can
express the question. Then:

- If that backend answers, or fails in a way another backend could not fix
  (a 400, a 429), the caller gets the outcome.
- If it fails as unavailable (network error, timeout, 5xx, refused key) and the
  call has at least one second left before its deadline, the call is read again
  from the request body and sent to the next candidate, with a fresh queue
  deadline there but the same overall deadline. The caller waits once, for the
  answer of whichever backend ends up serving it.
- If no candidate is left, the caller gets the last failure, or, when every
  breaker turned the call away, a 503 with `retry-after` set to the shortest
  cool-down left.

Fallback is decided per caller, after the batch returns, so one caller's
retry does not hold up the others of a merged call. The fallback backend batches
the call with its own callers like any other.

### When a merged call is rejected

If the backend answers 400 or 422 to a merged call, a question slipped past
local validation and sank the whole call. The gateway then replays each caller
alone. The replays cost a few extra requests, and the error reaches only the
caller whose question caused it. The `isolated_replays_total` metric counts
them.

## 9. Split

For a merged call, each caller gets:

- the answers to its own questions, under its own question ids. A question that
  two services asked is answered once and appears in both responses;
- the other fields of the backend's response as they were sent;
- its share of `usage`.

### Splitting the answer and the usage

The merged call's integer `usage` fields are split between callers so that the
shares add up to exactly what the backend billed, using the largest-remainder
method:

- **Input tokens.** The state is shared evenly between the callers. Each
  question is charged to whoever asked it, evenly when several did.
- **Output tokens.** Split by question, in the same way.

Fields that are not integers are copied. The per-service counter
`input_tokens_total` uses these shares, so it reconciles with the backend's
bill.

If the response cannot be read, or lacks the answer for a question, the caller
gets a 502.

### Response headers

The response adds `x-systemone-gateway-backend`, `x-systemone-gateway-batch-callers`
and `x-typesafe-request-id`, and `x-systemone-gateway-cache` when the answer
cache is on. See [API](api.md#response-headers).

## Token estimates

TypeSafe does not publish Jev's tokenizer, so the gateway estimates tokens
from the byte length of the minified JSON: `coalescing.bytes_per_token` bytes
(3 by default) per token, plus a fixed overhead per question. The estimate:

- decides whether a merged call fits the backend's context budget;
- books `tokens_per_second`, then corrects the booking with the real `usage`
  after each answer;
- weights the split of `usage` between callers. Only ratios matter there, so
  the constant cancels out.

The default leans high, because a call that turns out too large is rejected.

## Shutdown

On SIGTERM or SIGINT, the gateway marks itself not ready (`/readyz` answers
503 to any request that still reaches it), both servers stop accepting new
connections, and calls in flight are allowed to finish. The process exits when
they are done.
