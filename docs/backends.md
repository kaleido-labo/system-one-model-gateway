# Backends

A backend is one model provider behind the gateway, with its own key, limits,
queue and protocol. Services pick a provider by naming a model; the operator
can move a model from one provider to another in the configuration alone. The
answer comes back in TypeSafe's format either way.

- [How a model picks a backend](#how-a-model-picks-a-backend)
- [`protocol = "systemone"`](#protocol--systemone)
- [`protocol = "chat"`](#protocol--chat)
- [Choosing between them](#choosing-between-them)
- [Fallback and the circuit breaker](#fallback-and-the-circuit-breaker)

## How a model picks a backend

Each `[[backend]]` block lists the model names it serves in `models`: an exact
name, a prefix ending in `*`, or `*` alone for everything. A call goes to the
backend with the most specific match: an exact name beats any prefix, and a
longer prefix beats a shorter one. The order of the blocks does not matter.

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

| Requested `model` | Served by |
| --- | --- |
| `jev-latest` | `typesafe`, through the `jev-*` prefix |
| `Qwen/Qwen2.5-7B-Instruct` | `huggingface` |
| `gpt-4` | nobody: 422 `validation_error` |

With this file, a service moves a question from Jev to Qwen by changing
`"model": "jev-latest"` to `"model": "Qwen/Qwen2.5-7B-Instruct"`. The
`x-systemone-gateway-backend` response header says which backend answered.

Rules worth knowing:

- The same `models` entry cannot appear in two backends, so a name always
  leads to one backend. `["jev-local"]` in one backend and `["jev-*"]` in
  another is fine: the exact name wins for that model.
- A `[[service]]` can be limited to some models with `allowed_models`. The
  check happens before routing, and a refused model gets a 403.
- A configuration with no `[[backend]]` block sends every model to TypeSafe,
  with the key in `TYPESAFE_API_KEY`.
- Each backend has its own request limit, token budget, connection limit and
  merge queue. Two backends never slow each other down.

## `protocol = "systemone"`

This is the default. The gateway forwards calls to `POST <base_url>/v1/systemone`,
merged calls included, with the backend's key as a bearer token. It fits
TypeSafe and any server that implements the same endpoint.

- `base_url` defaults to `https://api.typesafe.ai`. Point it at another server
  to use one, and leave out `api_key_env` if that server takes no key.
- Merging is fully effective here: calls that share a state go upstream once,
  and the state is billed once. TypeSafe charges per input token and does not
  charge for output tokens ([models](https://docs.typesafe.ai/models.md)), and
  the state is often most of the input.
- Top-level request fields that the gateway does not know are forwarded.
- `GET /v1/models` of the gateway lists what the backend's own `GET /v1/models`
  returns.

## `protocol = "chat"`

For a generative model behind an OpenAI-compatible chat completions API: the
[Hugging Face router](https://huggingface.co/docs/inference-providers/tasks/chat-completion),
a Hugging Face Inference Endpoint, TGI or vLLM. `base_url` is the URL that
`chat/completions` hangs off, such as `https://router.huggingface.co/v1`; the
gateway calls `<base_url>/chat/completions`.

### How a question is answered

Each question becomes one chat request that asks for a single token, the label
of an answer, together with its `logprobs`. The request sets
`max_tokens = 1`, `logprobs = true`, `top_logprobs` (from the backend's
configuration) and `stream = false`.

The prompt has a fixed system message that tells the model to treat the state
as data and to reply with a single label. The user message starts with the
state, then the question, then the labels:

```
State:
<the state>

Question: <instructions>
Options:
A. billing: Billing and payments
B. shipping
C. returns: Returns and exchanges

Reply with the letter of one option only.
```

The state comes first in every prompt, so a server with prefix caching reads
the state once for all questions about it.

The model picks from these labels, and the gateway builds the answer from the
probability of each label:

| Question | Labels | Answer built from the label probabilities |
| --- | --- | --- |
| `noul` | `Yes`, `No` | `noul` = P(Yes) |
| `choice` | `A`, `B`, `C`... one letter per option, in the order of `criteria` | `choice` (the most likely option, the first on a tie), `probabilities` per option, `confidence` |
| `score` | `0`, `1`... one digit per level | `score` = sum of level x probability, `legend` (the level descriptions as sent), `probabilities`, `confidence` |

For a `noul`, the `true` and `false` criteria become "Answer Yes if: ..." and
"Answer No if: ..." lines when present.

To get the probabilities, the gateway looks at the candidates listed for the
first generated token:

1. Tokens that spell the same label add up: `Yes`, ` Yes` and `yes` all count
   for Yes. Matching ignores case, surrounding spaces, the word markers some
   tokenizers add (`▁`, `Ġ`) and a trailing `.`, `)` or `:`, as in `A.`.
2. Tokens that are no label are dropped.
3. What is left is renormalised so that the probabilities add up to 1.

`confidence` follows the [formula TypeSafe documents](https://docs.typesafe.ai/confidence.md):
`(n x max - 1) / (n - 1)` for `n` labels and `max` the highest probability,
clamped between 0 and 1.

The answers come from the model's token probabilities and never from a
confidence that the model writes out, because verbalised confidence is badly
calibrated.

The response's `usage` adds up the `prompt_tokens` and `completion_tokens` of
the chat calls (and is left out if the backend reports none). Its `model` is
the model the first chat response reports, or the name sent upstream.

### Limits compared to a System One backend

- **Candidate tokens.** The Hugging Face router returns at most 5 candidates
  (`top_logprobs`). A Choice therefore spreads its probability over 5 options
  at most, and every other option gets 0. vLLM and TGI accept up to 20: raise
  `top_logprobs` there.
- **Options.** A Choice has at most 26 options, one letter each. More gets a
  422 before anything is sent. A Score keeps the System One range of 2 to 10
  levels.
- **Calibration.** The probabilities are the model's raw ones. Nobody
  calibrated them the way TypeSafe calibrates Jev, so measure them on your own
  data before you pick thresholds.
- **Cost of merging.** Every question is one request that repeats the state,
  and each one books its own slot in `requests_per_minute`. Merging still
  sends a question that two services share only once, but it saves no state
  tokens. A server with prefix caching reads the shared state once anyway.
- **Model type.** Use an instruct model that answers straight away. A
  reasoning model that starts with a thinking token has no label in its first
  token, and the call fails with a 502. For vLLM, `request_extras` can turn
  thinking off: `chat_template_kwargs = { enable_thinking = false }`.
- **Request fields.** Top-level request fields other than `state`, `model` and
  `questions` are not sent, because a chat API has nowhere to put them.
- **Error bodies.** When the chat API rejects a request, the caller gets its
  status and body as sent, in the provider's format and not TypeSafe's.
- **All or nothing.** The questions of one call are sent in parallel. If one
  fails, the whole call fails, and the other requests are cancelled.

### Options

| Key | Use |
| --- | --- |
| `upstream_model` | Sends one fixed model id upstream, whatever name the service used. Services ask for `my-judge`, say, and the gateway calls `org/fine-tuned-model`. List `my-judge` in `models` as an exact name. |
| `top_logprobs` | Candidates requested per answer, 1 to 20. Defaults to 5, the router's maximum. |
| `request_extras` | Extra fields for every chat request, such as `temperature`. The gateway's own fields (`model`, `messages`, `max_tokens`, `logprobs`, `top_logprobs`, `stream`) win. |

Example for a vLLM server that hosts a fine-tuned model behind an alias:

```toml
[[backend]]
name = "local-vllm"
protocol = "chat"
base_url = "http://vllm.internal:8000/v1"
models = ["my-judge"]                      # no api_key_env: the server takes no key
upstream_model = "org/fine-tuned-model"
top_logprobs = 20
requests_per_minute = 600

[backend.request_extras]
chat_template_kwargs = { enable_thinking = false }
```

### Pacing

A chat batch takes one slot of `max_concurrency`, and each of its questions
books one request slot in `requests_per_minute`. The requests of a batch leave
one slot apart, not all at once. If the last slot falls beyond the call's
deadline, the caller gets a 429 instead.

## Choosing between them

| | System One backend | Chat backend |
| --- | --- | --- |
| Endpoint called | `POST /v1/systemone` | `POST /chat/completions` |
| Requests upstream per call | One per merged batch | One per distinct question |
| State billed | Once per batch | Once per question (less with prefix caching) |
| Probabilities | The provider's own, calibrated by the provider | The model's raw token probabilities |
| Choice options | Up to 255 | Up to 26, and at most `top_logprobs` visible |
| Extra top-level fields | Forwarded | Dropped |
| `GET /v1/models` | The provider's list, cached | The exact names in `models` |

## Fallback and the circuit breaker

A backend that is down makes every call to it wait through its retries and
then fail. Two opt-in settings keep the services going: a circuit breaker per
backend, which notices the outage, and a `fallback` list, which says where the
calls can go meanwhile.

### The circuit breaker

Every backend has one. It counts upstream calls, not callers: a merged call that
fails for ten services is one failed call.

- **Closed.** Calls go through. A call that fails after its retries adds one to
  a count, and an answer resets it to zero. A call fails when it ends in a
  network error or a timeout, a 5xx (529 included), or a 401, which means the
  backend refuses the gateway's own key. After `circuit_breaker_failures`
  failures in a row (5 by default), the breaker opens.
- **Open.** The gateway turns calls away without sending them, for
  `circuit_breaker_cooldown_ms` (30 s by default).
- **Half-open.** The first call that arrives after the cool-down is the trial.
  The other calls keep being turned away until the trial ends. If the backend
  answers it, the breaker closes. If the trial fails, the breaker opens again
  for another cool-down.

A 429 never trips the breaker. The backend answered, and the
[pause after a 429](architecture.md#retries-and-backoff) already holds the
traffic back. Nor do client errors (400, 403, 422): the call was at fault, not
the backend. A call that never started (its deadline passed first) or whose
answer could not be read says nothing either way.

`circuit_breaker_failures = 0` turns the breaker off for a backend.

### What a service sees

- **With a fallback**, the call goes to the next usable backend (see below), and
  the `x-systemone-gateway-backend` header names the backend that answered.
- **Without one**, or when every fallback is turned away too, the call fails at
  once with a `503` `unavailable_error`, with a `retry-after` equal to the time
  left of the cool-down. It is not queued, and nothing is sent upstream.

The breaker is per gateway process, like the pacing. With several replicas, each
finds out about an outage on its own.

### Fallback

```toml
[[backend]]
name = "typesafe"
api_key_env = "TYPESAFE_API_KEY"
models = ["jev-*"]
fallback = ["local-vllm"]

[[backend]]
name = "local-vllm"
protocol = "chat"
base_url = "http://vllm.internal:8000/v1"
models = ["my-judge"]
upstream_model = "org/fine-tuned-model"
```

A call for `jev-latest` goes to `typesafe`. If `typesafe` cannot take it, it goes
to `local-vllm`. The first of these sends it to a fallback:

1. The routed backend's breaker is open, or half-open with a trial under way.
   The call goes straight to the fallback, without trying the backend.
2. The call reached the routed backend and failed with an outage (a network
   error or timeout, a 5xx, a 401) after its retries, and at least one second
   is left before the call's deadline. The gateway then sends the call again to
   the fallback. That is safe because a failed call returned nothing to the
   caller, and a System One call changes nothing on the backend. A call that the
   backend refused for its own faults (400, 403, 422), or that was rate limited
   (429), is not sent again: another backend would not do better, and the caller
   should see the error.

Rules:

- **Order.** The fallbacks are tried in the order of the list. A fallback's own
  `fallback` list is followed right after it, and each backend is tried once.
  A fallback whose breaker is open is skipped. A fallback that cannot express
  the question, such as a Choice with more than 26 options for a chat backend,
  is skipped too.
- **Which model the fallback gets.** The request is passed on unchanged, so the
  fallback receives the model name the service asked for. A chat backend with
  `upstream_model` sends that id upstream whatever the name, so it can stand in
  for any model. A backend without one sends the requested name as it is, so it
  has to know it. The fallback does not have to list the model in its `models`:
  that list only decides which backend a model routes to, and a model still
  routes to one backend only.
- **Service limits.** A service's `allowed_models` is checked once, on the model
  it asked for, before routing. A fallback is never a way around it.
- **Capacity.** The call takes a slot from the fallback's own limits, and waits
  for its queue like any call. A 429 from the fallback is passed on.
- **Validation.** `check-config` refuses a fallback that names a backend that
  does not exist, names itself, lists a name twice, or leads back to a backend
  already in the chain.

The metric `fallback_calls_total{from, to}` counts the calls a fallback served,
where `from` is the backend the model routes to, and `circuit_state{backend}`
shows each breaker. See [Operations](operations.md#metrics).

### Chat backends are not calibrated like Jev

A chat backend as a fallback for a System One backend answers, but not the same
way. Its probabilities are the model's raw token probabilities, not the ones
TypeSafe calibrated for Jev, and its `confidence` and `noul` values do not mean
the same thing (see the
[limits of chat backends](#limits-compared-to-a-system-one-backend)). A service
that applies thresholds tuned on Jev can make different decisions on a fallback
answer. Measure a fallback on your own data before you rely on it, and watch
`fallback_calls_total` so that you know when it happens. The `x-systemone-gateway-backend`
header tells a service which backend answered, for a service that wants to
treat those answers differently.
