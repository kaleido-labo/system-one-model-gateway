# HTTP API

The gateway serves TypeSafe's System One API, so a service that already talks
to TypeSafe only changes its base URL and key. The official reference for the
request and response bodies is [TypeSafe's API page](https://docs.typesafe.ai/api.md);
this page covers what the gateway adds, checks and answers itself.

There are two ports:

| Port (default) | Endpoints | Who calls it |
| --- | --- | --- |
| Public, `server.listen` (8080) | `POST /v1/systemone`, `GET /v1/models` | Your services |
| Admin, `server.admin_listen` (9090) | `GET /healthz`, `GET /readyz`, `GET /metrics` | Probes and Prometheus |

## Authentication

Every public call carries the key the gateway issued to the service:

```
Authorization: Bearer s1gw_...
```

The scheme name is case-insensitive. The gateway hashes the key and looks the
hash up among the `key_sha256` values of the configuration, so a missing or
unknown key gets a 401 and never reaches a backend. The admin port has its own,
optional token, see [Admin endpoints](#admin-endpoints).

The TypeSafe Python SDK reads `TYPESAFE_BASE_URL` and `TYPESAFE_API_KEY`, so
pointing a service at the gateway is a matter of setting both. For another SDK,
check how it takes a base URL.

## `POST /v1/systemone`

Asks one or more questions about a state.

```sh
curl -s http://localhost:8080/v1/systemone \
  -H "Authorization: Bearer $TYPESAFE_API_KEY" \
  -H 'content-type: application/json' \
  -d '{
    "state": "Order 1042 - refund request, 18.00 EUR",
    "model": "jev-latest",
    "questions": {
      "refund": {"type": "noul", "instructions": "Is this a refund request?"}
    }
  }'
```

### Request

| Field | Rule |
| --- | --- |
| `state` | Required. A string, an object or an array. |
| `model` | Required. A non-empty string. It picks the backend, see [Backends](backends.md#how-a-model-picks-a-backend). |
| `questions` | Required. A non-empty object that maps your question ids to questions. |
| other top-level fields | Forwarded untouched to a System One backend, so a field TypeSafe adds later keeps working. A chat backend drops them. Calls that carry different values for them are never merged. |

Each question is an object with these fields:

| Field | Rule |
| --- | --- |
| `type` | Required. `"noul"`, `"choice"` or `"score"`. |
| `instructions` | Required. A string, an object or an array. |
| `criteria` for `noul` | Optional. An object with optional `true` and `false` keys, each a string, object or array. |
| `criteria` for `choice` | Required. An object of 1 to 255 options. Each key is an option and each value is its description (a string, object or array) or `null`. A chat backend accepts at most 26. |
| `criteria` for `score` | Required. An array of 2 to 10 level descriptions, each a string, object or array. |

These are the rules TypeSafe documents. The gateway checks them itself before a
question can join a merged call, so a malformed question fails alone and with
the field at fault in `param`. It stops at the documented rules: anything
stricter would turn away requests that TypeSafe accepts. A merged call that
TypeSafe still rejects is replayed one caller at a time, see
[Architecture](architecture.md#when-a-merged-call-is-rejected).

### Response

A `200` carries the backend's body, cut down to the caller's own questions, under
the caller's own question ids:

```json
{
  "answers": {"refund": {"type": "noul", "noul": 0.72}},
  "model": "jev-1.13.0",
  "usage": {"input_tokens": 38, "output_tokens": 10}
}
```

The exact fields of each answer are the backend's. `usage` is this caller's
share of the merged call: integer fields are split so that the shares of all
callers add up to exactly what the backend billed (see
[Architecture](architecture.md#splitting-the-answer-and-the-usage)).

From a chat backend, answers are built by the gateway in TypeSafe's shape:

| Question | Answer fields |
| --- | --- |
| `noul` | `type`, `noul` |
| `choice` | `type`, `choice`, `probabilities`, `confidence` |
| `score` | `type`, `score`, `legend`, `probabilities`, `confidence` |

### Response headers

| Header | Value |
| --- | --- |
| `x-systemone-gateway-backend` | Name of the backend that handled the call. Set on every outcome of a batch, success or error. Not set on errors decided before the call joined a batch (400, 401, 403, 422, a 429 for the service's own limits or a booked-out backend), nor on the 504 for `request_timeout_ms`. |
| `x-systemone-gateway-batch-callers` | How many service calls shared the upstream call that answered this one. `1` means the call went alone. Only on a `200`. |
| `x-typesafe-request-id` | The backend's request id, for support requests. It is the same for every caller of a merged call. For a chat backend, it is the first request id that its chat calls reported, read from the upstream `x-typesafe-request-id` or `x-request-id` header. |
| `x-request-id` | The id of this call in the gateway's logs. The gateway uses the one in the request when there is one, and makes one otherwise. |
| `retry-after`, `retry-after-ms` | On every 429 the gateway makes: how long to wait, in whole seconds (rounded up) and in milliseconds. The TypeSafe SDKs read `retry-after-ms` first. |

## `GET /v1/models`

Lists the models of every backend, in the order of the `[[backend]]` blocks,
under a `models` key. It needs the same bearer key as `POST /v1/systemone`.

```json
{"models": [{"name": "jev-latest", "description": "..."}, {"name": "Qwen/Qwen2.5-7B-Instruct", "description": "..."}]}
```

- A System One backend contributes the entries of its own `GET /v1/models`,
  as sent, cached for `models_cache_ttl_ms`. The first call after the cache
  expires books a request slot like any other upstream call and can fail with
  the backend's errors.
- A chat backend contributes one entry per exact name in its `models`.
  Prefix patterns such as `Qwen/*` are not listed, because the gateway cannot
  list what a prefix covers.

Listing a model does not mean a service may use it: a service's
`allowed_models` still applies to `POST /v1/systemone`.

## Admin endpoints

| Endpoint | Answer |
| --- | --- |
| `GET /healthz` | `200 ok` while the process runs. |
| `GET /readyz` | `200 ready` once both listeners are up; `503 shutting down` once shutdown has started. The admin server stops accepting connections at the same time, so a new probe may get a refused connection instead. |
| `GET /metrics` | Prometheus metrics in OpenMetrics text, see [Operations](operations.md#metrics). Needs `Authorization: Bearer <token>` when `server.admin_token_env` is configured; a missing or wrong token gets a 401 in the [error shape](#errors) below. |

The admin port is open unless `server.admin_token_env` names an environment
variable that holds a token. The token protects `/metrics` only. `/healthz` and
`/readyz` stay open in every case, because Kubernetes probes cannot easily send
a header, and what they answer is nothing you need to hide. The token is
compared in constant time, and the scheme name is case-insensitive.

## Errors

Errors the gateway makes itself have one JSON shape, close to TypeSafe's:

```json
{"error": {"type": "validation_error", "message": "questions.size.criteria lists 1 levels, a score needs between 2 and 10", "param": "questions.size.criteria"}}
```

`param` appears only when one field is at fault. Status codes follow the
TypeSafe API, so the vendor SDKs raise the same exception types as they would
against TypeSafe, and retry the same statuses (429 and 5xx) while honouring
`retry-after`.

| Status | `type` | When |
| --- | --- | --- |
| 400 | `invalid_request_error` | The body is not a JSON object. |
| 401 | `authentication_error` | Missing, malformed or unknown service key. On the admin port: a missing or wrong token for `/metrics`. |
| 403 | `permission_error` | The model is not in the service's `allowed_models`. `param` is `model`. |
| 404 | `not_found_error` | No such route on the public port. |
| 422 | `validation_error` | A documented rule is broken, no backend serves the model, or a chat backend cannot express the question (more than 26 options). `param` names the field. |
| 429 | `rate_limit_error` | See below. Always has `retry-after` and `retry-after-ms`. |
| 500 | `internal_error` | The batch that carried the call was dropped. This is a gateway bug: report it. |
| 502 | `upstream_error` | The backend is unreachable, refused the gateway's own key (the service's 401 is never the cause), sent a body the gateway cannot read or split, or, on a chat backend, answered with a first token that is no label or without logprobs. |
| 504 | `timeout_error` | No answer within `request_timeout_ms`, or the backend did not answer an attempt in time after all retries. |

A 429 from the gateway has one of these causes, and the `message` says which:

- the service is over its own `requests_per_minute`;
- the service already has `max_concurrent` calls in flight (`retry-after` is 1 s);
- the backend's shared request slots are booked further ahead than
  `max_queue_wait_ms`;
- every upstream connection stayed busy past `max_queue_wait_ms`;
- the shared `tokens_per_second` budget is spent;
- the backend asked everyone to back off and its pause outlasts the call.

When a backend answers an error status that the gateway does not retry (400,
403, 422...), or keeps answering 429, 500, 502, 503, 504 or 529 until the
retries run out, the caller gets that status and the backend's `retry-after`
headers. A backend's 401 is the exception: it means the gateway's key is wrong,
so the caller gets a 502. The body depends on the protocol:

- **A System One backend**: the body as sent, because it already is TypeSafe's.
- **A chat backend**: a body in the shape above. The `type` follows the status
  as in the table (a provider's 500 or 503 is `upstream_error`), and the
  `message` is the provider's own text when its body has one, see
  [Backends](backends.md#limits-compared-to-a-system-one-backend).

Errors from the HTTP layer do not use this shape. A body larger than
`server.max_body_bytes` gets a `413` with a plain-text body, and a wrong method
on a known route gets a `405` with an empty body. Both are sent before
authentication.
