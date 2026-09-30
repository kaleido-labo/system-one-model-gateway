# Getting started

This page takes you from a fresh clone to a running gateway, first with the
real providers, then against the bundled mock so you can try it without a
TypeSafe account.

You need a Rust toolchain (`rust-version` in `Cargo.toml` is 1.96) or Docker.

## Build

```sh
cargo build --release
# the binary is target/release/systemone-gateway
```

For Docker, see [Operations](operations.md#docker).

## Configure

```sh
cp config.example.toml gateway.toml
cargo run --release -- gen-key --service ocr-service
```

`gen-key` prints a key and a `[[service]]` block holding the key's hash. Give
the key to the service; it is not stored anywhere. Paste the hash over the
placeholder in `gateway.toml`. The file has two `[[service]]` blocks as
examples: keep one, rename it, or delete the other.

Then check the file:

```sh
cargo run --release -- check-config --config gateway.toml
```

```
gateway.toml is valid.
backend typesafe: System One at https://api.typesafe.ai for jev-*, 1200 requests/min, key from $TYPESAFE_API_KEY
backend huggingface: chat with logprobs at https://router.huggingface.co/v1 for Qwen/*, meta-llama/*, 300 requests/min, key from $HF_TOKEN
merging: calls sharing a state within 10 ms go out together
service ocr-service: 1 key(s), 600 requests/min
...
```

Every key is explained in the [configuration reference](configuration.md).

## Run

```sh
export TYPESAFE_API_KEY=...   # your TypeSafe key
export HF_TOKEN=...           # your Hugging Face token; any value works if you only call Jev
cargo run --release -- serve --config gateway.toml
```

The gateway refuses to start if a variable named by a backend's `api_key_env`
is empty, which is why `HF_TOKEN` is needed even when you only use Jev. Delete
the `huggingface` backend from your file if you do not want it.

The public API listens on port 8080, and health checks and metrics on 9090.

## Call it

The request is the one TypeSafe documents. Use the key that `gen-key` printed:

```sh
export SERVICE_KEY=s1gw_...
curl -s http://localhost:8080/v1/systemone \
  -H "Authorization: Bearer $SERVICE_KEY" \
  -H 'content-type: application/json' \
  -d '{"state": "Parking Saemes - 18,00 EUR", "model": "jev-latest",
       "questions": {"parking": {"type": "noul", "instructions": "Is this a parking receipt?"}}}'
```

```sh
curl -s localhost:9090/readyz     # ready
curl -s localhost:9090/metrics | grep calls_total
```

## Point a service at the gateway

The TypeSafe Python and JavaScript SDKs read their base URL and key from the
environment. Two variables are enough, and the service's code stays as it is:

```sh
TYPESAFE_BASE_URL=http://systemone-gateway:8080
TYPESAFE_API_KEY=s1gw_...   # the key the gateway issued to this service
```

To use a Hugging Face model instead of Jev, the service changes the `model`
in its request, for example to `Qwen/Qwen2.5-7B-Instruct`, and nothing else.
See [Backends](backends.md).

## Try it without a TypeSafe key

The repository ships the mock that the tests use. It stands in for TypeSafe
and for a chat model behind an OpenAI-compatible API. Its System One answers
carry an extra `echo` field with the question's instructions, so you can see
which answer went to which caller. The real API has no such field.

Start the mock in one terminal:

```sh
cargo run --example mock_upstream      # 127.0.0.1:9999
```

Write a configuration that points both backends at it, in `gateway.toml`:

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

Run the gateway in a second terminal. The mock expects the key
`mock-typesafe-key`:

```sh
cargo run -- check-config --config gateway.toml
TYPESAFE_API_KEY=mock-typesafe-key cargo run -- serve --config gateway.toml
```

### A call to the System One backend

```sh
curl -si http://localhost:8080/v1/systemone \
  -H "Authorization: Bearer $SERVICE_KEY" \
  -H 'content-type: application/json' \
  -d '{"state": "Parking Saemes - 18,00 EUR", "model": "jev-latest",
       "questions": {"parking": {"type": "noul", "instructions": "Is this a parking receipt?"}}}'
```

```
HTTP/1.1 200 OK
content-type: application/json
x-typesafe-request-id: req_1
x-systemone-gateway-batch-callers: 1
x-systemone-gateway-backend: typesafe
x-request-id: 6c4f9a18-bf9f-49a5-ba81-d6d92061f1cb

{"answers":{"parking":{"echo":"Is this a parking receipt?","noul":0.75,"type":"noul"}},"model":"jev-1.13.0","usage":{"input_tokens":35,"output_tokens":10}}
```

### A call to the chat backend

Changing the model to `Qwen/Qwen2.5-7B-Instruct` sends the request to the mock
chat API. This one adds a Choice question, which shows the probabilities:

```sh
curl -si http://localhost:8080/v1/systemone \
  -H "Authorization: Bearer $SERVICE_KEY" \
  -H 'content-type: application/json' \
  -d '{"state": "Parking Saemes - 18,00 EUR", "model": "Qwen/Qwen2.5-7B-Instruct",
       "questions": {
         "parking": {"type": "noul", "instructions": "Is this a parking receipt?"},
         "cat": {"type": "choice", "instructions": "Category?",
                 "criteria": {"tolls": "Motorway tolls", "fuel": null, "meals": "Restaurants"}}}}'
```

The answer comes back in TypeSafe's format:

```
HTTP/1.1 200 OK
content-type: application/json
x-typesafe-request-id: chat_2
x-systemone-gateway-batch-callers: 1
x-systemone-gateway-backend: huggingface

{"model":"Qwen/Qwen2.5-7B-Instruct","answers":{"parking":{"type":"noul","noul":0.7216494845360825},"cat":{"type":"choice","choice":"tolls","probabilities":{"tolls":0.6666666666666666,"fuel":0.33333333333333337,"meals":0.0},"confidence":0.5}},"usage":{"input_tokens":239,"output_tokens":2}}
```

### Two services, one upstream call

Send two requests with the same state and model at the same moment, from two
services or two terminals. They share one upstream call, and each gets only its
own answer and its share of the tokens:

```
HTTP 200  x-typesafe-request-id: req_2  x-systemone-gateway-batch-callers: 2
{"answers":{"is_toll":{...}},"model":"jev-1.13.0","usage":{"input_tokens":25,"output_tokens":10}}

HTTP 200  x-typesafe-request-id: req_2  x-systemone-gateway-batch-callers: 2
{"answers":{"altered":{...}},"model":"jev-1.13.0","usage":{"input_tokens":24,"output_tokens":10}}
```

Both responses carry the same request id and `batch-callers: 2`. Read
[Architecture](architecture.md) to see how that happens.

## Next steps

- [Configuration reference](configuration.md): every key.
- [Backends](backends.md): add a Hugging Face model or another System One server.
- [Operations](operations.md): Docker, Kubernetes, metrics and tuning.
