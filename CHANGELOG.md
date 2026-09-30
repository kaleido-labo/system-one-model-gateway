# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Repository files for contributors: CI workflow, Dependabot configuration,
  issue forms, pull request template, contributing guide, security policy,
  code of conduct, editor and rustfmt configuration, and a cargo-deny
  configuration for license and advisory checks.
- Repository metadata in `Cargo.toml`.
- A circuit breaker per backend (`circuit_breaker_failures`,
  `circuit_breaker_cooldown_ms`) and opt-in `fallback` backends. A call whose
  backend is down goes to the next usable fallback, or gets a fast 503 with
  `retry-after` when none is left.
- An opt-in in-memory answer cache (`[cache]`), keyed by model, state, extra
  fields and question, with a per-backend opt-out, `cache-control: no-cache`
  and an `x-systemone-gateway-cache` response header.
- An optional Redis-backed rate limiter (`[cluster]`), so that replicas share
  each backend's and each service's limits and the pause after a 429. Pacing
  falls back to a per-replica share while Redis is unreachable.
- Opt-in OpenTelemetry traces over OTLP/HTTP (`[tracing]`), with W3C
  `traceparent` propagation from callers to backends and one span per call,
  batch and upstream attempt.
- An optional bearer token for the admin port's `/metrics`
  (`server.admin_token_env`).
- An opt-in adaptive request rate per backend (`adaptive_rate`): a 429
  lowers the backend's `requests_per_minute` once per episode, down to a floor,
  and quiet periods raise it back to the configured value. The current rate is
  exported as `requests_per_minute_limit`.
- Configuration reload without a restart, on `SIGHUP` or when the file content
  changes (`server.config_reload_interval_ms`). Services, backends, merging,
  the cache and the request timeout are applied live; unchanged backends keep
  their queues, pacing and breaker state. An invalid file leaves the running
  configuration in place.

### Changed

- Errors from chat backends now reach the caller in TypeSafe's error shape,
  with the provider's status kept.
- The merging metrics (`batch_callers`, `batch_questions`,
  `queue_wait_seconds`, `deduplicated_questions_total`,
  `estimated_tokens_saved_total`, `isolated_replays_total`) carry a `backend`
  label. Queries that read them without one need a `sum by`.

## [0.1.0]

First version of the gateway.

### Added

- `POST /v1/systemone`, accepting the same request and response as TypeSafe's
  System One API, and `GET /v1/models`.
- Merging of calls that share a model, a state and the same other top-level
  fields into one upstream request. Identical questions are sent once, and the
  upstream `usage` is split between callers so the shares add up to what the
  backend billed.
- Validation of requests against the documented System One rules before they
  can join a merged call, so a malformed question fails on its own.
- Pacing of upstream calls against a requests-per-minute and a
  tokens-per-second budget, a cap on concurrent calls, and retries with
  exponential backoff on 429, 529, 5xx and network errors that honor
  `retry-after-ms` and `retry-after`.
- A pause on every batch after a 429, with batches leaving one slot apart when
  it ends, and a bounded queue wait: a call that cannot be served in time gets
  a 429 with `retry-after`.
- Replay of each caller alone when a backend rejects a merged request, so the
  error reaches only the caller whose question caused it.
- Service keys stored as SHA-256 hashes, with per-service quotas (requests per
  minute, concurrent calls, allowed models) and several hashes per service for
  key rotation.
- Backends selected by model name, with exact and `prefix*` matching. The
  `systemone` protocol covers TypeSafe or any server with the same API. The
  `chat` protocol covers chat completion APIs such as Hugging Face Inference
  Providers, vLLM or TGI, and reads answers from logprobs.
- Prometheus metrics and the `/healthz` and `/readyz` probes on a separate
  admin port, and logs in text or JSON that do not contain states or
  questions.
- Graceful shutdown that lets in-flight calls finish.
- `serve`, `check-config`, `gen-key` and `hash-key` commands.
- TOML configuration, documented in `config.example.toml`.
- A distroless, non-root Docker image.
- A mock upstream in `examples/`, also used by the end-to-end tests, to run
  the gateway without a TypeSafe key.
- MIT license.

[Unreleased]: https://github.com/kaleido-labo/system-one-model-gateway/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kaleido-labo/system-one-model-gateway/releases/tag/v0.1.0
