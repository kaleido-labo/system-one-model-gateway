# Documentation

| Page | Read it to |
| --- | --- |
| [Getting started](getting-started.md) | Build the gateway, configure a service, make a first call, and try it locally against the bundled mock. |
| [Architecture](architecture.md) | Follow a call through authentication, validation, routing, batching by state, pacing, retries and answer splitting. |
| [Configuration](configuration.md) | Look up a configuration key: type, default, meaning and validation rules. |
| [API](api.md) | Know the endpoints, headers, error shape and status codes that services see. |
| [Backends](backends.md) | Route models to TypeSafe, another System One server or a chat model, and know the limits of chat models. |
| [Operations](operations.md) | Run it in Docker or Kubernetes, read the logs and metrics, tune limits and deadlines. |

Also useful:

- [`config.example.toml`](../config.example.toml): a complete, commented configuration that the test suite checks.
- [Project README](../README.md): what the gateway is and a short quick start.

The pages describe the gateway by concept, not by source file. The module map at
the end of the [README](../README.md#development) lists where each concept lives
in the code.
