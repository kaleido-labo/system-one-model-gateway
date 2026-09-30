# Contributing

Thanks for your interest in systemone-gateway. Bug reports, fixes, tests and
documentation improvements are all welcome. For anything bigger than a small
fix, open an issue first so we can agree on the approach before you write code.

By taking part you agree to follow the [Code of Conduct](CODE_OF_CONDUCT.md).
To report a security problem, see [SECURITY.md](SECURITY.md) and do not open a
public issue.

## Requirements

- Rust 1.96 or newer (see `rust-version` in `Cargo.toml`). The crate uses
  edition 2024.
- Docker, only if you want to build the image.

## Build, test and lint

```sh
cargo build
cargo test                                   # unit tests, and end-to-end tests against a mock upstream
cargo fmt --check                            # `cargo fmt` fixes the formatting
cargo clippy --all-targets -- -D warnings
```

These are the checks CI runs on every pull request, along with a Docker image
build (`docker build -t systemone-gateway .`). Run them before you push.
The end-to-end tests start their own mock upstream on a local port, so they
need no network access and no API key.

## Run the gateway against the mock upstream

The `examples/` directory holds a mock TypeSafe API. It is the same one the
tests use, and it also serves a chat completions API with logprobs under
`/v1`. You can try the gateway without a TypeSafe key.

1. Start the mock (listens on `127.0.0.1:9999`, expects the key
   `mock-typesafe-key`):

   ```sh
   cargo run --example mock_upstream
   ```

2. In another terminal, generate a key for a calling service. The command
   prints the key and the hash that goes in the configuration:

   ```sh
   cargo run -- gen-key --service demo
   ```

3. Write `gateway.toml`, pasting the printed hash:

   ```toml
   [[backend]]
   name = "typesafe"
   base_url = "http://127.0.0.1:9999"
   api_key_env = "TYPESAFE_API_KEY"
   models = ["jev-*"]

   [[service]]
   name = "demo"
   key_sha256 = ["<hash printed by gen-key>"]
   ```

   `config.example.toml` lists every option with its default.

4. Check the file, then start the gateway:

   ```sh
   cargo run -- check-config --config gateway.toml
   TYPESAFE_API_KEY=mock-typesafe-key cargo run -- serve --config gateway.toml
   ```

5. Send a call with the key from step 2:

   ```sh
   curl -s http://127.0.0.1:8080/v1/systemone \
     -H "Authorization: Bearer <key printed by gen-key>" \
     -H 'content-type: application/json' \
     -d '{"state": "Parking receipt, 18 EUR", "model": "jev-latest",
          "questions": {"parking": {"type": "noul", "instructions": "Is this a parking receipt?"}}}'
   ```

   The mock adds an `echo` field to each answer so you can see which answer
   went to which question. Health and metrics are on port 9090
   (`/healthz`, `/readyz`, `/metrics`).

Do not commit keys or local configuration files such as `gateway.toml`.

## Making a change

1. Fork the repository and create a branch from `main`. Use a short, descriptive
   name such as `fix-retry-after-parsing`.
2. Keep the pull request to one intent. A refactor, a rename or a reformat goes
   in its own pull request, not inside a feature or a fix.
3. Add or update tests with the code. Behavior that crosses the HTTP boundary
   belongs in `tests/`, next to the existing end-to-end tests.
4. Update `README.md`, `config.example.toml` and `CHANGELOG.md` (under
   `Unreleased`) when you change behavior, configuration or the API.
5. Explain the why in a code comment next to the code. The pull request
   description is read once; the comment stays.

### Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/): one
sentence, imperative, in English, such as
`fix: follow retry-after-ms before retry-after`. Types in use:

| Type | For |
| --- | --- |
| `feat` | A new capability |
| `fix` | A bug fix |
| `refactor` | A change that alters neither behavior nor the public interface |
| `docs` | Documentation only |
| `test` | Tests only |
| `chore` | Tooling, CI, dependencies, repository files |

Add `!` after the type (`feat!:`) for a breaking change, and label the pull
request `breaking-change`.

### Pull requests

- Rebase onto a fresh `main` and run the checks above on that base.
- Fill in the pull request template: TL;DR, Context, What changed, How to
  verify (the exact commands and their output), Notes for the reviewer.
- Prefer pull requests under about 400 changed lines. If yours is larger,
  consider splitting it.
- Read your own diff first and comment on anything the code cannot explain by
  itself, such as deletions or moved files.
- During review, push follow-up commits instead of amending or force-pushing,
  so the reviewer only reads what changed. Reply to every comment, even if the
  answer is "done".
- CI must be green before a pull request is merged.

## Reporting bugs and asking for features

Use the issue forms. A bug report is easiest to act on when it includes the
smallest configuration and request that reproduce the problem, ideally against
the mock upstream as shown above. Remove API keys and anything private from
logs and configuration before you paste them.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).
