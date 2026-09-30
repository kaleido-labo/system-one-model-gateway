# Security policy

## Reporting a vulnerability

Please do not report security problems in a public issue, pull request or
discussion.

Use GitHub's private vulnerability reporting instead:

1. Open the [Security tab](https://github.com/kaleido-labo/system-one-model-gateway/security)
   of the repository.
2. Choose **Report a vulnerability**, or go straight to the
   [advisory form](https://github.com/kaleido-labo/system-one-model-gateway/security/advisories/new).
3. Describe the problem.

Useful details:

- the version or commit you tested;
- what an attacker can do and what they need to do it;
- the smallest configuration and request that reproduce it;
- a suggested fix, if you have one.

Only the maintainers can see the report. We will confirm that we received it,
keep you updated while we investigate, and agree on a disclosure date with you
once a fix is ready. The reporter is credited in the advisory unless you ask us
not to.

## Supported versions

The project is at version 0.1. Only the latest release and the `main` branch
receive security fixes.

## Scope

In scope: the gateway itself, such as authentication of calling services,
quota and model restrictions, handling of backend keys, request parsing, and
the Docker image built from this repository.

Out of scope:

- vulnerabilities in TypeSafe, Hugging Face or any other backend the gateway
  calls: report those to the vendor;
- vulnerabilities in a dependency that do not affect the gateway: report them
  upstream, although a pointer is welcome;
- findings that require an attacker who already holds the gateway's
  configuration file or process environment.

## Keys and secrets

The configuration file stores only the SHA-256 of each service key. Backend
keys are read from environment variables, not from the configuration file.
The gateway is designed not to write states or questions to its logs. If you
find a code path that logs a key, a state or a question, treat it as a
security report.
