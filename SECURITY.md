# Security Policy

SentryGate is a portfolio / reference project. It has not been independently audited and is not intended for production use as-is.

## Supported versions

| Version | Supported |
|---|---|
| `main` | Yes |

## What is enforced (implemented and tested)

- **Agent identity.** Each agent has its own API key (`SENTRYGATE_AGENT_KEYS`). Keys resolve to an agent ID by constant-time comparison of SHA-256 digests across all keys, with no early exit. The configuration is rejected at startup if entries are malformed, IDs or keys are duplicated, or keys are short. Keys are never logged, stored in records, or returned.
- **Strict ingress.**
  - 64 KiB body limit.
  - Exactly one JSON object; unknown fields are rejected, including `risk_score`, which earlier versions trusted. The decoder is Go's `encoding/json`, so field names match case-insensitively (`"ID"` fills `id`) and a repeated key is accepted with the last value winning. The decoded values are what is evaluated, hashed and recorded, and identity never comes from the body.
  - Proposal ID character and length limits.
  - Internal errors are not returned to clients.
  - Server timeouts: 5s to read headers, 30s to read the whole request (which also bounds handler work), and 120s for idle keep-alive connections.
- **Deterministic authorization.** Declared agents, per-agent command permissions, a command catalogue, a target registry, protected targets (all mutations denied) and per-environment rules. Anything not declared is denied.
- **Decision evidence.** Each evaluated, well-formed request produces a decision record with identity, request hash, verdict, reasons and the policy version and digest. There are two exceptions. A request cancelled before evaluation gets `503` and no record. If the record write fails, the failure is logged, a `DENY` or `REQUIRE_APPROVAL` is still refused, and an `ALLOW` is not executed (`503`).
- **Re-validation.** The workflow re-evaluates each proposal against the worker's current policy before any side effect and refuses on anything but `ALLOW`.
- **Policy integrity.** Strict schema; the digest is logged and exposed. An invalid reload never replaces the active policy.
- **Container hygiene.** Distroless nonroot images; Compose binds all ports to `127.0.0.1`.

## Trusted computing base

These components must be trusted, and their compromise defeats SentryGate:

- **Temporal and its database.** Temporal is unauthenticated in the Compose stack. Anyone who can start workflows on the task queue can claim any agent ID. Re-validation still enforces the policy for the claimed identity, but it cannot verify the identity.
- **The worker.** It holds the policy and executes activities.
- **The shared audit volume.** Records are append-only in code, but anyone with file access can modify or delete them. They are not tamper-evident.
- **The policy file and the environment providing `SENTRYGATE_AGENT_KEYS`.**

## Known limitations (do not assume otherwise)

- An agent that holds infrastructure credentials can bypass SentryGate entirely. It is only effective if agents have no other path to the infrastructure.
- No semantic judgment: permitted commands on permitted targets are allowed regardless of payload content.
- No prompt-injection defense.
- `REQUIRE_APPROVAL` is a verdict only; there is no approval workflow yet.
- The infrastructure adapter is simulated. Compensation is best effort and not idempotent.
- Single node; no TLS termination; no rate limiting beyond a startup-sized evaluation pool; no key rotation without restart.
- The audit database is not encrypted at rest.
- Record reads are not scoped per agent. Any authenticated agent can read the decision and audit records of any proposal ID through `GET /v1/decisions/{id}` and `GET /v1/audit/{id}`, including proposals submitted by other agents.

## Reporting a vulnerability

Please open a private security advisory on the GitHub repository. Include the affected commit, reproduction steps and impact. Do not file public issues for exploitable vulnerabilities until a fix is available.

## Hardening checklist before any non-lab use

1. Generate high-entropy per-agent keys, supply them from a secret store, and plan for rotation.
2. Enable Temporal authentication/authorization (mTLS and an authorizer), and restrict who can start workflows on the task queue.
3. Terminate TLS in front of the proxy.
4. Ensure agents have no direct infrastructure credentials.
5. Restrict worker egress to the infrastructure APIs it needs.
6. Move records to storage with access control, backups and tamper evidence.
7. Pin image digests and scan images in CI.
