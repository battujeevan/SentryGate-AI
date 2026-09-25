# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` / `v0.1.x` | Yes |

## What is enforced today

- **API key authentication** on all `/v1/*` routes (`X-API-Key`).
- **Deterministic proposal validation** (risk ceiling, protected targets).
- **Concurrency bounding** via channel worker pool.
- **Durable saga compensation** for failed infrastructure mutations.
- **Append-only audit persistence** (SQLite).
- **Policy pack isolation** in `policies/*.yaml` (hot-reloadable).

## Out of scope (do not assume)

- Mutual TLS termination or SPIFFE identities
- Fine-grained RBAC / OAuth2 / OIDC
- Encryption at rest for the audit database (use volume encryption in production)
- Guaranteed prevention of all unsafe LLM behavior outside tool-call schema
- Hardened multi-tenant isolation

## Reporting a vulnerability

Please open a private security advisory on the GitHub repository (or email the maintainers if advisories are unavailable). Include:

1. Affected component / commit
2. Reproduction steps
3. Impact assessment

Do not file public issues for exploitable vulnerabilities until a fix is available.

## Production hardening checklist

Before exposing SentryGate beyond a lab:

1. Replace `SENTRYGATE_API_KEY` with a high-entropy secret (or migrate to mTLS/JWT).
2. Run proxy and worker as non-root (compose uses distroless nonroot).
3. Put TLS in front (ingress / reverse proxy).
4. Restrict network egress from the worker to known infra APIs.
5. Back up and retain audit SQLite/Postgres according to compliance retention.
6. Pin image digests and scan CI artifacts.
