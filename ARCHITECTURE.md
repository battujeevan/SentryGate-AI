# Architecture

## Purpose

SentryGate-AI is an **agent control-plane firewall**: a deterministic Go proxy that sits between LLM tool-calling agents and enterprise infrastructure APIs (F5, Zscaler, Kubernetes-style control planes). It validates intent, throttles concurrency, starts durable Temporal sagas for accepted mutations, and records an immutable audit trail.

## Control flow

```
Agent tool-call JSON
        │
        ▼
┌───────────────────┐
│  API key auth     │
│  OTel HTTP span   │
└─────────┬─────────┘
          ▼
┌───────────────────┐     reject (403)
│ InterceptAndValidate ──────────────────► client
│ risk ceiling + protected targets
└─────────┬─────────┘
          │ accept
          ▼
┌───────────────────┐
│ Temporal workflow │  SentryGateSagaWorkflow
│ DispatchConfig    │
│ (+ compensation)  │
└─────────┬─────────┘
          ▼
┌───────────────────┐
│ SQLite audit rows │  queryable via GET /v1/audit/{id}
└───────────────────┘
```

## Package boundaries

| Package | Responsibility |
|---------|----------------|
| `shared/contracts` | Command types, errors, policy shape, audit record schema — **compliance boundary** |
| `proxy` | Zero-alloc-friendly validation, worker pool, F5/Zscaler client interfaces |
| `workflows` | Temporal saga + compensation + audit activity API |
| `internal/policy` | YAML policy pack with hot-reload |
| `internal/audit` | Append-only SQLite store |
| `internal/auth` | API key middleware |
| `internal/config` | Environment configuration |
| `cmd/sentrygate` | Ingress proxy + workflow starter |
| `cmd/worker` | Temporal worker |
| `cmd/agent-sim` | Demo agent scenarios |

## Saga semantics

1. Record `INGRESS_VALIDATION` (pass assumed at proxy).
2. Execute `DispatchConfig`.
3. On failure: disconnected-context `RevertStateCompensation`, record compensation + failure phases.
4. On success: record dispatch + workflow complete.

Non-retryable infrastructure faults use `MaximumAttempts: 1` and the `NON_RETRYABLE_INFRASTRUCTURE_FAULT` contract type so AI retry loops cannot amplify outages.

## Threat model (abbreviated)

| Threat | Mitigation |
|--------|------------|
| Agent hallucinates destructive delete | Protected target list blocks `DELETE_POLICY` on `ROOT_CORE_EDGE` |
| High-risk proposals | Risk ceiling from policy pack |
| Agent flood / retry storm | Bounded worker pool + throttled validate |
| Unauthenticated callers | `X-API-Key` required on `/v1/*` |
| Half-applied infra change | Temporal compensation activity |
| Audit tampering in-process | Append-only SQLite inserts (no update/delete API) |
| Compliance rule churn requiring redeploy | Policy YAML hot-reload separate from workflow binary |

## Non-goals (current)

- Replacing a full API gateway (rate limits beyond pool, WAF, JWT federation)
- Live production F5/Zscaler credentials in the open demo (mocks + interfaces; see `INTEGRATION.md`)
- Multi-tenant SaaS control plane
- LLM prompt filtering (this product guards **tool-call side effects**, not chat text)

## Observability

- Structured JSON logs (`log/slog`)
- OpenTelemetry HTTP instrumentation (`otelhttp`); exporter `stdout` or `none`
- Health: `/healthz`, readiness: `/readyz`
- Temporal UI on compose port `8088`
