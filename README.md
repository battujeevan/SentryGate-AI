# SentryGate-AI

**Deterministic agent control-plane firewall** in Go. Sits between autonomous LLM tool-calling agents and enterprise infrastructure APIs, enforcing non-bypassable policy, bounded concurrency, Temporal sagas with compensation, and an immutable audit trail.

[![CI](https://github.com/sentrygate-ai/sentrygate/actions/workflows/ci.yml/badge.svg)](https://github.com/sentrygate-ai/sentrygate/actions/workflows/ci.yml)

> Replace the badge URL with your fork after you push to GitHub.

## Why it exists

Prompt rules do not stop an agent from calling `DELETE_POLICY` on a root edge profile. SentryGate treats tool-calls as **control-plane mutations**: validate → throttle → durable apply → rollback → audit.

## Architecture (short)

```
AI Agent ──JSON──► Proxy (auth + policy) ──► Temporal Saga ──► F5 / Zscaler / cloud APIs
                         │                         │
                         └────── SQLite audit ◄────┘
```

See [ARCHITECTURE.md](ARCHITECTURE.md) and [SECURITY.md](SECURITY.md).

## Quick demo (Docker)

Prerequisites: Docker, Docker Compose, Go 1.21+.

```powershell
# Windows
.\scripts\demo.ps1
```

```bash
# macOS / Linux
chmod +x scripts/demo.sh
./scripts/demo.sh
```

This brings up Temporal, the worker, and the proxy, then runs three scenarios:

| Scenario | What you should see |
|----------|---------------------|
| **deny** | `403` — `ROOT_CORE_EDGE` delete blocked |
| **allow** | `200` — saga started; audit trail shows PASS phases |
| **rollback** | `200` accept, activity fails on `FAILING_NODE`, compensation + FAIL audit |

- Proxy: http://localhost:8080  
- Temporal UI: http://localhost:8088  
- Default API key: `dev-secret-change-me` (override with `SENTRYGATE_API_KEY`)

### Manual agent-sim

```bash
export SENTRYGATE_API_KEY=dev-secret-change-me
go run ./cmd/agent-sim all
```

## Local binaries (without Compose)

```bash
cp .env.example .env
# start Temporal: temporal server start-dev   OR use compose for Temporal only

export SENTRYGATE_API_KEY=dev-secret-change-me
go run ./cmd/worker &
go run ./cmd/sentrygate
```

## HTTP API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/healthz` | no | Liveness |
| GET | `/readyz` | no | Readiness (audit DB) |
| POST | `/v1/intercept` | `X-API-Key` | Validate + start saga |
| GET | `/v1/audit/{proposal_id}` | `X-API-Key` | Audit rows |
| GET | `/v1/policy` | `X-API-Key` | Active policy snapshot |

## Performance

```bash
go test ./proxy -bench=BenchmarkProxy -benchmem
```

Typical results (Windows, Go 1.27, i5-13420H):

```
BenchmarkProxyInterceptorThroughput-12     ~5 ns/op     0 B/op    0 allocs/op
BenchmarkProxyHighConcurrencyAllocations-12 ~100 ns/op  0 B/op    0 allocs/op
```

## Configuration

See [.env.example](.env.example). Policy pack: [policies/default.yaml](policies/default.yaml) (hot-reloaded).

## Repo layout

```
cmd/sentrygate   Ingress proxy
cmd/worker       Temporal worker
cmd/agent-sim    Demo client
proxy/           Firewall + vendor client mocks
workflows/       Saga + compensation
shared/contracts Compliance types & errors
internal/        Config, auth, audit DB, policy, telemetry
policies/        Hot-reloadable policy YAML
```

## Downstream vendors

Mock F5 / Zscaler clients ship for demos. Live wiring notes: [INTEGRATION.md](INTEGRATION.md).

## License

Apache-2.0 — see [LICENSE](LICENSE).
