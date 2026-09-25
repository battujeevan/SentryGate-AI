# Changelog

## v0.1.0 — 2026-09-25

### Phase A — Demoable stack
- Docker Compose: Temporal + UI + proxy + worker
- `cmd/agent-sim` scenarios: deny / allow / rollback
- `scripts/demo.ps1` and `scripts/demo.sh`
- Polished README with quickstart

### Phase B — Production-shaped runtime
- Env-based config (`.env.example`)
- API key auth on `/v1/*`
- Structured JSON logging (`slog`)
- OpenTelemetry HTTP spans (`stdout` | `none`)
- SQLite append-only audit store (shared volume)
- Hot-reloadable policy YAML (`policies/default.yaml`)
- `/healthz`, `/readyz`, `/v1/policy`, `/v1/audit/{id}`
- Proxy starts Temporal saga on accept

### Phase C — Engineering hygiene
- Apache-2.0 LICENSE
- ARCHITECTURE.md, SECURITY.md, INTEGRATION.md
- GitHub Actions CI (`go vet`, `go test`, build)
- Unit + Temporal workflow tests
- Makefile targets
