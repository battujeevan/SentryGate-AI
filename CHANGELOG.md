# Changelog

## Unreleased: Phase 1, decision boundary

### Breaking changes
- `AgentProposal.risk_score` has been removed. The value was supplied by the agent and trusted by the proxy. Requests containing it, or any other unknown field, now get `400`.
- `SENTRYGATE_API_KEY` (one shared key) has been replaced on the proxy by `SENTRYGATE_AGENT_KEYS` (`agent-id:key,...`). `agent-sim` still reads `SENTRYGATE_API_KEY` as the key it presents.
- New policy schema: `version`, `max_parallel_tasks`, `environments`, `commands`, `agents`, `targets`. The old `max_risk_ceiling` / `protected_targets` format is rejected. No field has a default.
- `GET /v1/policy` returns metadata (version, digest, load time, reload status) instead of policy contents.
- Workflow input is now `ExecutionRequest`, and the audit phase `INGRESS_VALIDATION` has been replaced by `WORKFLOW_REVALIDATION`.

### Added
- Per-agent identity resolved from the API key, compared in constant time across all keys.
- Command catalogue, per-agent command permissions, target registry with environments and protected flag, and per-environment `allow` / `require_approval` / `deny` rules.
- `ALLOW` / `DENY` / `REQUIRE_APPROVAL` verdicts with stable reason codes and fixed precedence.
- Decision records (SQLite `decision_records`) for each evaluated, well-formed request, written before any workflow starts. The exceptions are requests cancelled before evaluation and failed writes. `GET /v1/decisions/{proposal_id}`.
- Canonical SHA-256 request hash.
- Workflow re-validation against the worker's current policy before any side effect. Non-`ALLOW` fails with non-retryable `REVALIDATION_DENIED`.
- HTTP hardening:
  - 64 KiB body limit (`413`).
  - Single JSON object only, with trailing data rejected.
  - Proposal ID validation.
  - Generic `502` / `500` bodies.
  - `409 not_executed` when a workflow for the proposal ID is already running, instead of reporting the running execution as accepted.
  - Server read (30s) and idle (120s) timeouts in addition to the 5s header timeout.
- `agent-sim` scenarios for each rule and a `-check` mode.
- CI: `go test -race` and a Docker Compose smoke job that runs `agent-sim -check`.
- Makefile targets `race` and `check`.

### Changed
- Policy reload detects changes by SHA-256 digest instead of modification time. An invalid reload keeps the last good policy, is logged, and shows in `/v1/policy`. The proxy reads the live snapshot directly.
- The worker loads the policy (it's required for re-validation) and mounts it in Compose.
- SQLite now uses WAL mode with a 5s busy timeout. Inserts are idempotent for identical content.
- Compose binds Temporal (7233), Temporal UI (8088), worker health (8081) and the proxy (8080) to `127.0.0.1`.
- The Dockerfile creates `/data` owned by the nonroot user, so the audit volume is writable.
- Audit record IDs include the workflow run ID.
- Documentation now distinguishes implemented, simulated, planned and out-of-scope behaviour.

### Removed
- Risk-score ceiling and the server-side scoring experiment.
- The unused F5 / Zscaler accessors on the proxy.

### Fixed
- Policy reload applied changes only when the protected-target *count* changed; same-length edits were ignored.
- Reload errors were silently discarded.
- Protected targets blocked only `DELETE_POLICY`; they now block every command.
- Unknown commands and unregistered targets were allowed.
- Denials were not recorded.
- The workflow recorded an ingress `PASS` without checking anything.

## v0.1.0 (2026-09-25)

### Phase A: demoable stack
- Docker Compose: Temporal + UI + proxy + worker
- `cmd/agent-sim` scenarios: deny / allow / rollback
- `scripts/demo.ps1` and `scripts/demo.sh`

### Phase B: runtime
- Env-based config (`.env.example`)
- API key auth on `/v1/*`
- Structured JSON logging (`slog`)
- OpenTelemetry HTTP spans (`stdout` | `none`)
- SQLite audit store (shared volume)
- Hot-reloadable policy YAML
- `/healthz`, `/readyz`, `/v1/policy`, `/v1/audit/{id}`

### Phase C: engineering hygiene
- Apache-2.0 LICENSE
- ARCHITECTURE.md, SECURITY.md, INTEGRATION.md
- GitHub Actions CI (`go vet`, `go test`, build)
- Unit + Temporal workflow tests
- Makefile targets
