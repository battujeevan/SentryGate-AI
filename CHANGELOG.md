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
- Ingress decision binding. Before re-validation, the workflow requires `ingress_decision_id` to name a recorded `INGRESS`-stage `ALLOW` with the same agent ID, recomputed request hash, proposal ID and workflow ID. Otherwise it records a `DENY` with an `INGRESS_DECISION_*` reason and fails with non-retryable `INGRESS_DECISION_INVALID`. This also applies when the record cannot be read. Previously the ID was carried but never checked, so a workflow started directly in Temporal with a fabricated ID could dispatch. Workflows started before this change will fail replay on an upgraded worker.
- Execution claims (SQLite `execution_claims`). The workflow atomically claims its ingress decision after verifying it, marks the claim `EXECUTING` before dispatch, and `COMPLETED` or `FAILED` after. A decision claimed by another run fails with non-retryable `EXECUTION_CLAIM_REJECTED` (`INGRESS_DECISION_ALREADY_CLAIMED`) without dispatching, so one ingress decision authorizes at most one dispatch, including after its workflow has closed and under concurrent starts. Claims are released on every exit before dispatch. Previously a closed workflow's input could be started again and would dispatch again. The SQLite connection now enforces foreign keys.
- Explicit dispatch outcomes and reconciliation.
  - Dispatch now reports `SUCCESS`, `FAILURE` or `UNKNOWN` through the `workflows.Adapter` contract. Any dispatch activity error (timeout, cancellation, lost worker) or unrecognised status is `UNKNOWN`, never `FAILED`.
  - New claim state `RECONCILIATION_REQUIRED`, entered from `EXECUTING` on `UNKNOWN`. It is never released or claimed again and moves to `COMPLETED` or `FAILED` once reconciliation confirms an outcome.
  - The owning run reconciles with backoff for up to 24 hours and never re-dispatches. If the outcome is still unknown, it fails with non-retryable `DISPATCH_OUTCOME_UNKNOWN` and leaves the claim unresolved.
  - Every dispatch and reconciliation carries an idempotency key derived only from the ingress decision.
  - Compensation runs only on a confirmed `FAILURE` with `partially_applied`, not on `UNKNOWN` and not on a plain `FAILURE`.
  - Confirmed failures now fail the workflow with non-retryable `DISPATCH_FAILED`.
  - New audit phase `DISPATCH_RECONCILIATION` and audit verdict `UNKNOWN`.
  - The simulated adapter is now deterministic per target, with new targets `LOST_RESPONSE_NODE`, `UNREACHABLE_NODE` and `PARTITIONED_NODE`.
    - Previously, any dispatch error marked the claim `FAILED` and ran compensation, even when the change might have been applied.
- Execution fence inside the dispatch activity.
  - The `CLAIMED` → `EXECUTING` transition now happens in `DispatchConfig`, immediately before the adapter call, for the owner built from the activity's own Temporal info. The adapter is called only if that compare-and-set succeeds. The workflow no longer writes `EXECUTING`, and `AdvanceExecutionClaim` refuses it.
  - `EXECUTING` can be entered only from `CLAIMED`; the same-owner `EXECUTING` → `EXECUTING` retry has been removed.
  - A failed fence returns non-retryable `EXECUTION_FENCE_REJECTED` or `EXECUTION_FENCE_UNKNOWN` without calling the adapter. The workflow releases the claim if it is still `CLAIMED` and fails with `EXECUTION_CLAIM_REJECTED`; if the fence may have committed, it treats the outcome as `UNKNOWN` and reconciles.
  - New audit phase `EXECUTION_FENCE`, written only when the fence was not acquired.
  - Previously, `EXECUTING` was written by a workflow activity before `DispatchConfig` was scheduled, and the dispatch activity did not check the claim. A run created by a Temporal reset after that point replayed the earlier result and could call the adapter again.
  - `DispatchConfig` now takes `contracts.FencedDispatch`, and the workflow's event history has changed. Workflows started before this change will fail replay on an upgraded worker.
- HTTP hardening:
  - 64 KiB body limit (`413`).
  - Single JSON object only, with trailing data rejected.
  - Proposal ID validation.
  - Generic `502` / `500` bodies.
  - `409 not_executed` when a workflow for the proposal ID is already running, instead of reporting the running execution as accepted.
  - Server read (30s) and idle (120s) timeouts in addition to the 5s header timeout.
- `agent-sim` scenarios for each rule, an unknown-outcome reconciliation scenario, and a `-check` mode.
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
