# SentryGate-AI

A deterministic policy gate between AI agents and infrastructure changes, written in Go.

An agent submits a proposed change (command, target, payload) over HTTP. SentryGate authenticates the agent, evaluates the proposal against a static policy (declared agents, a command catalogue, a target registry and per-environment rules), records the decision, and only for an `ALLOW` starts a Temporal workflow. That workflow re-evaluates the proposal against the current policy before running a **simulated** infrastructure adapter.

[![CI](https://github.com/battujeevan/SentryGate-AI/actions/workflows/ci.yml/badge.svg)](https://github.com/battujeevan/SentryGate-AI/actions/workflows/ci.yml)

## Status

| | |
|---|---|
| **Implemented** | Per-agent API keys resolved to an agent identity. Strict JSON ingress (unknown fields such as `risk_score` are rejected). Deterministic policy evaluation with `ALLOW` / `DENY` / `REQUIRE_APPROVAL` verdicts and stable reason codes. Strictly validated policy file with SHA-256 digest and digest-based hot reload that keeps the last good policy. A decision record for each evaluated, well-formed request, written before any workflow starts. There are two exceptions. A request cancelled before evaluation gets `503` and no record. If the record write fails, the failure is logged and nothing executes (see [SECURITY.md](SECURITY.md)). Canonical request hash. Before any side effect, the workflow requires a matching recorded ingress `ALLOW`, atomically claims it so that it authorizes at most one execution, and re-validates against the current policy. Explicit dispatch outcomes (`SUCCESS` / `FAILURE` / `UNKNOWN`): an unknown outcome, including a timeout, is never treated as failure. It keeps the claim, is never re-dispatched or compensated, and is reconciled through the adapter with a stable per-decision idempotency key. SQLite storage in WAL mode. |
| **Simulated** | The only infrastructure adapter. It changes nothing; it prints, and answers dispatch and reconciliation deterministically per target (success by default; partial failure, lost response, unreachable and partitioned targets for testing). It honours idempotency keys in process memory only. The F5 / Zscaler clients in `proxy/clients.go` are in-memory test doubles and are not wired to the workflow. |
| **Planned** | An approval workflow for `REQUIRE_APPROVAL` (today it is only a verdict; nothing is executed). A real adapter that reconciles against a target, resolution of outcomes still unknown after the reconciliation window, pre-image based compensation, per-target serialization and tamper-evident records. |
| **Out of scope** | Judging whether a change is wise, prompt-injection defense, real vendor credentials, multi-node deployment, UI. |

## How a request is decided

```
Agent ──X-API-Key──► auth (key → agent ID)
                      │
                      ▼
            strict decode (64 KiB, one JSON object, no unknown fields)
                      │  malformed → 400/413, logged, not recorded
                      ▼
            policy evaluation ──► decision record (SQLite)
                      │
      DENY → 403 "rejected"      REQUIRE_APPROVAL → 403 "not_executed"
                      │ ALLOW (only if the record was written, else 503)
                      ▼
            Temporal workflow (409 "not_executed" if one is already running for this ID)
              1. verify the recorded ingress ALLOW (agent, request hash, proposal, workflow ID)
                 missing/mismatched → record + INGRESS_DECISION_INVALID, nothing dispatched
              2. claim the ingress decision (at most one execution per decision)
                 already claimed → record + EXECUTION_CLAIM_REJECTED, nothing dispatched
              3. re-evaluate against the worker's current policy
                 non-ALLOW → record + REVALIDATION_DENIED, claim released, nothing dispatched
              4. dispatch activity: fence CLAIMED → EXECUTING as this run, then simulated dispatch once
                 (idempotency key from the decision); fence not acquired → adapter not called
                 SUCCESS → COMPLETED
                 FAILURE → FAILED (+ compensation only if partly applied)
                 UNKNOWN / timeout → RECONCILIATION_REQUIRED, reconcile (never re-dispatch)
                   → COMPLETED / FAILED, or still unknown: DISPATCH_OUTCOME_UNKNOWN
              5. audit rows per phase
```

Checks run in a fixed order and every applicable deny reason is reported:

1. `AGENT_UNKNOWN`: the authenticated agent is not declared in the policy.
2. `COMMAND_UNKNOWN`: the command is not in the catalogue.
3. `COMMAND_NOT_PERMITTED_FOR_AGENT`: the agent may not issue this command.
4. `TARGET_UNREGISTERED`: the target is not in the registry.
5. `TARGET_PROTECTED`: protected targets deny every mutation.
6. The environment rule for the target's environment: `allow` gives `ENVIRONMENT_ALLOWED`, `require_approval` gives `APPROVAL_REQUIRED`, and `deny` gives `ENVIRONMENT_DENIED`.

`DENY` beats `REQUIRE_APPROVAL`, which beats `ALLOW`. The response and the record list only the reasons that decided the verdict.

## Quick demo (Docker)

Prerequisites: Docker with Compose, Go 1.27+.

```powershell
.\scripts\demo.ps1          # Windows
```

```bash
./scripts/demo.sh           # macOS / Linux
```

This starts Temporal, the worker and the proxy (all ports bound to `127.0.0.1`), then runs the simulator:

| Scenario | Proposal | Expected |
|---|---|---|
| `protected-delete` | `DELETE_POLICY` on `ROOT_CORE_EDGE` | `403` `DENY` `TARGET_PROTECTED` |
| `protected-modify` | `MODIFY_ROUTING` on `ROOT_CORE_EDGE` | `403` `DENY` `TARGET_PROTECTED` |
| `unknown-command` | `REBOOT_ALL` on `edge-node-west-1` | `403` `DENY` `COMMAND_UNKNOWN` |
| `unregistered-target` | `MODIFY_ROUTING` on `shadow-edge-9` | `403` `DENY` `TARGET_UNREGISTERED` |
| `approval-required` | `UPDATE_CERTIFICATE` on `prod-payments-edge` | `403` `REQUIRE_APPROVAL`, not executed |
| `allowed` | `MODIFY_ROUTING` on `edge-node-west-1` | `200` `ALLOW`, workflow completes |
| `rollback` | `UPDATE_CERTIFICATE` on `FAILING_NODE` | `200` `ALLOW`, simulated dispatch partly applies and fails, compensation runs |
| `unknown-outcome` | `MODIFY_ROUTING` on `LOST_RESPONSE_NODE` | `200` `ALLOW`, simulated response lost (`UNKNOWN`), reconciliation confirms success, workflow completes |

`go run ./cmd/agent-sim -check all` runs the same scenarios and exits non-zero if any status, verdict, decision record or workflow outcome differs from the table. CI runs this against the Compose stack.

- Proxy: http://localhost:8080
- Temporal UI: http://localhost:8088
- Demo key: `dev-secret-change-me` for agent `agent-sim` (override with `SENTRYGATE_AGENT_KEYS` / `SENTRYGATE_API_KEY`)

## Configuration

Agent keys are supplied to the proxy as `agent-id:key` pairs and never appear in the policy file:

```bash
export SENTRYGATE_AGENT_KEYS="agent-sim:$(openssl rand -hex 24),deploy-bot:$(openssl rand -hex 24)"
```

The proxy refuses to start on malformed entries, duplicate agent IDs, duplicate keys, keys shorter than 16 bytes, or keys containing whitespace. Presented keys are compared against SHA-256 digests of the configured keys, in constant time. Keys are never logged, recorded or returned. The raw `SENTRYGATE_AGENT_KEYS` value still exists in the process environment and configuration, so protect it like any other secret.

The policy ([policies/default.yaml](policies/default.yaml)) is strictly validated: unknown fields, missing fields, duplicates and invalid values are rejected, and there are no defaults.

```yaml
version: "2026-09-27.1"
max_parallel_tasks: 64          # startup only
environments: {staging: allow, production: require_approval}
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - {id: agent-sim, commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]}
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-node-west-1, environment: staging, protected: false}
```

The proxy and the worker each re-read the file every `SENTRYGATE_POLICY_RELOAD` (default 5s) and switch when its SHA-256 digest changes. An invalid file is rejected, logged, reported by `GET /v1/policy`, and the last good policy stays active. An invalid policy at startup stops the process. See [.env.example](.env.example) for all variables.

## HTTP API

All `/v1/*` routes require `X-API-Key`.

| Method | Path | Description |
|---|---|---|
| GET | `/healthz` | Liveness |
| GET | `/readyz` | Readiness (audit database) |
| POST | `/v1/intercept` | Submit a proposal: `{"id","type","target_id","payload"}` |
| GET | `/v1/decisions/{proposal_id}` | Decision records (ingress and workflow re-validation) |
| GET | `/v1/audit/{proposal_id}` | Workflow phase records |
| GET | `/v1/policy` | Active policy version, digest, load time and reload status (no policy contents) |

`POST /v1/intercept` responses:

| Status | Meaning |
|---|---|
| `200` `accepted` | `ALLOW`; decision recorded, workflow started |
| `403` `rejected` | `DENY` |
| `403` `not_executed` | `REQUIRE_APPROVAL`; recorded, nothing executed |
| `400` | Malformed JSON, unknown field, trailing data, or invalid `id` (`^[A-Za-z0-9._:-]{1,128}$`) |
| `401` | Missing or unknown key |
| `409` `not_executed` | `ALLOW` recorded, but a workflow for this proposal ID is already running; this request was not executed |
| `413` | Body over 64 KiB |
| `502` | Workflow start failed or its outcome is unknown (generic message); check `GET /v1/decisions/{proposal_id}` before retrying |
| `503` | Cancelled before evaluation, or `ALLOW` could not be recorded; nothing was started |

## Tests

```bash
go test ./...
make race    # go test -race ./... (needs a C toolchain; runs in CI)
make bench
```

The tests cover authentication and identity, every decision rule, decision records (identity, policy version and digest, request hash), write-failure behaviour, HTTP hardening, policy reload edge cases, ingress-decision verification for direct Temporal submissions (missing, forged, denied, wrong-stage, other-agent, altered-content and other-workflow decisions never dispatch), the execution claim (a decision executed twice, sequentially or concurrently, dispatches once; claim-store failures, lost responses, release and cancellation), dispatch outcomes (timeouts and other ambiguous results become `UNKNOWN`, keep the claim and are never re-dispatched or compensated; reconciliation to success, failure or still unknown; a stable idempotency key; repeated reconciliation applies nothing twice), and workflow re-validation when the policy is tightened between ingress and execution.

Policy evaluation benchmark, from a single local run (Windows, Go 1.27, i5-13420H; indicative only):

```
BenchmarkProxyEvaluate-12                    68 ns/op   16 B/op   1 allocs/op
BenchmarkProxyEvaluateThrottledParallel-12  252 ns/op   16 B/op   1 allocs/op
```

This measures evaluation only, not HTTP, SQLite or Temporal.

## Limitations

- **SentryGate only gates what is routed through it.** An agent that holds infrastructure credentials can bypass it entirely. The agent must have no direct access.
- **Temporal, its database, the worker and the audit volume are trusted.** Temporal is unauthenticated in the Compose stack. A workflow started there dispatches only if it matches a recorded ingress `ALLOW` that no other execution has claimed. Anyone who can write the database can still forge a record or reset a claim.
- **No semantic judgment.** A permitted command on a permitted target is allowed even if the payload is a bad idea.
- **No prompt-injection defense.** An injected agent issuing a permitted command is indistinguishable from a legitimate one.
- **Single node.** SQLite, with no HA. `max_parallel_tasks` is applied at startup only.
- **Simulated adapter.** Nothing touches real infrastructure, and reconciliation only asks the simulator, whose record of keys is in memory: it is lost on worker restart and not shared between workers. Compensation is best effort, runs once, and is not idempotent.
- **Unresolved outcomes need an operator.** If reconciliation still has no answer after 24 hours, or the workflow is cancelled while reconciling, the claim stays `RECONCILIATION_REQUIRED`. The decision cannot run again, and nothing yet resolves the claim or exposes it through the API.
- **Approval is a verdict, not a workflow.** `REQUIRE_APPROVAL` requests are recorded and refused; there is no way to approve them yet.
- Records are append-only in code but not tamper-evident. Anyone with access to the database file can modify them.

## Repository layout

```
cmd/sentrygate     HTTP ingress (handler.go) and wiring (main.go)
cmd/worker         Temporal worker
cmd/agent-sim      Scenario simulator with -check
internal/policy    Policy schema, validation, loader
internal/decision  Evaluation, request hash, decision store interface
internal/auth      Agent keyring and middleware
internal/audit     SQLite store (decisions + workflow audit)
proxy/             Throttled evaluator; simulated F5/Zscaler clients (tests only)
workflows/         Re-validation, adapter contract, simulated adapter, reconciliation, audit
shared/contracts   Wire and record types
policies/          Policy file
```

More detail: [ARCHITECTURE.md](ARCHITECTURE.md), [SECURITY.md](SECURITY.md), [INTEGRATION.md](INTEGRATION.md), [CHANGELOG.md](CHANGELOG.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
