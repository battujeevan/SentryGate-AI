# Architecture

## Purpose

SentryGate is a policy gate for agent-proposed infrastructure changes. It answers one question deterministically: *is this authenticated agent permitted to issue this command against this registered target, under the current policy?* It records the answer and executes only an `ALLOW`, through a workflow that asks the question again immediately before acting.

It does not decide whether a change is a good idea. See [Limitations](README.md#limitations).

## Components and trust boundaries

```
                 untrusted                 │             trusted computing base
                                           │
 Agent ──HTTP──► proxy (cmd/sentrygate) ───┼──► Temporal ──► worker (cmd/worker)
                   │ auth, decode,         │     (no auth       │ re-validate
                   │ evaluate, record      │      in compose)   │ simulated dispatch
                   ▼                       │                    ▼
               SQLite (shared volume) ◄────┼────────────────────┘
```

- **Untrusted:** agents and everything they send. Identity comes only from the API key; nothing in the request body affects trust.
- **Trusted:** Temporal and its database, the worker, the audit volume, and the policy file. Temporal is not authenticated in the Compose stack, so a caller who can reach it can start workflows with any agent ID. Workflow re-validation still applies the policy to such submissions, but it cannot authenticate them. Compose binds every port to `127.0.0.1` for this reason.

## Request lifecycle

1. **Authenticate.** `internal/auth` hashes the `X-API-Key` header and compares it in constant time against every configured key. The loop never exits early. A match yields the agent ID, which is placed in the request context. No match gives `401`.
2. **Decode.** The body is limited to 64 KiB (`413` if larger). It must be exactly one JSON object with only the fields `id`, `type`, `target_id` and `payload`, and `id` must match `^[A-Za-z0-9._:-]{1,128}$`. Violations get `400` and a log line with the agent ID. They are not decision records, because they are not decisions.
3. **Evaluate.** `internal/decision.Evaluate` runs against the policy snapshot current at that moment (see the rules below). This is pure: no I/O.
4. **Record.** An ingress `DecisionRecord` is written to SQLite with a random decision ID.
   - For `ALLOW`, a failed write returns `503` and no workflow starts.
   - For `DENY` and `REQUIRE_APPROVAL`, a failed write is logged, the response omits `decision_id`, and the request stays refused.
5. **Respond or start.** `DENY` returns `403 rejected`. `REQUIRE_APPROVAL` returns `403 not_executed`. `ALLOW` starts workflow `saga-<id>` with an `ExecutionRequest{agent_id, proposal, ingress_decision_id, request_hash}`.
   - The start sets `WorkflowExecutionErrorWhenAlreadyStarted`. If a workflow with that ID is already running, the response is `409 not_executed` and the running execution is not reported as this request's run.
   - Any other start error returns a generic `502`. The start may still have succeeded, so the outcome is unknown to the caller.
   - The ingress record's `workflow_id` is the workflow a start was attempted for. Whether this request actually ran is shown by a `WORKFLOW_REVALIDATION` record with the same request hash.
   - A request cancelled before evaluation gets `503` and no record.

The HTTP server uses a 5s header timeout, a 30s read timeout and a 120s idle timeout. When the read timeout expires, `net/http` also cancels the request context, so 30s is also the upper bound on handler work. It leaves room for the SQLite busy timeout (5s) and the Temporal start RPC (10s SDK default). There is no write timeout, because it could cut off the response to a workflow that had already started.

## Decision rules

Evaluated in this order. Every applicable deny reason is collected:

| # | Check | Reason code |
|---|---|---|
| 1 | Agent declared in policy | `AGENT_UNKNOWN` |
| 2 | Command in catalogue | `COMMAND_UNKNOWN` |
| 3 | Command permitted for this agent (only checked when 1 and 2 pass) | `COMMAND_NOT_PERMITTED_FOR_AGENT` |
| 4 | Target registered | `TARGET_UNREGISTERED` |
| 5 | Target not protected | `TARGET_PROTECTED` |
| 6 | Environment rule `deny` | `ENVIRONMENT_DENIED` |

If any deny reason applies, the verdict is `DENY` and those reasons are returned. Otherwise the target environment's rule decides: `require_approval` gives `REQUIRE_APPROVAL` with `APPROVAL_REQUIRED`, and `allow` gives `ALLOW` with `ENVIRONMENT_ALLOWED`. A missing policy gives `DENY` with `POLICY_UNAVAILABLE`.

## Policy lifecycle

`internal/policy` decodes YAML with `KnownFields(true)`, requires exactly one document, and validates every field. Nothing has a default, and `protected` must be explicit. The resulting `Snapshot` is immutable and carries the operator's `version` and the SHA-256 `digest` of the raw bytes.

`Loader` re-reads the file on an interval and compares digests, so a change is detected regardless of modification time or which field changed. A valid new file is swapped in atomically.
- **Invalid file:** it's rejected and the active snapshot is kept. The error is logged once per distinct content and exposed in `GET /v1/policy` as `reload_status: error`.
- **Invalid file at startup:** the process exits.
- **`max_parallel_tasks`:** sizes the proxy's evaluation slot pool at startup; a change is logged but not applied.

The proxy and the worker each run their own loader over the same file.

## Workflow

`SentryGateSagaWorkflow(ExecutionRequest)` runs in this order:

1. **Re-validate.** Activity `EvaluateExecution` evaluates the request against the worker's current snapshot and recomputes the request hash. It runs in an activity so the workflow never reads mutable state, and its result is stored in history.
   - A hash mismatch adds `REQUEST_HASH_MISMATCH` and forces `DENY`.
   - The result is written as a `WORKFLOW_REVALIDATION` decision record with ID `wf_<workflow_id>_<run_id>`. The ID and timestamp come from workflow state, so a retried write is idempotent.
   - Anything other than `ALLOW` writes a `WORKFLOW_REVALIDATION FAIL` audit row and fails with the non-retryable application error type `REVALIDATION_DENIED`. No infrastructure activity is scheduled.
   - On `ALLOW`, if the decision record cannot be written, the workflow fails without dispatching.
2. **Dispatch.** The simulated `DispatchConfig` runs once.
3. **Compensation.** On dispatch failure, the simulated `RevertStateCompensation` runs once in a disconnected context. The result is recorded; it is best effort, not atomic, and not idempotent.
4. **Audit.** Each phase appends a row keyed by proposal, run, phase and sequence.

This closes two gaps: a proposal submitted straight to Temporal, and a policy tightened between ingress and execution. In both cases the stricter answer wins.

## Records

| Table | Written by | Contents |
|---|---|---|
| `decision_records` | proxy (stage `INGRESS`), worker (stage `WORKFLOW_REVALIDATION`) | decision ID, stage, proposal ID, agent ID, request hash, command, target, environment, verdict, reasons, policy version, policy digest, workflow ID, trace ID, timestamp |
| `audit_records` | worker | one row per workflow phase |

Inserts use `ON CONFLICT DO NOTHING` and then compare content, so an identical retry succeeds and a conflicting reuse of an ID is an error. The database runs in WAL mode with a 5s busy timeout because two processes share it. There is no update or delete path in code; rows are not tamper-evident.

### Request hash

`RequestHash` is the lowercase hex SHA-256 of:

```
"sentrygate.request.v1" || for f in [id, type, target_id, payload]: uint64_be(len(f)) || f
```

It is computed over decoded values, so JSON whitespace and key order do not change it. The length prefixes stop bytes shifting between fields from producing the same hash. `payload` is an opaque string: any change to it changes the hash. The hash ties the executed proposal to the ingress record. It is not authentication: anyone who can submit to Temporal can compute it.

## Package boundaries

| Package | Responsibility |
|---|---|
| `shared/contracts` | Wire types (`AgentProposal`, `ExecutionRequest`), verdicts, reason codes, record types |
| `internal/policy` | Schema, strict parsing, `Snapshot`, `Loader`, `Source` |
| `internal/decision` | `Evaluate`, `Revalidate`, `RequestHash`, `Store` interface, in-memory store |
| `internal/auth` | `Keyring`, middleware, identity in context |
| `internal/audit` | SQLite store implementing both record tables |
| `proxy` | Evaluation slot pool around `decision.Evaluate`; simulated vendor clients (tests only) |
| `workflows` | Workflow, re-validation activities, simulated infrastructure activities, audit activity |
| `cmd/sentrygate` | `newHandler` (HTTP) and process wiring |
| `cmd/worker` | Worker wiring and health endpoint |
| `cmd/agent-sim` | Scenario runner with `-check` |

## Observability

- **Recorded vs logged.** Every decision that is written goes to `decision_records`, at both stages. This is the evidence trail. Structured JSON logs (`log/slog`) are operational output. The proxy logs each ingress decision with decision ID, agent ID, verdict, reasons and policy version, along with malformed requests and record-write failures. The worker's re-validation outcome is recorded and visible in Temporal's workflow history, but the worker does not emit a structured log line for it. Keys are never logged.
- **Tracing.** HTTP requests are wrapped in OpenTelemetry spans. `trace_id` on an ingress record is the trace ID of that request's span, if one exists. Compose sets `OTEL_EXPORTER=none`, which installs a no-op tracer provider, so `trace_id` is empty there. With `stdout` (the default outside Compose), spans are exported to stdout and `trace_id` is filled. No propagator is configured, so an incoming `traceparent` header is not honoured. Re-validation records have no trace ID.
- `/healthz`, `/readyz` on the proxy (`:8080`) and worker (`:8081`); Temporal UI on `127.0.0.1:8088` in Compose.
