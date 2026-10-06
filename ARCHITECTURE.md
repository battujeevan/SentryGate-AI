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
- **Trusted:** Temporal and its database, the worker, the audit volume, and the policy file. Temporal is not authenticated in the Compose stack, so a caller who can reach it can start workflows. Such a workflow dispatches only if it matches a recorded ingress `ALLOW` (agent, request hash, proposal ID and workflow ID); otherwise it fails before re-validation. Each ingress decision can be claimed for at most one dispatch, so re-running a legitimate execution does not dispatch again. Anyone who can write the SQLite database can still forge an ingress record or reset a claim. Compose binds every port to `127.0.0.1` for this reason.

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

1. **Verify the ingress decision.** Activity `VerifyIngressDecision` loads the record named by `ingress_decision_id` and requires all of the following. The workflow ID is taken from Temporal's activity info, not from the input.
   - The record exists (`INGRESS_DECISION_NOT_FOUND`).
   - Its stage is `INGRESS` (`INGRESS_DECISION_WRONG_STAGE`).
   - Its verdict is `ALLOW` (`INGRESS_DECISION_NOT_ALLOW`).
   - Its agent ID equals the request's agent ID (`INGRESS_DECISION_AGENT_MISMATCH`).
   - Its request hash equals the hash recomputed from the proposal (`INGRESS_DECISION_HASH_MISMATCH`).
   - Its proposal ID and workflow ID equal the proposal's ID and the running workflow's ID (`INGRESS_DECISION_IDENTITY_MISMATCH`).

   A failed check writes a `WORKFLOW_REVALIDATION` `DENY` record with that reason and a `WORKFLOW_REVALIDATION FAIL` audit row, then fails with the non-retryable application error type `INGRESS_DECISION_INVALID`, whose details carry the reason code. If the record cannot be read, the workflow fails with the same error type and no reason. Policy re-validation and infrastructure activities do not run in either case.
2. **Claim the decision.** Activity `ClaimExecution` claims the ingress decision for this run (see [Execution claims](#execution-claims)). If another execution holds it, or has executed it, the workflow writes a `DENY` record with `INGRESS_DECISION_ALREADY_CLAIMED` and a FAIL audit row, then fails with the non-retryable application error type `EXECUTION_CLAIM_REJECTED`, whose details carry that reason. If the claim store fails, the workflow fails with the same error type and no reason. Re-validation and infrastructure activities do not run in either case.
3. **Re-validate.** Activity `EvaluateExecution` evaluates the request against the worker's current snapshot and recomputes the request hash. It runs in an activity so the workflow never reads mutable state, and its result is stored in history.
   - A hash mismatch adds `REQUEST_HASH_MISMATCH` and forces `DENY`.
   - The result is written as a `WORKFLOW_REVALIDATION` decision record with ID `wf_<workflow_id>_<run_id>`. The ID and timestamp come from workflow state, so a retried write is idempotent.
   - Anything other than `ALLOW` writes a `WORKFLOW_REVALIDATION FAIL` audit row and fails with the non-retryable application error type `REVALIDATION_DENIED`. No infrastructure activity is scheduled.
   - On `ALLOW`, if the decision record cannot be written, the workflow fails without dispatching.
4. **Execution fence.** The workflow schedules `DispatchConfig` with the claim still `CLAIMED`. Immediately before calling the adapter, the activity moves the claim from `CLAIMED` to `EXECUTING` as the owner it reads from its own activity info, and calls the adapter only if that compare-and-set succeeds. If the fence is not acquired, the adapter is not called; see [The execution fence](#the-execution-fence).
5. **Dispatch.** `DispatchConfig` runs once, never retried, with the decision's idempotency key, and reports an outcome. See [Dispatch outcomes](#dispatch-outcomes-and-reconciliation).
   - `SUCCESS`: the claim moves to `COMPLETED`.
   - `FAILURE`: the claim moves to `FAILED`.
   - `UNKNOWN`: the claim moves to `RECONCILIATION_REQUIRED` and the workflow reconciles. Any dispatch activity error, including a timeout, counts as `UNKNOWN`.
6. **Compensation.** `RevertStateCompensation` runs once, in a disconnected context, only when the adapter confirmed a `FAILURE` with `partially_applied`: the target reports that part of the change took effect. It never runs for `UNKNOWN`, and not for a plain `FAILURE`, which means nothing took effect. The result is recorded; it is best effort, not atomic, and not idempotent.
7. **Audit.** Each phase appends a row keyed by proposal, run, phase and sequence. From step 4 on, audit and claim writes use a disconnected context, so a dispatch outcome is recorded even if the workflow is cancelled.

Step 1 means a workflow started straight in Temporal cannot dispatch unless the proxy recorded an `ALLOW` for that authenticated agent, that exact proposal content and that workflow ID. Step 2 means that ingress decision authorizes at most one dispatch, however many workflows are started with it. Step 3 covers a policy tightened between ingress and execution.

### Execution claims

Each ingress `ALLOW` has at most one row in `execution_claims`, keyed by decision ID, which references `decision_records`. A decision with no row is unclaimed.

```
(unclaimed) ──claim──► CLAIMED ──fence──► EXECUTING ──SUCCESS──► COMPLETED
                         │  ▲               │  └─────FAILURE──► FAILED
                 release │  │ claim         │ UNKNOWN
                         ▼  │               ▼
                       RELEASED        RECONCILIATION_REQUIRED ──SUCCESS──► COMPLETED
                                                            └───FAILURE──► FAILED
```

| Transition | Who | Allowed from |
|---|---|---|
| claim | any run that passed ingress verification | no row, `RELEASED` (or `CLAIMED` by the same owner, as a retry) |
| → `EXECUTING` (the fence) | owner's `DispatchConfig` activity, immediately before the adapter call | `CLAIMED` only |
| → `RECONCILIATION_REQUIRED` | owner, when the dispatch outcome is `UNKNOWN` | `EXECUTING`, `RECONCILIATION_REQUIRED` |
| → `COMPLETED` | owner, on a confirmed `SUCCESS` | `EXECUTING`, `RECONCILIATION_REQUIRED`, `COMPLETED` |
| → `FAILED` | owner, on a confirmed `FAILURE` | `EXECUTING`, `RECONCILIATION_REQUIRED`, `FAILED` |
| → `RELEASED` | owner, on any exit before the fence commits | `CLAIMED`, `RELEASED` |

- **Owner.** The owner is the triple (workflow ID, run ID, token). The workflow ID and run ID are read from Temporal's activity info. The token is generated once per run with `workflow.SideEffect`, so it is stored in history and is the same after a worker restart or replay. Every transition checks all three, so another run cannot advance or release a claim it does not own.
- **Temporal reset.** SentryGate does not prevent a reset: anyone who can reach Temporal can reset a workflow (see [SECURITY.md](SECURITY.md)). A reset run keeps the workflow ID and inherits the token from history, but gets a new run ID. It cannot claim, advance or release a claim that the original run still owns. If its history already shows the claim as taken, it goes on to schedule `DispatchConfig`, whose fence fails for the new run ID, so the adapter is not called and the run fails with `EXECUTION_CLAIM_REJECTED`. The reset terminates the original run, so a claim it held is left with an owner that will never resolve it (see the crash behaviour below).
- **Atomicity.** Each operation is one SQL statement. Claiming is an `INSERT … ON CONFLICT(decision_id) DO UPDATE … WHERE state = 'RELEASED'`, so it either creates the row, takes over a released one, or changes nothing. Transitions are `UPDATE … WHERE` owner `AND state IN (…)`. SQLite serializes writers across connections and processes, so two executions cannot both succeed. The row count shows whether this caller won; a follow-up read only distinguishes an idempotent retry from a claim held by someone else.
- **Self-transitions** let Temporal retry an activity whose write committed but whose result was lost, for example because the worker crashed. The retry presents the same owner and succeeds without changing anything. The fence is the exception: `EXECUTING` can be entered only from `CLAIMED`, so a second attempt, even by the owner, cannot pass it.
- **`CLAIMED` and `RELEASED`** guarantee that the adapter has not been called, so a claim can safely be released and taken over. The workflow releases its claim, from a disconnected context, on every exit before dispatch is scheduled (re-validation denied or unavailable, a failed record or audit write, cancellation) and when `DispatchConfig` reports that the fence was not acquired. A later run of the same decision must still pass ingress verification and re-validation.
- **`EXECUTING`, `RECONCILIATION_REQUIRED`, `COMPLETED` and `FAILED`** are never released or claimed again. `EXECUTING` is written by the dispatch activity immediately before it calls the adapter, so it means the adapter call may have started; a crash during dispatch, or a failed `COMPLETED` write, leaves the decision permanently used. `COMPLETED` and `FAILED` record a confirmed outcome. `RECONCILIATION_REQUIRED` records that the outcome is not known; it is not terminal and moves to `COMPLETED` or `FAILED` when reconciliation confirms one.

#### The execution fence

No adapter call happens unless the `DispatchConfig` activity that makes it has just moved the claim from `CLAIMED` to `EXECUTING` as its own run.

- **Owner.** The activity takes the decision ID, the claim token and the dispatch request as input, but builds the owner from `activity.GetInfo(ctx)` (workflow ID and run ID) plus the token. It does not accept a workflow or run ID from its input. It also refuses a request whose idempotency key is not `decision.ExecutionKey` of the fenced decision.
- **Fence.** The fence is the store's owner-checked compare-and-set on `CLAIMED`. It is not repeatable, so of any number of concurrent or repeated attempts, at most one succeeds.
- **Failure.** If the fence is not acquired, the activity returns a non-retryable application error without calling the adapter, so a Temporal retry cannot bypass it. There are two error types:
  - `EXECUTION_FENCE_REJECTED`: the store definitively refused the transition (another owner, a claim in another state, or no claim), or the request was invalid. The claim was not changed by this attempt.
  - `EXECUTION_FENCE_UNKNOWN`: the fence write failed and may have committed (for example a lost response), the claim could not be read back, or the claim is held by this run's own owner in a state other than `RELEASED`.
- **Workflow handling.** On either error the workflow tries to release the claim. Release is a compare-and-set on `CLAIMED` by this owner, so it succeeds only if the fence did not commit.
  - On `EXECUTION_FENCE_REJECTED`, or when the release succeeds, the workflow writes an `EXECUTION_FENCE FAIL` audit row (`adapter not called`, whether the claim was released) and fails with `EXECUTION_CLAIM_REJECTED`.
  - On `EXECUTION_FENCE_UNKNOWN` when the release fails, the claim may be `EXECUTING`, so it is not treated as released. The workflow writes an `EXECUTION_FENCE UNKNOWN` audit row, records the outcome as `UNKNOWN` and reconciles with the same key. The adapter was not called, so with the simulated adapter reconciliation reports `FAILURE` and the claim moves to `FAILED`. If the claim was in fact still `CLAIMED` (the fence write and the release both failed), the later transitions fail and it stays `CLAIMED`: nothing was dispatched, but the decision cannot be used again.
- **Audit.** No `DISPATCH_CONFIG` row is written unless the activity returned without a fence error. An `EXECUTION_FENCE` row is written only when the fence was not acquired, and never claims that the adapter ran.

**Crash behaviour.** A worker crash does not end a Temporal run. Another worker replays the run's history, so completed activities, including the claim and the token, are not repeated. An activity that was in flight times out and is retried under the same owner. Dispatch has `MaximumAttempts: 1`, so a timed-out dispatch is not retried: its outcome is `UNKNOWN` and the workflow reconciles. Reconciliation waits are durable timers, so a restart neither repeats nor resets the schedule.

Some states cannot be recovered with the same decision:
- A `CLAIMED` row whose run ended without releasing it: the run was terminated or reset, every release attempt failed, or the dispatch activity was lost before its fence committed. In the last case the workflow treats the outcome as `UNKNOWN`, but its transitions from `CLAIMED` fail. The adapter was not called, but the decision cannot be used again; the change must be resubmitted through the proxy to get a new decision.
- An `EXECUTING` row whose run was terminated or reset. The adapter call may have started, and nothing reconciles it.

### Dispatch outcomes and reconciliation

**An unknown outcome is not equivalent to failure.** A timeout, a reset connection or a lost worker means the workflow did not learn the result; the change may still have reached the target and taken effect. So `UNKNOWN` never releases the claim, never dispatches again and never compensates.

| Outcome | Meaning | Claim | Compensation |
|---|---|---|---|
| `SUCCESS` | target confirmed the change took effect | `COMPLETED` | no |
| `FAILURE` | target confirmed the change did not take effect and no longer can | `FAILED` | no |
| `FAILURE` with `partially_applied` | target confirmed part of the change took effect, then it failed | `FAILED` | once |
| `UNKNOWN` | not known whether the change took effect | `RECONCILIATION_REQUIRED` | no |

The workflow treats any dispatch activity error (timeout, cancellation, worker loss) and any status it does not recognise as `UNKNOWN`. Only an outcome the adapter returns explicitly can be `SUCCESS` or `FAILURE`.

- **Idempotency key.** `decision.ExecutionKey(ingress_decision_id)`, for example `sentrygate.exec.v1:dec_…`. It depends only on the ingress decision, not on the run or claim token, so dispatch, every reconciliation attempt, every activity retry and any later run of the same decision present the same key. A target that honours idempotency keys therefore applies the change at most once even if it receives it twice.
- **Reconciliation.** The owning run calls `ReconcileDispatch` with the same request and key. It asks immediately, then after waits that double from 10s up to 30 minutes, for up to 24 hours. Reconciliation activities may be retried, because they never dispatch. A confirmed `SUCCESS` or `FAILURE` resolves the claim as in the table. If the outcome is still `UNKNOWN` at the end of the window, or the workflow is cancelled while reconciling, the workflow fails with the non-retryable application error type `DISPATCH_OUTCOME_UNKNOWN`, whose details carry the idempotency key, and the claim stays in `RECONCILIATION_REQUIRED`. Nothing resolves it after that yet (see [Limitations](README.md#limitations)).
- **Adapter contract** (`workflows.Adapter`). `Dispatch` must report `SUCCESS` or `FAILURE` only when the target confirmed it, and must apply a key at most once. `Reconcile` must not apply anything; it may report `FAILURE` only if the change has not taken effect and can no longer take effect, for example because the target now rejects the key, since otherwise a request still in flight could land after `FAILED` was recorded.
- **Audit.** The `DISPATCH_CONFIG` row (or, if the fence outcome was unknown, an `EXECUTION_FENCE UNKNOWN` row) and, when reconciliation ran, a `DISPATCH_RECONCILIATION` row carry verdict `PASS`, `FAIL` or `UNKNOWN`, with the outcome, the key and the adapter's detail.

**The simulated adapter** (`workflows.SimulatedAdapter`) is the only implementation. It changes nothing; its answers depend only on the target ID and on the keys it has seen. It records each key it applies and returns that record for a repeated dispatch or a reconciliation; reconciling a key it has never seen fences the key and reports `FAILURE`. That record is in process memory: it is lost on worker restart and not shared between workers. Fixed targets exercise each path:

| Target | Dispatch | Reconcile |
|---|---|---|
| any other registered target | `SUCCESS` | `SUCCESS` |
| `FAILING_NODE` | `FAILURE`, partially applied | same |
| `LOST_RESPONSE_NODE` | `UNKNOWN` (applied, response lost) | `SUCCESS` |
| `UNREACHABLE_NODE` | `UNKNOWN` (never arrived) | `FAILURE` (key fenced) |
| `PARTITIONED_NODE` | `UNKNOWN` | `UNKNOWN` |

## Records

| Table | Written by | Contents |
|---|---|---|
| `decision_records` | proxy (stage `INGRESS`), worker (stage `WORKFLOW_REVALIDATION`) | decision ID, stage, proposal ID, agent ID, request hash, command, target, environment, verdict, reasons, policy version, policy digest, workflow ID, trace ID, timestamp |
| `audit_records` | worker | one row per workflow phase |
| `execution_claims` | worker | decision ID, owner (workflow ID, run ID, token), claim state, claimed and updated timestamps |

Decision and audit inserts use `ON CONFLICT DO NOTHING` and then compare content, so an identical retry succeeds and a conflicting reuse of an ID is an error. Decision and audit rows have no update or delete path in code. Claim rows change only through the conditional statements described under [Execution claims](#execution-claims). The database runs in WAL mode with a 5s busy timeout because two processes share it, and with foreign keys enforced. Rows are not tamper-evident.

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
| `internal/decision` | `Evaluate`, `Revalidate`, `RequestHash`, ingress verification, `Store` and `ClaimStore` interfaces and the claim transition table, in-memory store |
| `internal/auth` | `Keyring`, middleware, identity in context |
| `internal/audit` | SQLite store implementing both record tables and execution claims |
| `proxy` | Evaluation slot pool around `decision.Evaluate`; simulated vendor clients (tests only) |
| `workflows` | Workflow, re-validation activities, `Adapter` contract and infrastructure activities, simulated adapter, audit activity |
| `cmd/sentrygate` | `newHandler` (HTTP) and process wiring |
| `cmd/worker` | Worker wiring and health endpoint |
| `cmd/agent-sim` | Scenario runner with `-check` |

## Observability

- **Recorded vs logged.** Every decision that is written goes to `decision_records`, at both stages. This is the evidence trail. Structured JSON logs (`log/slog`) are operational output. The proxy logs each ingress decision with decision ID, agent ID, verdict, reasons and policy version, along with malformed requests and record-write failures. The worker's re-validation outcome is recorded and visible in Temporal's workflow history, but the worker does not emit a structured log line for it. Keys are never logged.
- **Tracing.** HTTP requests are wrapped in OpenTelemetry spans. `trace_id` on an ingress record is the trace ID of that request's span, if one exists. Compose sets `OTEL_EXPORTER=none`, which installs a no-op tracer provider, so `trace_id` is empty there. With `stdout` (the default outside Compose), spans are exported to stdout and `trace_id` is filled. No propagator is configured, so an incoming `traceparent` header is not honoured. Re-validation records have no trace ID.
- `/healthz`, `/readyz` on the proxy (`:8080`) and worker (`:8081`); Temporal UI on `127.0.0.1:8088` in Compose.
