# Integration notes

## Calling SentryGate from an agent (implemented)

An agent's tool implementation sends the proposed change to the proxy instead of calling infrastructure directly:

```http
POST /v1/intercept
X-API-Key: <this agent's key>
Content-Type: application/json

{"id":"chg-2026-0001","type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"{\"route\":\"stable\"}"}
```

- `id` is chosen by the caller and must match `^[A-Za-z0-9._:-]{1,128}$`. Use a fresh ID per proposal. Re-submitting an ID is not deduplicated (see Limitations).
- `payload` is an opaque string, and it is covered by the request hash.
- Do not send any other field; unknown fields are rejected with `400`.

Handle the response by status:

| Status | `status` | Agent should |
|---|---|---|
| `200` | `accepted` | Poll `GET /v1/audit/{id}` for `WORKFLOW_COMPLETE` or `WORKFLOW_FAILED`. A `DISPATCH_CONFIG` or `EXECUTION_FENCE` row with verdict `UNKNOWN` means the outcome is being reconciled; an `EXECUTION_FENCE` row with verdict `FAIL` means the change was not dispatched; a final `DISPATCH_RECONCILIATION` row with verdict `UNKNOWN` means it is still unknown and needs an operator. Do not resubmit in that case: the change may have been applied |
| `403` | `rejected` | Treat as a hard refusal; `reasons` explains why |
| `403` | `not_executed` | Stop. The change needs human approval, which SentryGate does not yet provide |
| `400` / `413` | `invalid` | Fix the request; do not retry unchanged |
| `401` | `unauthorized` | Configuration error |
| `409` | `not_executed` | A workflow for this `id` is already running, so this request was not executed. The `ALLOW` decision is recorded. Resubmit with a fresh `id` if the change is still wanted |
| `502` | `error` | The workflow-start outcome is unknown: the start may have succeeded even though the response failed. Check the recorded state (below) before retrying |
| `503` | `unavailable` | Nothing was started: the request was cancelled before evaluation, or an `ALLOW` could not be recorded |

After a `502`, `GET /v1/decisions/{id}` shows what was recorded. The ingress record carries the `workflow_id` a start was attempted for. The worker writes a `WORKFLOW_REVALIDATION` record, with the same `request_hash`, before it can dispatch anything; if that record is present with verdict `ALLOW`, your request's workflow started and passed re-validation, and `GET /v1/audit/{id}` shows the outcome. If it is absent, nothing has been dispatched yet, but a start that did succeed may still be about to run, so check again before resubmitting. A retry with the same `id` gets `409` while that workflow is running. Once it has finished, a retry is a new submission with a new decision, so it may execute a second time. Re-running the original decision cannot.

Example `403` body:

```json
{"status":"rejected","verdict":"DENY","reasons":["TARGET_PROTECTED"],"decision_id":"dec_…","proposal_id":"chg-2026-0001"}
```

`GET /v1/decisions/{id}` returns the ingress record and, after execution starts, the workflow re-validation record.

## Adding an agent

1. Declare it in the policy with the commands it may issue:
   ```yaml
   agents:
     - id: deploy-bot
       commands: [MODIFY_ROUTING]
   ```
2. Give it a key in `SENTRYGATE_AGENT_KEYS` (`deploy-bot:<key>`) and restart the proxy. Keys are read at startup only.

A key for an agent that is not declared in the policy authenticates, but every request from it is denied with `AGENT_UNKNOWN`, and the proxy logs a warning at startup.

## Adding a target or command

Add it to `targets` or `commands` in the policy file. Both the proxy and the worker pick up the change on their next reload. Check `GET /v1/policy` for the new `version` and `digest`, and for `reload_status: ok`.

## Infrastructure adapters

**Contract (implemented).** An adapter implements `workflows.Adapter`, which `workflows.InfrastructureActivities` runs on the worker after re-validation:

```go
Dispatch(ctx, contracts.DispatchRequest) contracts.DispatchOutcome
Reconcile(ctx, contracts.DispatchRequest) contracts.DispatchOutcome
Compensate(ctx, contracts.DispatchRequest) error
```

`DispatchRequest` carries the proposal and an idempotency key derived from the ingress decision (`sentrygate.exec.v1:<decision_id>`). The key is the same for dispatch and every reconciliation of that execution. An adapter must:

- report `SUCCESS` or `FAILURE` only when the target confirmed it, and `UNKNOWN` for anything ambiguous (timeouts, reset connections, responses that do not say whether the change was made). An unknown outcome is not equivalent to failure.
- pass the key to the target so a repeated request is not applied twice.
- in `Reconcile`, look the key up on the target without applying anything, and report `FAILURE` only if the change has not taken effect and can no longer take effect (for example, the target now rejects the key).
- set `PartiallyApplied` on a `FAILURE` only when the target confirmed that part of the change took effect; that is the only case in which the workflow compensates.

**Simulated today.** `workflows.SimulatedAdapter` is the only implementation and touches nothing. It answers per target: success by default, `FAILING_NODE` partly applies and fails, `LOST_RESPONSE_NODE` applies but loses the response, `UNREACHABLE_NODE` never receives the request, and `PARTITIONED_NODE` never answers. It remembers keys in process memory only. The F5 BIG-IP and Zscaler clients in `proxy/clients.go` are in-memory test doubles; they are not used by the workflow and make no network calls.

**Planned.** Before connecting a real adapter to real infrastructure it also needs:

- a target-side idempotency or lookup mechanism that the adapter's `Dispatch` and `Reconcile` actually use
- a captured pre-image, so compensation restores the prior state rather than a hard-coded "stable" state
- retryable compensation with an operator-escalation state
- a way to resolve claims still in `RECONCILIATION_REQUIRED` after the reconciliation window
- per-target serialization

None of these exist yet. The proxy should stay free of vendor SDK code.

## Limitations relevant to integrators

- Re-submitting the same proposal `id` creates a new decision. While the previous workflow is running, an `ALLOW` gets `409` and is not executed. If the previous workflow has finished, the new decision may start a new run and execute. Each decision executes at most once.
- `REQUIRE_APPROVAL` cannot be approved yet.
- See [SECURITY.md](SECURITY.md) for the trusted computing base.
