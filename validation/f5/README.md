# F5 AI Gateway / MCP Gateway: independent validation harness

## Purpose

This harness tests whether an operation that an AI agent was authorized to perform is the operation that actually runs against infrastructure, and that it runs at most once. It is built to validate **F5 AI Gateway / F5 MCP Gateway** as the system under test, with **SentryGate-AI** as an independent, complementary control and the infrastructure's own record as the ground truth.

It is not a security product and does not model, emulate or predict F5. No F5 component is included. Until F5 is placed in the request path and a real observation provider is connected, every F5 result is `NOT_TESTED`, and the report says so.

## Scope

In scope: the authorization-to-execution path of MCP tool calls. Twelve tests (TC01-TC12):

| Test | Scenario |
| --- | --- |
| TC01 | Legitimate read and update (baseline) |
| TC02 | Tool substitution |
| TC03 | Target substitution, including target smuggling in arguments |
| TC04 | Request (argument) mutation |
| TC05 | Policy change V1 to V2 between authorization and execution |
| TC06 | Authorization replay |
| TC07 | Concurrent execution |
| TC08 | Workflow replay and reset (after completion and during dispatch) |
| TC09 | Unknown outcome: applied, response lost |
| TC10 | Forged authorization |
| TC11 | Identity substitution |
| TC12 | Privilege escalation |

The definitions (objective, method, expected result, expected mutation count) are in `validation/cases/tc.go` and are copied into each test's evidence.

Out of scope: prompt injection, content inspection, model behavior, performance, production deployments.

## Prerequisites

- Go, at the version in `go.mod`.
- Network access on the first run, to download the Temporal CLI that serves as the dev server. The Temporal Go SDK downloads it into the user cache directory (`sentrygate-validation/temporal-cli`); set `TEMPORAL_CLI_PATH` to use an existing binary instead.
- No Docker and no C toolchain are needed.
- `make` is optional. On Windows, use the `go run` commands below.

## Architecture

```text
deterministic test agent (validation/agent)
   |  gateway path: POST /v1/intercept with the agent's API key
   v
[insertion point A: F5 AI Gateway in front of SentryGate ingress]   <- not present by default
   v
SentryGate proxy (cmd/sentrygate)
   v
Temporal dev server   <- execution-boundary path: the agent starts, terminates or resets workflows directly
   v
SentryGate worker (cmd/worker, SENTRYGATE_ADAPTER=mcp)
   v
[insertion point B: F5 MCP Gateway in front of the MCP server]      <- not present by default
   v
test MCP server (validation/cmd/mcp-test-server)
```

- **Test MCP server** (`validation/mcpserver`): an MCP server built on the official Go MCP SDK, with tools `read_customer`, `update_customer`, `export_customer` and `delete_customer` over a disposable SQLite database. Every `tools/call` is recorded (execution ID, timestamp, agent ID, tool, target, request, request hash, SentryGate request hash, idempotency key, result, mutation count) and **nothing is deduplicated**, so the record shows whether an operation happened 0, 1 or more times. Fault injection: calls to `customer-lost-response` are applied and then the connection is closed without a response; calls to `customer-gated` wait until `POST /gate/release`. A status tool, `sentrygate_execution_status`, reports whether an idempotency key was applied and can fence a key that was never seen.
- **Deterministic agent** (`validation/agent`): no LLM. It sends the exact legitimate, modified-tool, modified-target, modified-arguments, modified-identity, replayed and concurrent requests each test defines, through ingress (gateway path) or directly to Temporal (execution-boundary path, modelling an attacker who can reach the workflow engine).
- **F5 observation boundary** (`validation/f5`): the `F5ObservationProvider` interface returns, per gateway request, `request_id`, `agent_identity`, `tool`, `target`, `authorization_decision`, `policy_identifier`, `timestamp`, `audit_event`, `downstream_request_reference` and `raw_evidence_reference`. The only implementation, `MockProvider`, sets every field to `MOCK` and the decision to `NOT_OBSERVED`. It never returns ALLOW or DENY.
- **SentryGate observation** (`validation/harness/observe.go`): agent identity, command/tool, target, environment, policy version and digest, request hash, ingress decision, re-validation decision, claim state, execution state, fence result, workflow and run IDs, reconciliation state and final outcome, read from SentryGate's audit store and Temporal history.
- **Harness** (`validation/harness`): builds the proxy, worker and test MCP server, starts the Temporal dev server once, and gives each test its own stack: free ports, task queue, audit database, MCP database, policy file and per-stack random agent keys (never written to evidence).

The only production changes the harness needs are already in the repository: `workflows.MCPAdapter`, opt-in adapter selection in the worker (`SENTRYGATE_ADAPTER=mcp`, `SENTRYGATE_MCP_URL`, `SENTRYGATE_MCP_TARGET_ARGUMENT`), and `AgentID` in `contracts.DispatchRequest`. The default adapter remains `simulated`.

## Environment

| Item | Value |
| --- | --- |
| Policies | `validation/policies/validation-v1.yaml`, `validation-v2.yaml` (V2 removes UPDATE_CUSTOMER from agent-a) |
| Agents | `agent-a` (READ_CUSTOMER, UPDATE_CUSTOMER), `agent-b` (READ_CUSTOMER) |
| Targets | `customer-123`, `customer-456`, `customer-lost-response`, `customer-gated` (environment `test`, rule `allow`) |
| MCP target argument | `customer_id` (the adapter adds it from the authorized target and refuses arguments that set it) |
| Working files | `validation/run/` (binaries, databases, process logs; git-ignored, never evidence) |

## Comparison model

Every test is read on three layers:

1. **F5**: the decision F5 recorded for each gateway-path request (`f5_result`).
2. **SentryGate**: what SentryGate decided and did (`sentrygate_result`).
3. **Infrastructure**: what the test MCP server actually received and changed (`infrastructure_result`, `mutation_count`). This is the key metric.

`f5_result` is `NOT_TESTED` whenever an observation did not come from F5, `NOT_APPLICABLE` when a test sends no request of the relevant role through a gateway path, `UNKNOWN` when a real observation has no decision, and otherwise `PREVENTED` or `ALLOWED`. It is never inferred from SentryGate or the infrastructure.

Matrix `status`:

| Status | Meaning |
| --- | --- |
| VERIFIED | The test ran and every check passed. Describes the stack that was run; not an F5 result unless `f5_result` is. |
| FAILED | The test ran and at least one check failed. |
| BLOCKED | The test could not be carried out (environment or harness problem). |
| NOT_TESTED | No evidence. |
| UNKNOWN | Evidence insufficient, incomplete or inconsistent. |
| NOT_APPLICABLE | The test does not apply. |

## Baseline run (no F5)

```sh
make validation-f5
# or
go run ./validation/cmd/f5validate -tc all
go run ./validation/cmd/f5report
```

## Running one test

```sh
make validation-f5-tc05
# or
go run ./validation/cmd/f5validate -tc TC05
go run ./validation/cmd/f5validate -tc TC02,TC03
```

Running one test replaces that test's evidence only; the matrix is rebuilt from all stored evidence.

## Evidence

Each test writes `validation/evidence/TCxx/`:

| File | Content |
| --- | --- |
| `test-result.json` | Definition, status, layer results, every check (expected, observed, result), request summaries, notes, limitations, environment |
| `request.json` | Every request the agent sent (path, agent, proposal or raw body, Temporal request, response); never an API key |
| `authorization.json` | F5 observations per gateway request (with the provider and source) and SentryGate's ingress decisions |
| `sentrygate-decision.json` | SentryGate observation per workflow run, including audit trail, decision records, claim and history summary |
| `execution.json` | Every `tools/call` the test MCP server received, and counts |
| `infrastructure-state.json` | Customers, exports, fenced keys, transport events, mutation count against expectation |
| `before-state.json`, `after-state.json`, `execution-count.json` | Full database snapshots and counts |
| `supplementary-*.json` | Labeled supplementary material (TC07: output of the concurrent claim and fence component tests; not end-to-end evidence) |

## Report

```sh
make validation-report
# or
go run ./validation/cmd/f5report
```

writes `validation/results/validation-matrix.csv` and `validation/report/F5-AI-Gateway-Independent-Security-Validation.md` from the stored evidence only. The output does not depend on when it is generated. A test without evidence is `NOT_TESTED`; a test whose evidence is incomplete or whose stored status disagrees with its checks is `UNKNOWN`. The report uses fixed wording for F5: "NOT TESTED", "UNKNOWN — insufficient evidence", "VERIFIED — F5 prevented the tested scenario", and "F5 allowed the tested request; SentryGate independently blocked execution." The last two can only appear for a test whose observations came from F5.

## Connecting F5 later

Only the F5 observation layer and the network path change; the tests, the agent, SentryGate and the test MCP server stay as they are.

1. **Place F5 in the path.**
   - Insertion point A (gateway in front of ingress): route F5 to the SentryGate proxy and run with `-ingress-url <F5 URL>`. The agent then sends every gateway-path request to F5.
   - Insertion point B (MCP gateway in front of the MCP server): fix the test server's address with `-mcp-listen 127.0.0.1:8090`, route F5 to `http://127.0.0.1:8090/mcp`, and run with `-worker-mcp-url <F5 MCP URL>`.
   - The agent keys are generated per stack. If F5 must authenticate agents itself, the harness has to be extended to use keys F5 knows; it does not do this today.
2. **Implement a provider.** Add an `f5.F5ObservationProvider` that reads F5's records through F5's documented audit or logging interface and fills every `Observation` field from a real record, with `Source: "F5"`, the real `authorization_decision` (`ALLOW` or `DENY`, or `NOT_OBSERVED` if no record is found) and an empty `MockedFields`. Correlate using `Correlation` (test ID, request label, proposal ID, agent, tool, target, send time). The mapping from F5 records to these fields must be confirmed against real F5 output; nothing here assumes its format.
3. **Register it** in `validation/cmd/f5validate` under a new `-f5-provider` value.
4. **Re-run** the suite and the report.

Correlation today is per gateway-path request (insertion point A). If F5 sits only at insertion point B, its records describe the worker's `tools/call` requests; correlate them with the MCP server's `execution.json` (idempotency key and proposal ID are sent as MCP request metadata) and extend the runner to observe those calls.

## Limitations

- No F5 component has been tested with this harness.
- The test MCP server is a test fixture; its status tool exists so reconciliation can be exercised.
- One run per test; timing-dependent results hold for the observed run.
- Temporal is the single-process dev server.
- Process stops are abrupt kills.
