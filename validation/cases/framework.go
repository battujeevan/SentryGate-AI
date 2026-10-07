// Package cases defines validation tests TC01-TC12 and runs them against a
// harness stack, writing the evidence each test produces.
package cases

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/validation/agent"
	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/f5"
	"github.com/battujeevan/SentryGate-AI/validation/harness"
	"github.com/battujeevan/SentryGate-AI/validation/mcpserver"
)

// Case is one validation test.
type Case struct {
	ID                string
	Name              string
	Kind              string
	Objective         string
	Method            []string
	Expected          string
	ExpectedMutations int
	Limitations       []string
	Run               func(t *T) error
}

// SentryGate outcome of one attempt.
const (
	OutcomeDeniedAtIngress   = "DENIED_AT_INGRESS"
	OutcomeRejectedAtIngress = "REJECTED_AT_INGRESS"
	OutcomeNotStarted        = "NOT_STARTED"
	OutcomeRefusedAtBoundary = "REFUSED_AT_EXECUTION_BOUNDARY"
	OutcomeRefusedByAdapter  = "REFUSED_BY_ADAPTER"
	OutcomeExecuted          = "EXECUTED"
	OutcomeTerminated        = "TERMINATED_BEFORE_EXECUTION"
	OutcomeUnknown           = "UNKNOWN"
)

// RoleSetup marks requests that only prepare a test's scenario.
const RoleSetup = "setup"

var blockedOutcomes = map[string]bool{
	OutcomeDeniedAtIngress: true, OutcomeRejectedAtIngress: true, OutcomeNotStarted: true,
	OutcomeRefusedAtBoundary: true, OutcomeRefusedByAdapter: true,
}

// Attempt is a request the agent sent, with what came of it.
type Attempt struct {
	agent.Request
	Outcome          *harness.RunOutcome `json:"workflow_outcome,omitempty"`
	SentryGate       string              `json:"sentrygate_outcome"`
	SentryGateDetail string              `json:"sentrygate_detail,omitempty"`

	attempt      bool
	exec         *contracts.ExecutionRequest
	expect       string
	expectDetail string
}

// DecisionID is the ingress decision the attempt was recorded under or
// presented.
func (a *Attempt) DecisionID() string {
	if a.exec != nil {
		return a.exec.IngressDecisionID
	}
	if a.Response != nil {
		return a.Response.DecisionID
	}
	return ""
}

// ExecutionRequest is what the attempt's workflow was started with.
func (a *Attempt) ExecutionRequest() (contracts.ExecutionRequest, bool) {
	if a.exec == nil {
		return contracts.ExecutionRequest{}, false
	}
	return *a.exec, true
}

// ExpectAnyBlocked accepts any outcome in which SentryGate did not let the
// attempt execute.
const ExpectAnyBlocked = "any refusal (not EXECUTED)"

// Expect sets the SentryGate outcome the attempt must have, and optionally a
// string its detail must contain (for example a reason code).
func (a *Attempt) Expect(outcome, detail string) *Attempt {
	a.expect, a.expectDetail = outcome, detail
	return a
}

// T is the context of one running test.
type T struct {
	Ctx   context.Context
	S     *harness.Stack
	Case  Case
	Repo  string
	nonce string

	attempts []*Attempt
	checks   []evidence.Check
	notes    []string
	extra    map[string]any
}

// ID returns a proposal ID unique to this test run.
func (t *T) ID(name string) string { return t.Case.ID + "-" + t.nonce + "-" + name }

// Agent returns a validation agent's identity.
func (t *T) Agent(id string) agent.Identity { return t.S.Agents[id] }

// Note adds a note to the test result.
func (t *T) Note(format string, args ...any) { t.notes = append(t.notes, fmt.Sprintf(format, args...)) }

// Supplementary stores additional evidence under supplementary-<name>.json.
func (t *T) Supplementary(name string, v any) { t.extra[name] = v }

// Submit sends call to ingress as agentID.
func (t *T) Submit(label, role, agentID string, call agent.ToolCall) *Attempt {
	r := t.S.Gateway().Submit(t.Ctx, label, role, t.Agent(agentID), call.Proposal())
	return t.add(r, true)
}

// SubmitRaw sends body to ingress unchanged with id's key.
func (t *T) SubmitRaw(label, role string, id agent.Identity, body string) *Attempt {
	return t.add(t.S.Gateway().SubmitRaw(t.Ctx, label, role, id, body), true)
}

// Start starts a workflow directly in Temporal.
func (t *T) Start(label, workflowID string, req contracts.ExecutionRequest) *Attempt {
	a := t.add(t.S.Boundary().Start(t.Ctx, label, workflowID, req), true)
	a.exec = &req
	return a
}

// Terminate terminates a run directly in Temporal.
func (t *T) Terminate(label, workflowID, runID string) *Attempt {
	return t.add(t.S.Boundary().Terminate(t.Ctx, label, workflowID, runID), false)
}

// Reset resets a run directly in Temporal; req is the request of the run.
func (t *T) Reset(label, workflowID, runID string, eventID int64, req contracts.ExecutionRequest) *Attempt {
	a := t.add(t.S.Boundary().Reset(t.Ctx, label, workflowID, runID, eventID), true)
	a.exec = &req
	return a
}

func (t *T) add(r agent.Request, isAttempt bool) *Attempt {
	a := &Attempt{Request: r, attempt: isAttempt}
	if r.Path == agent.PathGateway && r.HTTPStatus == 200 && r.Response != nil && r.Proposal != nil {
		a.exec = &contracts.ExecutionRequest{
			AgentID: r.AgentID, Proposal: *r.Proposal,
			IngressDecisionID: r.Response.DecisionID, RequestHash: decision.RequestHash(*r.Proposal),
		}
	}
	t.attempts = append(t.attempts, a)
	return a
}

// Accepted reports whether ingress accepted the attempt and started a
// workflow; otherwise it records a failed check and returns an error that
// makes the test BLOCKED, because the remaining steps depend on it.
func (t *T) Accepted(a *Attempt) error {
	if a.HTTPStatus == 200 && a.Response != nil && a.Response.Status == "accepted" {
		return nil
	}
	return fmt.Errorf("prerequisite %q was not accepted by ingress: status %d %+v %s", a.Label, a.HTTPStatus, a.Response, a.Error)
}

// Started returns an error if a direct start failed.
func (t *T) Started(a *Attempt) error {
	if a.Error != "" || a.RunID == "" {
		return fmt.Errorf("prerequisite %q did not start a workflow: %s", a.Label, a.Error)
	}
	return nil
}

// Wait waits for the attempt's workflow run to close.
func (t *T) Wait(a *Attempt, timeout time.Duration) harness.RunOutcome {
	o := t.S.WaitRun(t.Ctx, a.WorkflowID, a.RunID, timeout)
	a.Outcome = &o
	return o
}

// Check records a check.
func (t *T) Check(name, expected, observed string, pass bool) {
	res := evidence.CheckFail
	if pass {
		res = evidence.CheckPass
	}
	t.checks = append(t.checks, evidence.Check{Name: name, Expected: expected, Observed: observed, Result: res})
}

// CheckEq records a check that observed equals expected.
func (t *T) CheckEq(name, expected, observed string) {
	t.Check(name, expected, observed, expected == observed)
}

// CheckInt records a check that observed equals expected.
func (t *T) CheckInt(name string, expected, observed int) {
	t.Check(name, fmt.Sprint(expected), fmt.Sprint(observed), expected == observed)
}

// HoldAuthorization obtains an ALLOW decision for call that has not been
// used: it stops the worker, submits call through ingress, and terminates the
// workflow ingress started before any worker can run it. The worker stays
// stopped; the caller presents the decision at the execution boundary and
// then starts the worker.
func (t *T) HoldAuthorization(label, agentID string, call agent.ToolCall) (*Attempt, contracts.ExecutionRequest, error) {
	t.S.StopWorker()
	a := t.Submit(label, agent.RoleLegitimate, agentID, call)
	if err := t.Accepted(a); err != nil {
		return a, contracts.ExecutionRequest{}, err
	}
	a.Expect(OutcomeTerminated, "")
	term := t.Terminate("setup: terminate the authorized run before any worker runs it", a.WorkflowID, a.RunID)
	term.Role = RoleSetup
	if term.Error != "" {
		return a, contracts.ExecutionRequest{}, fmt.Errorf("terminate %s: %s", a.WorkflowID, term.Error)
	}
	t.Wait(a, 10*time.Second)
	req, _ := a.ExecutionRequest()
	return a, req, nil
}

// Infrastructure returns the current test MCP server state.
func (t *T) Infrastructure() (mcpserver.State, error) { return t.S.Infrastructure(t.Ctx) }

// Proposal returns a copy of p with fields replaced by f.
func Proposal(p contracts.AgentProposal, f func(*contracts.AgentProposal)) contracts.AgentProposal {
	f(&p)
	return p
}

// classify sets the attempt's SentryGate outcome from what was observed.
func classify(a *Attempt, execByKey map[string]int) {
	switch {
	case a.Path == agent.PathGateway:
		switch {
		case a.Error != "" && a.HTTPStatus == 0:
			a.SentryGate, a.SentryGateDetail = OutcomeUnknown, "no response: "+a.Error
		case a.HTTPStatus == 403 && a.Response != nil:
			a.SentryGate = OutcomeDeniedAtIngress
			a.SentryGateDetail = fmt.Sprintf("verdict=%s reasons=%s decision_id=%s", a.Response.Verdict, strings.Join(a.Response.Reasons, ","), a.Response.DecisionID)
		case a.HTTPStatus == 400 || a.HTTPStatus == 401 || a.HTTPStatus == 413:
			a.SentryGate = OutcomeRejectedAtIngress
			a.SentryGateDetail = fmt.Sprintf("HTTP %d %s", a.HTTPStatus, responseError(a))
		case a.HTTPStatus == 409:
			a.SentryGate = OutcomeNotStarted
			a.SentryGateDetail = fmt.Sprintf("HTTP 409 %s", responseError(a))
		case a.HTTPStatus == 200:
			classifyRun(a, execByKey)
		default:
			a.SentryGate, a.SentryGateDetail = OutcomeUnknown, fmt.Sprintf("HTTP %d %s", a.HTTPStatus, responseError(a))
		}
	case a.Error != "":
		a.SentryGate, a.SentryGateDetail = OutcomeNotStarted, a.Error
	default:
		classifyRun(a, execByKey)
	}
}

func responseError(a *Attempt) string {
	if a.Response != nil {
		return a.Response.Error
	}
	return a.Error
}

func classifyRun(a *Attempt, execByKey map[string]int) {
	o := a.Outcome
	if o == nil {
		a.SentryGate, a.SentryGateDetail = OutcomeUnknown, "workflow run not observed"
		return
	}
	detail := strings.TrimSpace(o.ErrorType + " " + o.Reason)
	switch o.Status {
	case harness.RunCompleted:
		a.SentryGate, a.SentryGateDetail = OutcomeExecuted, "workflow completed"
	case harness.RunTerminated:
		a.SentryGate, a.SentryGateDetail = OutcomeTerminated, "run terminated"
	case harness.RunFailed:
		switch o.ErrorType {
		case contracts.IngressDecisionInvalidErrorType, contracts.ExecutionClaimRejectedErrorType, contracts.RevalidationDeniedErrorType:
			a.SentryGate, a.SentryGateDetail = OutcomeRefusedAtBoundary, detail
		case contracts.DispatchFailedErrorType:
			if execByKey[decision.ExecutionKey(a.DecisionID())] == 0 {
				a.SentryGate, a.SentryGateDetail = OutcomeRefusedByAdapter, detail+": no tools/call reached the MCP server; "+o.Message
			} else {
				a.SentryGate, a.SentryGateDetail = OutcomeExecuted, detail+": the MCP server received the call; "+o.Message
			}
		default:
			a.SentryGate, a.SentryGateDetail = OutcomeUnknown, detail+": "+o.Message
		}
	default:
		a.SentryGate, a.SentryGateDetail = OutcomeUnknown, o.Status+" "+o.Message
	}
}

// Runner runs cases and writes their evidence.
type Runner struct {
	Suite       *harness.Suite
	Provider    f5.F5ObservationProvider
	EvidenceDir string
	// RepoRoot is used to make evidence paths relative.
	RepoRoot string
}

// Run runs c in a fresh stack and writes its evidence. It returns an error
// only if the evidence could not be written.
func (r *Runner) Run(ctx context.Context, c Case) (evidence.Result, error) {
	started := time.Now().UTC()
	dir := filepath.Join(r.EvidenceDir, c.ID)
	if err := os.RemoveAll(dir); err != nil {
		return evidence.Result{}, err
	}
	res := evidence.Result{
		TestID: c.ID, TestName: c.Name, Kind: c.Kind, Objective: c.Objective, Method: c.Method, Expected: c.Expected,
		ExpectedMutations: c.ExpectedMutations, F5Provider: r.Provider.Name(), Checks: []evidence.Check{},
		Attempts: []evidence.AttemptSummary{}, Notes: []string{}, Limitations: append([]string{}, c.Limitations...),
		StartedAt: started.Format(time.RFC3339),
	}
	if rel, err := filepath.Rel(r.RepoRoot, dir); err == nil {
		res.EvidencePath = evidence.Slash(rel)
	} else {
		res.EvidencePath = evidence.Slash(dir)
	}
	envMap := map[string]any{}
	if b, err := json.Marshal(r.Suite.Env); err == nil {
		_ = json.Unmarshal(b, &envMap)
	}
	res.Environment = envMap

	st, err := r.Suite.NewStack(ctx, c.ID)
	if err != nil {
		res.Status = evidence.StatusBlocked
		res.BlockedReason = "stack did not start: " + err.Error()
		res.F5Result, res.SentryGateResult, res.InfrastructureResult = f5.ResultNotTested, evidence.SGUnknown, evidence.InfraUnknown
		res.FinishedAt = time.Now().UTC().Format(time.RFC3339)
		return res, r.writeMinimal(dir, res)
	}
	defer st.Close()

	t := &T{Ctx: ctx, S: st, Case: c, Repo: r.RepoRoot, nonce: strings.ToLower(started.Format("150405")), extra: map[string]any{}}
	before, beforeErr := st.Infrastructure(ctx)
	runErr := c.Run(t)
	// Let in-flight workflow and audit writes settle before reading state.
	time.Sleep(500 * time.Millisecond)
	after, afterErr := st.Infrastructure(ctx)

	counts := after.Count()
	execByKey := map[string]int{}
	for _, e := range after.Executions {
		execByKey[e.IdempotencyKey]++
	}
	for _, a := range t.attempts {
		if a.attempt {
			classify(a, execByKey)
		} else {
			a.SentryGate = "N/A (not an execution attempt)"
		}
	}

	// Checks on every attempt's expected outcome.
	for _, a := range t.attempts {
		if a.expect == "" {
			continue
		}
		pass := a.SentryGate == a.expect && strings.Contains(a.SentryGateDetail, a.expectDetail)
		if a.expect == ExpectAnyBlocked {
			pass = blockedOutcomes[a.SentryGate]
		}
		exp := a.expect
		if a.expectDetail != "" {
			exp += " (" + a.expectDetail + ")"
		}
		t.Check("SentryGate outcome: "+a.Label, exp, strings.TrimSpace(a.SentryGate+" "+a.SentryGateDetail), pass)
	}

	// Infrastructure checks common to every test.
	if afterErr == nil {
		t.CheckInt("infrastructure mutation count (test MCP server database)", c.ExpectedMutations, counts.Mutations)
		dup := []string{}
		applied := map[string]int{}
		for _, e := range after.Executions {
			if e.Result == mcpserver.ResultApplied && e.MutationCount > 0 {
				applied[e.IdempotencyKey]++
			}
		}
		for k, n := range applied {
			if n > 1 {
				dup = append(dup, fmt.Sprintf("%s applied %d times", k, n))
			}
		}
		sort.Strings(dup)
		t.Check("no idempotency key mutated the target more than once", "none", orNone(dup), len(dup) == 0)
	} else {
		t.checks = append(t.checks, evidence.Check{Name: "infrastructure state readable", Expected: "readable",
			Observed: afterErr.Error(), Result: evidence.CheckUnknown})
	}

	// SentryGate observations of every run.
	var observations []harness.SentryGateObservation
	seen := map[string]bool{}
	for _, a := range t.attempts {
		req, ok := a.ExecutionRequest()
		if !ok || a.Outcome == nil || seen[a.WorkflowID+"/"+a.RunID] {
			continue
		}
		seen[a.WorkflowID+"/"+a.RunID] = true
		observations = append(observations, st.Observe(ctx, *a.Outcome, req))
	}

	// F5 observations of every gateway request.
	type f5Entry struct {
		RequestLabel string         `json:"request_label"`
		Role         string         `json:"role"`
		Correlation  f5.Correlation `json:"correlation"`
		Observation  f5.Observation `json:"observation"`
		Error        string         `json:"error,omitempty"`
	}
	var f5Entries []f5Entry
	var scenarioObs []f5.Observation
	scenarioRole := agent.RoleAttack
	if c.Kind != evidence.KindAttack {
		scenarioRole = agent.RoleLegitimate
	}
	source := ""
	f5ByLabel := map[*Attempt]f5.Observation{}
	for _, a := range t.attempts {
		if a.Path != agent.PathGateway {
			continue
		}
		corr := f5.Correlation{TestID: c.ID, RequestLabel: a.Label, AgentID: a.AgentID, SentAt: a.SentAt}
		if a.Proposal != nil {
			corr.ProposalID, corr.Tool, corr.Target = a.Proposal.ID, strings.ToLower(string(a.Proposal.Type)), a.Proposal.TargetID
		}
		obs, err := r.Provider.Observe(ctx, corr)
		e := f5Entry{RequestLabel: a.Label, Role: a.Role, Correlation: corr, Observation: obs}
		if err != nil {
			e.Error = err.Error()
			obs = f5.Observation{Source: "ERROR", AuthorizationDecision: f5.DecisionNotObserved}
		}
		f5Entries = append(f5Entries, e)
		f5ByLabel[a] = obs
		if a.Role == scenarioRole {
			scenarioObs = append(scenarioObs, obs)
		}
		if source == "" {
			source = obs.Source
		} else if source != obs.Source {
			source = "MIXED"
		}
	}
	res.F5Result = f5.Classify(scenarioObs)
	if source == "" {
		source = "NONE (no gateway-path requests)"
	}
	res.F5ObservationSource = source
	if res.F5Result == f5.ResultNotTested {
		t.Note("F5 result NOT_TESTED: no F5 component was in the request path; the F5 observation provider was %q and returned placeholders only.", r.Provider.Name())
	}
	if res.F5Result == f5.ResultNotApplicable {
		t.Note("F5 result NOT_APPLICABLE: every %s request in this test was sent to the execution boundary (Temporal) directly, a path that does not pass through a gateway in front of SentryGate ingress.", scenarioRole)
	}

	// Layer results.
	res.SentryGateResult = sentryGateResult(c.Kind, t.attempts)
	res.MutationCount, res.Invocations = counts.Mutations, counts.Invocations
	switch {
	case afterErr != nil:
		res.InfrastructureResult = evidence.InfraUnknown
	case counts.Invocations == 0:
		res.InfrastructureResult = evidence.InfraNotExecuted
	case counts.Mutations == 0:
		res.InfrastructureResult = evidence.InfraExecutedNoMutation
	default:
		res.InfrastructureResult = evidence.InfraMutated
	}

	res.Checks = t.checks
	res.Notes = append(res.Notes, t.notes...)
	switch {
	case runErr != nil:
		res.Status = evidence.StatusBlocked
		res.BlockedReason = runErr.Error()
	default:
		res.Status = evidence.StatusVerified
		for _, ch := range t.checks {
			if ch.Result == evidence.CheckFail {
				res.Status = evidence.StatusFailed
				break
			}
			if ch.Result == evidence.CheckUnknown {
				res.Status = evidence.StatusUnknown
			}
		}
	}
	for _, a := range t.attempts {
		s := evidence.AttemptSummary{
			Label: a.Label, Role: a.Role, Path: a.Path, Action: a.Action, AgentID: a.AgentID,
			SentryGate: a.SentryGate, SentryGateDetail: a.SentryGateDetail,
		}
		if a.Proposal != nil {
			s.Tool, s.Target = strings.ToLower(string(a.Proposal.Type)), a.Proposal.TargetID
		}
		if o, ok := f5ByLabel[a]; ok {
			s.F5Decision, s.F5Source = o.AuthorizationDecision, o.Source
		}
		res.Attempts = append(res.Attempts, s)
	}

	// Decision records of every proposal the test used.
	proposals := map[string]bool{}
	for _, a := range t.attempts {
		if a.Proposal != nil {
			proposals[a.Proposal.ID] = true
		}
	}
	ids := make([]string, 0, len(proposals))
	for id := range proposals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var ingressDecisions []harness.DecisionRef
	for _, id := range ids {
		for _, d := range st.Decisions(ctx, id) {
			if d.Stage == string(contracts.StageIngress) {
				ingressDecisions = append(ingressDecisions, d)
			}
		}
	}

	files := map[string]any{
		"request.json": map[string]any{"test_id": c.ID, "requests": t.attempts,
			"note": "API keys are never recorded. path=gateway is POST /v1/intercept; path=execution-boundary is direct Temporal access."},
		"authorization.json": map[string]any{"test_id": c.ID, "f5_provider": r.Provider.Name(), "f5_result": res.F5Result,
			"f5_observation_source": res.F5ObservationSource, "f5_observations": orEmpty(f5Entries),
			"sentrygate_ingress_decisions": orEmpty(ingressDecisions)},
		"sentrygate-decision.json": map[string]any{"test_id": c.ID, "sentrygate_result": res.SentryGateResult,
			"attempts": res.Attempts, "runs": orEmpty(observations)},
		"execution.json": map[string]any{"test_id": c.ID, "executions": after.Executions, "counts": counts,
			"note": "Every tools/call that reached the test MCP server, in order. The server never deduplicates."},
		"infrastructure-state.json": map[string]any{"test_id": c.ID, "infrastructure_result": res.InfrastructureResult,
			"mutation_count": counts.Mutations, "expected_mutation_count": c.ExpectedMutations, "tool_invocations": counts.Invocations,
			"customers": after.Customers, "exports": after.Exports, "fenced_keys": after.FencedKeys,
			"transport_events": after.TransportEvents, "read_error": errString(afterErr)},
		"before-state.json":    map[string]any{"test_id": c.ID, "state": before, "read_error": errString(beforeErr)},
		"after-state.json":     map[string]any{"test_id": c.ID, "state": after, "read_error": errString(afterErr)},
		"execution-count.json": map[string]any{"test_id": c.ID, "counts": counts},
	}
	for name, v := range t.extra {
		files["supplementary-"+name+".json"] = v
	}
	names := make([]string, 0, len(files)+1)
	for name := range files {
		names = append(names, name)
	}
	names = append(names, "test-result.json")
	sort.Strings(names)
	res.Files = names
	res.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	for name, v := range files {
		if err := evidence.WriteJSON(filepath.Join(dir, name), v); err != nil {
			return res, err
		}
	}
	return res, evidence.WriteJSON(filepath.Join(dir, "test-result.json"), res)
}

// writeMinimal writes the required files for a test that could not start.
func (r *Runner) writeMinimal(dir string, res evidence.Result) error {
	empty := map[string]any{"test_id": res.TestID, "note": "test BLOCKED before any request was sent: " + res.BlockedReason}
	res.Files = append([]string{}, evidence.RequiredFiles...)
	sort.Strings(res.Files)
	for _, f := range evidence.RequiredFiles {
		if f == "test-result.json" {
			continue
		}
		if err := evidence.WriteJSON(filepath.Join(dir, f), empty); err != nil {
			return err
		}
	}
	return evidence.WriteJSON(filepath.Join(dir, "test-result.json"), res)
}

func sentryGateResult(kind string, attempts []*Attempt) string {
	role := agent.RoleAttack
	if kind != evidence.KindAttack {
		role = agent.RoleLegitimate
	}
	n, executed, blocked, unknown := 0, 0, 0, 0
	for _, a := range attempts {
		if !a.attempt || a.Role != role {
			continue
		}
		n++
		switch {
		case a.SentryGate == OutcomeExecuted:
			executed++
		case blockedOutcomes[a.SentryGate]:
			blocked++
		default:
			unknown++
		}
	}
	switch {
	case n == 0:
		return evidence.SGNotApplicable
	case kind == evidence.KindAttack && executed > 0:
		return evidence.SGDidNotBlock
	case kind == evidence.KindAttack && blocked == n:
		return evidence.SGBlocked
	case kind != evidence.KindAttack && executed == n:
		return evidence.SGExecuted
	case unknown > 0:
		return evidence.SGUnknown
	default:
		return evidence.SGNotExecuted
	}
}

func orNone(s []string) string {
	if len(s) == 0 {
		return "none"
	}
	return strings.Join(s, "; ")
}

func orEmpty[E any](s []E) []E {
	if s == nil {
		return []E{}
	}
	return s
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
