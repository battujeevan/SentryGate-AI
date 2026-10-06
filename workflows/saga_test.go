package workflows_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

const workflowPolicy = `version: wf-v1
max_parallel_tasks: 4
environments:
  staging: allow
  production: require_approval
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - id: agent-a
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
  - id: agent-b
    commands: [MODIFY_ROUTING]
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-1, environment: staging, protected: false}
  - {id: prod-1, environment: production, protected: false}
  - {id: FAILING_NODE, environment: staging, protected: false}
  - {id: LOST_RESPONSE_NODE, environment: staging, protected: false}
  - {id: UNREACHABLE_NODE, environment: staging, protected: false}
  - {id: PARTITIONED_NODE, environment: staging, protected: false}
`

// Same as workflowPolicy except edge-1 is now protected.
const tightenedPolicy = `version: wf-v2
max_parallel_tasks: 4
environments:
  staging: allow
  production: require_approval
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - id: agent-a
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
  - id: agent-b
    commands: [MODIFY_ROUTING]
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-1, environment: staging, protected: true}
  - {id: prod-1, environment: production, protected: false}
  - {id: FAILING_NODE, environment: staging, protected: false}
`

var ingressTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// Dispatch outcomes an adapter can report.
var (
	succeeded      = contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, Detail: "applied"}
	failedOutright = contracts.DispatchOutcome{Status: contracts.OutcomeFailure, Detail: "rejected by target"}
	failedPartly   = contracts.DispatchOutcome{Status: contracts.OutcomeFailure, PartiallyApplied: true, Detail: "partly applied, then rejected"}
	unknown        = contracts.UnknownOutcome("response lost")
)

type swappableSource struct {
	p atomic.Pointer[policy.Snapshot]
}

func (s *swappableSource) Current() *policy.Snapshot { return s.p.Load() }

func parsePolicy(t *testing.T, doc string) *policy.Snapshot {
	t.Helper()
	s, err := policy.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

type harness struct {
	env       *testsuite.TestWorkflowEnvironment
	src       *swappableSource
	decisions *decision.MemoryStore
	audit     *workflows.MemoryAuditStore

	mu      sync.Mutex
	started []string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithStores(t, nil, nil)
}

// newHarnessWithStores registers DecisionActivities against store and claims,
// each defaulting to the harness's MemoryStore when nil.
func newHarnessWithStores(t *testing.T, store decision.Store, claims decision.ClaimStore) *harness {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	h := &harness{
		env:       ts.NewTestWorkflowEnvironment(),
		src:       &swappableSource{},
		decisions: decision.NewMemoryStore(),
		audit:     workflows.NewMemoryAuditStore(),
	}
	if store == nil {
		store = h.decisions
	}
	if claims == nil {
		claims = h.decisions
	}
	h.src.p.Store(parsePolicy(t, workflowPolicy))
	h.env.RegisterActivity(&workflows.DecisionActivities{Policy: h.src, Store: store, Claims: claims})
	h.env.RegisterActivity(&workflows.AuditActivities{Store: h.audit})
	h.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		h.mu.Lock()
		h.started = append(h.started, info.ActivityType.Name)
		h.mu.Unlock()
	})
	return h
}

func (h *harness) ran(name string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Contains(h.started, name)
}

func (h *harness) count(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.started {
		if s == name {
			n++
		}
	}
	return n
}

func (h *harness) order() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.started)
}

// seed writes a decision record directly to the harness store, standing in for
// the proxy's ingress write.
func (h *harness) seed(t *testing.T, rec contracts.DecisionRecord) {
	t.Helper()
	if err := h.decisions.AppendDecision(context.Background(), rec); err != nil {
		t.Fatalf("seed decision: %v", err)
	}
}

// run executes the workflow under workflowID, as the proxy would start it.
func (h *harness) run(workflowID string, req contracts.ExecutionRequest) {
	h.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: workflowID})
	h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, req)
}

// workflowRecords returns the decisions written by the workflow itself.
func (h *harness) workflowRecords() []contracts.DecisionRecord {
	var out []contracts.DecisionRecord
	for _, r := range h.decisions.All() {
		if r.Stage == contracts.StageWorkflowRevalidation {
			out = append(out, r)
		}
	}
	return out
}

func request(agentID string, cmd contracts.CommandType, target string) contracts.ExecutionRequest {
	prop := contracts.AgentProposal{ID: "prop-" + target, Type: cmd, TargetID: target, Payload: `{"k":"v"}`}
	return contracts.ExecutionRequest{
		AgentID:           agentID,
		Proposal:          prop,
		IngressDecisionID: "dec_ingress_" + prop.ID,
		RequestHash:       decision.RequestHash(prop),
	}
}

// workflowIDFor mirrors the proxy's workflow ID for a proposal.
func workflowIDFor(req contracts.ExecutionRequest) string {
	return "saga-" + req.Proposal.ID
}

// ingressAllow is the record the proxy writes when it allows req.
func ingressAllow(req contracts.ExecutionRequest) contracts.DecisionRecord {
	return contracts.DecisionRecord{
		DecisionID:    req.IngressDecisionID,
		Stage:         contracts.StageIngress,
		ProposalID:    req.Proposal.ID,
		AgentID:       req.AgentID,
		RequestHash:   decision.RequestHash(req.Proposal),
		Command:       req.Proposal.Type,
		TargetID:      req.Proposal.TargetID,
		Environment:   "staging",
		Verdict:       contracts.VerdictAllow,
		Reasons:       []contracts.ReasonCode{contracts.ReasonEnvironmentAllowed},
		PolicyVersion: "wf-v1",
		WorkflowID:    workflowIDFor(req),
		RecordedAt:    ingressTime,
	}
}

// runAllowed seeds a legitimate ingress ALLOW for req and runs the workflow
// under the proxy's workflow ID.
func (h *harness) runAllowed(t *testing.T, req contracts.ExecutionRequest) {
	t.Helper()
	h.seed(t, ingressAllow(req))
	h.run(workflowIDFor(req), req)
}

func requireRevalidationDenied(t *testing.T, err error) {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != contracts.RevalidationDeniedErrorType {
		t.Fatalf("expected %s application error, got %v", contracts.RevalidationDeniedErrorType, err)
	}
	if !appErr.NonRetryable() {
		t.Fatal("REVALIDATION_DENIED must be non-retryable")
	}
}

// requireIngressInvalid asserts a non-retryable INGRESS_DECISION_INVALID error
// and returns its reason code ("" when the error carries none).
func requireIngressInvalid(t *testing.T, err error) contracts.ReasonCode {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != contracts.IngressDecisionInvalidErrorType {
		t.Fatalf("expected %s application error, got %v", contracts.IngressDecisionInvalidErrorType, err)
	}
	if !appErr.NonRetryable() {
		t.Fatal("INGRESS_DECISION_INVALID must be non-retryable")
	}
	var reason contracts.ReasonCode
	if appErr.HasDetails() {
		if err := appErr.Details(&reason); err != nil {
			t.Fatalf("decode error details: %v", err)
		}
	}
	return reason
}

func requireNoInfrastructure(t *testing.T, h *harness) {
	t.Helper()
	if h.ran("DispatchConfig") || h.ran("RevertStateCompensation") {
		t.Fatalf("infrastructure activity ran: %v", h.order())
	}
}

func TestAllowedWorkflowDispatches(t *testing.T) {
	h := newHarness(t)
	var infra *workflows.InfrastructureActivities
	var calls atomic.Int64
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(
		fencedDispatch(h.decisions, countingAdapter{n: &calls, outcome: succeeded}, nil)).Once()

	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	h.runAllowed(t, req)

	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not complete cleanly: %v", h.env.GetWorkflowError())
	}
	h.env.AssertExpectations(t)
	if calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want 1", calls.Load())
	}

	order := h.order()
	verify := slices.Index(order, "VerifyIngressDecision")
	claim := slices.Index(order, "ClaimExecution")
	evaluate := slices.Index(order, "EvaluateExecution")
	dispatch := slices.Index(order, "DispatchConfig")
	if verify < 0 || claim < 0 || evaluate < 0 || dispatch < 0 ||
		!(verify < claim && claim < evaluate && evaluate < dispatch) ||
		slices.Contains(order[:dispatch], "AdvanceExecutionClaim") {
		t.Fatalf("want verification, claim, re-validation, then dispatch with no claim transition in between; got %v", order)
	}

	c, err := h.decisions.GetClaim(context.Background(), req.IngressDecisionID)
	if err != nil || c.State != contracts.ClaimStateCompleted || c.Owner.WorkflowID != workflowIDFor(req) ||
		c.Owner.RunID == "" || !strings.HasPrefix(c.Owner.Token, "exe_") {
		t.Fatalf("claim after success = %+v, %v", c, err)
	}

	recs := h.workflowRecords()
	if len(recs) != 1 {
		t.Fatalf("expected one re-validation decision, got %+v", recs)
	}
	r := recs[0]
	if r.Verdict != contracts.VerdictAllow || r.AgentID != "agent-a" ||
		r.PolicyVersion != "wf-v1" || r.RequestHash != req.RequestHash || r.WorkflowID != workflowIDFor(req) {
		t.Fatalf("unexpected re-validation record: %+v", r)
	}

	var phases []contracts.AuditPhase
	for _, a := range h.audit.All() {
		phases = append(phases, a.Phase)
	}
	want := []contracts.AuditPhase{contracts.AuditPhaseRevalidation, contracts.AuditPhaseDispatch, contracts.AuditPhaseWorkflowComplete}
	if !slices.Equal(phases, want) {
		t.Fatalf("audit phases = %v, want %v", phases, want)
	}
}

// Each case presents an execution that is not backed by a valid ingress ALLOW
// for exactly this agent, proposal and workflow. The workflow must reject it
// with the matching reason before policy re-validation or any infrastructure
// activity runs.
func TestIngressDecisionIsRequiredForExecution(t *testing.T) {
	base := request("agent-a", contracts.CmdModifyRouting, "edge-1")

	cases := []struct {
		name       string
		seed       func(rec *contracts.DecisionRecord) bool // false: write no record
		mutateReq  func(req *contracts.ExecutionRequest)
		workflowID string // "" means the proxy's workflow ID
		reason     contracts.ReasonCode
	}{
		{
			name:      "missing ingress decision ID",
			seed:      func(*contracts.DecisionRecord) bool { return false },
			mutateReq: func(r *contracts.ExecutionRequest) { r.IngressDecisionID = "" },
			reason:    contracts.ReasonIngressDecisionNotFound,
		},
		{
			name:   "no recorded ingress decision",
			seed:   func(*contracts.DecisionRecord) bool { return false },
			reason: contracts.ReasonIngressDecisionNotFound,
		},
		{
			name:      "fake ingress decision ID",
			mutateReq: func(r *contracts.ExecutionRequest) { r.IngressDecisionID = "dec_fabricated" },
			reason:    contracts.ReasonIngressDecisionNotFound,
		},
		{
			name: "ingress verdict DENY",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.Verdict = contracts.VerdictDeny
				rec.Reasons = []contracts.ReasonCode{contracts.ReasonTargetProtected}
				return true
			},
			reason: contracts.ReasonIngressDecisionNotAllow,
		},
		{
			name: "ingress verdict REQUIRE_APPROVAL",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.Verdict = contracts.VerdictRequireApproval
				rec.Reasons = []contracts.ReasonCode{contracts.ReasonApprovalRequired}
				return true
			},
			reason: contracts.ReasonIngressDecisionNotAllow,
		},
		{
			name: "wrong stage",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.Stage = contracts.StageWorkflowRevalidation
				return true
			},
			reason: contracts.ReasonIngressDecisionWrongStage,
		},
		{
			name: "agent mismatch",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.AgentID = "agent-b"
				return true
			},
			reason: contracts.ReasonIngressAgentMismatch,
		},
		{
			name: "recorded request hash differs",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.RequestHash = "0000000000000000000000000000000000000000000000000000000000000000"
				return true
			},
			reason: contracts.ReasonIngressHashMismatch,
		},
		{
			// The attacker recomputes RequestHash for the altered payload, so
			// the request is internally consistent; only the recorded hash
			// shows that this is not the content that was allowed.
			name: "payload changed after ingress",
			mutateReq: func(r *contracts.ExecutionRequest) {
				r.Proposal.Payload = `{"k":"tampered"}`
				r.RequestHash = decision.RequestHash(r.Proposal)
			},
			reason: contracts.ReasonIngressHashMismatch,
		},
		{
			name: "recorded workflow ID differs",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.WorkflowID = "saga-other"
				return true
			},
			reason: contracts.ReasonIngressIdentityMismatch,
		},
		{
			name: "recorded workflow ID empty",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.WorkflowID = ""
				return true
			},
			reason: contracts.ReasonIngressIdentityMismatch,
		},
		{
			name:       "started under a different workflow ID",
			workflowID: "attacker-chosen-id",
			reason:     contracts.ReasonIngressIdentityMismatch,
		},
		{
			name: "recorded proposal ID differs",
			seed: func(rec *contracts.DecisionRecord) bool {
				rec.ProposalID = "prop-other"
				return true
			},
			reason: contracts.ReasonIngressIdentityMismatch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var infra *workflows.InfrastructureActivities
			h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()
			h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Never()

			rec := ingressAllow(base)
			if tc.seed == nil || tc.seed(&rec) {
				h.seed(t, rec)
			}
			req := base
			if tc.mutateReq != nil {
				tc.mutateReq(&req)
			}
			wfID := tc.workflowID
			if wfID == "" {
				wfID = workflowIDFor(base)
			}
			h.run(wfID, req)

			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if got := requireIngressInvalid(t, h.env.GetWorkflowError()); got != tc.reason {
				t.Fatalf("reason = %q, want %q", got, tc.reason)
			}
			requireNoInfrastructure(t, h)
			if h.ran("ClaimExecution") || h.ran("EvaluateExecution") {
				t.Fatalf("claim or re-validation ran before the ingress decision was verified: %v", h.order())
			}
			if _, err := h.decisions.GetClaim(context.Background(), rec.DecisionID); !errors.Is(err, contracts.ErrClaimNotFound) {
				t.Fatalf("rejected execution left a claim on the decision: %v", err)
			}
			h.env.AssertExpectations(t)

			recs := h.workflowRecords()
			if tc.seed != nil && rec.Stage == contracts.StageWorkflowRevalidation {
				recs = slices.DeleteFunc(recs, func(r contracts.DecisionRecord) bool { return r.DecisionID == rec.DecisionID })
			}
			if len(recs) != 1 || recs[0].Verdict != contracts.VerdictDeny ||
				!slices.Equal(recs[0].Reasons, []contracts.ReasonCode{tc.reason}) {
				t.Fatalf("expected one recorded DENY with %s, got %+v", tc.reason, recs)
			}
			audits := h.audit.All()
			if len(audits) != 1 || audits[0].Phase != contracts.AuditPhaseRevalidation || audits[0].Verdict != contracts.AuditVerdictFail {
				t.Fatalf("expected one WORKFLOW_REVALIDATION FAIL audit row, got %+v", audits)
			}
		})
	}
}

// brokenGetStore fails every lookup, as if the decision database were down.
type brokenGetStore struct{ *decision.MemoryStore }

func (brokenGetStore) GetDecision(context.Context, string) (contracts.DecisionRecord, error) {
	return contracts.DecisionRecord{}, errors.New("database is locked")
}

func TestIngressDecisionStoreFailureFailsClosed(t *testing.T) {
	store := brokenGetStore{decision.NewMemoryStore()}
	h := newHarnessWithStores(t, store, store)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()

	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	if err := store.AppendDecision(context.Background(), ingressAllow(req)); err != nil {
		t.Fatal(err)
	}
	h.run(workflowIDFor(req), req)

	requireIngressInvalid(t, h.env.GetWorkflowError())
	requireNoInfrastructure(t, h)
	if h.ran("EvaluateExecution") || h.ran("ClaimExecution") {
		t.Fatalf("claim or re-validation ran without a verified ingress decision: %v", h.order())
	}
	h.env.AssertExpectations(t)
}

// Adversarial: someone with access to Temporal starts the workflow directly,
// bypassing the proxy. Every request below is one the current policy would
// ALLOW at re-validation, and the store holds real records the attacker can
// try to borrow. None of them may reach dispatch.
func TestDirectTemporalStartWithoutLegitimateIngressNeverDispatches(t *testing.T) {
	victim := request("agent-a", contracts.CmdModifyRouting, "edge-1")

	// Denied at ingress while edge-1 was protected; the policy has since been
	// relaxed, so re-validation alone would now allow it.
	deniedEarlier := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	deniedEarlier.Proposal.ID = "prop-denied-earlier"
	deniedEarlier.IngressDecisionID = "dec_ingress_denied_earlier"
	deniedEarlier.RequestHash = decision.RequestHash(deniedEarlier.Proposal)
	deniedRec := ingressAllow(deniedEarlier)
	deniedRec.Verdict = contracts.VerdictDeny
	deniedRec.Reasons = []contracts.ReasonCode{contracts.ReasonTargetProtected}
	deniedRec.WorkflowID = ""

	priorRevalidation := ingressAllow(victim)
	priorRevalidation.DecisionID = "wf_" + workflowIDFor(victim) + "_run-1"
	priorRevalidation.Stage = contracts.StageWorkflowRevalidation

	corpus := []contracts.DecisionRecord{ingressAllow(victim), deniedRec, priorRevalidation}

	withProposal := func(req contracts.ExecutionRequest, id, payload string) contracts.ExecutionRequest {
		req.Proposal.ID = id
		req.Proposal.Payload = payload
		req.RequestHash = decision.RequestHash(req.Proposal)
		return req
	}

	attacks := []struct {
		name       string
		req        contracts.ExecutionRequest
		workflowID string
	}{
		{"no ingress decision ID", func() contracts.ExecutionRequest {
			r := withProposal(victim, "prop-attacker", `{"k":"evil"}`)
			r.IngressDecisionID = ""
			return r
		}(), "saga-prop-attacker"},
		{"fabricated ingress decision ID", func() contracts.ExecutionRequest {
			r := withProposal(victim, "prop-attacker", `{"k":"evil"}`)
			r.IngressDecisionID = decision.NewDecisionID()
			return r
		}(), "saga-prop-attacker"},
		{"victim's ALLOW reused for another proposal",
			withProposal(victim, "prop-attacker", `{"k":"evil"}`), "saga-prop-attacker"},
		{"victim's ALLOW reused with altered payload",
			withProposal(victim, victim.Proposal.ID, `{"k":"evil"}`), workflowIDFor(victim)},
		{"victim's ALLOW reused by another agent", func() contracts.ExecutionRequest {
			r := victim
			r.AgentID = "agent-b"
			return r
		}(), workflowIDFor(victim)},
		{"victim's ALLOW started under another workflow ID", victim, "saga-shadow-run"},
		{"workflow re-validation record presented as ingress", func() contracts.ExecutionRequest {
			r := victim
			r.IngressDecisionID = priorRevalidation.DecisionID
			return r
		}(), workflowIDFor(victim)},
		{"ingress DENY resubmitted after policy relaxed", deniedEarlier, workflowIDFor(deniedEarlier)},
	}

	for _, a := range attacks {
		t.Run(a.name, func(t *testing.T) {
			h := newHarness(t)
			if got := decision.Revalidate(h.src.Current(), a.req); got.Verdict != contracts.VerdictAllow {
				t.Fatalf("precondition: re-validation alone must ALLOW this request, got %+v", got)
			}
			for _, rec := range corpus {
				h.seed(t, rec)
			}
			var infra *workflows.InfrastructureActivities
			h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()
			h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Never()

			h.run(a.workflowID, a.req)

			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if reason := requireIngressInvalid(t, h.env.GetWorkflowError()); reason == "" {
				t.Fatal("rejection carries no reason code")
			}
			requireNoInfrastructure(t, h)
			if h.ran("ClaimExecution") {
				t.Fatal("a forged execution reached the claim step")
			}
			for _, rec := range corpus {
				if _, err := h.decisions.GetClaim(context.Background(), rec.DecisionID); !errors.Is(err, contracts.ErrClaimNotFound) {
					t.Fatalf("forged execution claimed %s: %v", rec.DecisionID, err)
				}
			}
			h.env.AssertExpectations(t)
		})
	}

	// Control: the same corpus does authorize the victim's own execution.
	t.Run("legitimate execution still dispatches", func(t *testing.T) {
		h := newHarness(t)
		for _, rec := range corpus {
			h.seed(t, rec)
		}
		var infra *workflows.InfrastructureActivities
		var calls atomic.Int64
		h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(
			fencedDispatch(h.decisions, countingAdapter{n: &calls, outcome: succeeded}, nil)).Once()

		h.run(workflowIDFor(victim), victim)

		if err := h.env.GetWorkflowError(); err != nil {
			t.Fatalf("legitimate execution failed: %v", err)
		}
		h.env.AssertExpectations(t)
		if calls.Load() != 1 {
			t.Fatalf("adapter calls = %d, want 1", calls.Load())
		}
	})
}

// A valid ingress ALLOW is necessary but not sufficient: re-validation against
// the worker's current policy must still deny before any infrastructure
// activity starts.
func TestRevalidationDeniesDespiteIngressAllow(t *testing.T) {
	cases := []struct {
		name   string
		req    contracts.ExecutionRequest
		reason contracts.ReasonCode
	}{
		{"unknown command", request("agent-a", "REBOOT_ALL", "edge-1"), contracts.ReasonCommandUnknown},
		{"unknown target", request("agent-a", contracts.CmdModifyRouting, "shadow-edge"), contracts.ReasonTargetUnregistered},
		{"protected target", request("agent-a", contracts.CmdModifyRouting, contracts.RootCoreEdgeID), contracts.ReasonTargetProtected},
		{"undeclared agent", request("ghost", contracts.CmdModifyRouting, "edge-1"), contracts.ReasonAgentUnknown},
		{"command not permitted for agent", request("agent-b", contracts.CmdDeletePolicy, "edge-1"), contracts.ReasonCommandNotPermitted},
		{"approval required", request("agent-a", contracts.CmdUpdateCert, "prod-1"), contracts.ReasonApprovalRequired},
		{"request hash field tampered", func() contracts.ExecutionRequest {
			r := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			r.RequestHash = "bogus"
			return r
		}(), contracts.ReasonRequestHashMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var infra *workflows.InfrastructureActivities
			h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()
			h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Never()

			h.runAllowed(t, tc.req)

			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			requireRevalidationDenied(t, h.env.GetWorkflowError())
			requireNoInfrastructure(t, h)
			h.env.AssertExpectations(t)

			recs := h.workflowRecords()
			if len(recs) != 1 || recs[0].Verdict == contracts.VerdictAllow || !slices.Contains(recs[0].Reasons, tc.reason) {
				t.Fatalf("expected recorded non-ALLOW with %s, got %+v", tc.reason, recs)
			}
			audits := h.audit.All()
			if len(audits) != 1 || audits[0].Phase != contracts.AuditPhaseRevalidation || audits[0].Verdict != contracts.AuditVerdictFail {
				t.Fatalf("expected one WORKFLOW_REVALIDATION FAIL audit row, got %+v", audits)
			}
		})
	}
}

// A proposal allowed at ingress must be denied if the policy is tightened
// before the workflow runs.
func TestPolicyTightenedBetweenIngressAndExecution(t *testing.T) {
	h := newHarness(t)
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")

	ingress := decision.Evaluate(h.src.Current(), req.AgentID, req.Proposal)
	if ingress.Verdict != contracts.VerdictAllow {
		t.Fatalf("precondition: ingress should ALLOW, got %+v", ingress)
	}

	h.src.p.Store(parsePolicy(t, tightenedPolicy))

	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()
	h.runAllowed(t, req)

	requireRevalidationDenied(t, h.env.GetWorkflowError())
	if h.ran("DispatchConfig") {
		t.Fatal("dispatch ran under a stricter policy")
	}
	recs := h.workflowRecords()
	if len(recs) != 1 || recs[0].Verdict != contracts.VerdictDeny || recs[0].PolicyVersion != "wf-v2" ||
		!slices.Equal(recs[0].Reasons, []contracts.ReasonCode{contracts.ReasonTargetProtected}) {
		t.Fatalf("unexpected re-validation record: %+v", recs)
	}
}

func TestMissingPolicyFailsClosed(t *testing.T) {
	h := newHarness(t)
	h.src.p.Store(nil)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()

	h.runAllowed(t, request("agent-a", contracts.CmdModifyRouting, "edge-1"))

	requireRevalidationDenied(t, h.env.GetWorkflowError())
	if recs := h.workflowRecords(); len(recs) != 1 || !slices.Equal(recs[0].Reasons, []contracts.ReasonCode{contracts.ReasonPolicyUnavailable}) {
		t.Fatalf("expected POLICY_UNAVAILABLE record, got %+v", recs)
	}
}

func TestPartlyAppliedDispatchFailureRunsCompensation(t *testing.T) {
	h := newHarness(t)
	var infra *workflows.InfrastructureActivities
	var calls atomic.Int64
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(
		fencedDispatch(h.decisions, countingAdapter{n: &calls, outcome: failedPartly}, nil)).Once()
	h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Once()

	h.runAllowed(t, request("agent-a", contracts.CmdUpdateCert, contracts.FailingNodeID))

	if !h.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	requireRejected(t, h.env.GetWorkflowError(), contracts.DispatchFailedErrorType)
	h.env.AssertExpectations(t)

	var phases []contracts.AuditPhase
	for _, a := range h.audit.All() {
		phases = append(phases, a.Phase)
	}
	want := []contracts.AuditPhase{
		contracts.AuditPhaseRevalidation, contracts.AuditPhaseDispatch,
		contracts.AuditPhaseCompensation, contracts.AuditPhaseWorkflowFailed,
	}
	if !slices.Equal(phases, want) {
		t.Fatalf("audit phases = %v, want %v", phases, want)
	}
}

// failingAppendStore serves seeded records but fails every new write, so the
// ingress check passes and the re-validation record write fails.
type failingAppendStore struct{ *decision.MemoryStore }

func (failingAppendStore) AppendDecision(context.Context, contracts.DecisionRecord) error {
	return errors.New("disk full")
}

func TestAllowedWorkflowDoesNotDispatchWithoutDecisionRecord(t *testing.T) {
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	store := failingAppendStore{decision.NewMemoryStore()}
	if err := store.MemoryStore.AppendDecision(context.Background(), ingressAllow(req)); err != nil {
		t.Fatal(err)
	}
	h := newHarnessWithStores(t, store, store)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()

	h.run(workflowIDFor(req), req)

	err := h.env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected failure when the re-validation decision cannot be recorded")
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == contracts.IngressDecisionInvalidErrorType {
		t.Fatalf("ingress verification should have passed; got %v", err)
	}
	if !h.ran("EvaluateExecution") {
		t.Fatal("re-validation did not run")
	}
	requireNoInfrastructure(t, h)
	h.env.AssertExpectations(t)
}
