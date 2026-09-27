package workflows_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
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
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-1, environment: staging, protected: false}
  - {id: prod-1, environment: production, protected: false}
  - {id: FAILING_NODE, environment: staging, protected: false}
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
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-1, environment: staging, protected: true}
  - {id: prod-1, environment: production, protected: false}
  - {id: FAILING_NODE, environment: staging, protected: false}
`

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
	var ts testsuite.WorkflowTestSuite
	h := &harness{
		env:       ts.NewTestWorkflowEnvironment(),
		src:       &swappableSource{},
		decisions: decision.NewMemoryStore(),
		audit:     workflows.NewMemoryAuditStore(),
	}
	h.src.p.Store(parsePolicy(t, workflowPolicy))
	h.env.RegisterActivity(&workflows.DecisionActivities{Policy: h.src, Store: h.decisions})
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

func request(agentID string, cmd contracts.CommandType, target string) contracts.ExecutionRequest {
	prop := contracts.AgentProposal{ID: "prop-" + target, Type: cmd, TargetID: target, Payload: `{"k":"v"}`}
	return contracts.ExecutionRequest{
		AgentID:           agentID,
		Proposal:          prop,
		IngressDecisionID: "dec_test",
		RequestHash:       decision.RequestHash(prop),
	}
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

func TestAllowedWorkflowDispatches(t *testing.T) {
	h := newHarness(t)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil).Once()

	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, req)

	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not complete cleanly: %v", h.env.GetWorkflowError())
	}
	h.env.AssertExpectations(t)

	recs := h.decisions.All()
	if len(recs) != 1 {
		t.Fatalf("expected one re-validation decision, got %+v", recs)
	}
	r := recs[0]
	if r.Stage != contracts.StageWorkflowRevalidation || r.Verdict != contracts.VerdictAllow ||
		r.AgentID != "agent-a" || r.PolicyVersion != "wf-v1" || r.RequestHash != req.RequestHash {
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

// Direct submissions to Temporal bypass the HTTP proxy. Re-validation must
// deny them before any infrastructure activity starts.
func TestDirectWorkflowStartIsDeniedAtRevalidation(t *testing.T) {
	cases := []struct {
		name   string
		req    contracts.ExecutionRequest
		reason contracts.ReasonCode
	}{
		{"unknown command", request("agent-a", "REBOOT_ALL", "edge-1"), contracts.ReasonCommandUnknown},
		{"unknown target", request("agent-a", contracts.CmdModifyRouting, "shadow-edge"), contracts.ReasonTargetUnregistered},
		{"protected target", request("agent-a", contracts.CmdModifyRouting, contracts.RootCoreEdgeID), contracts.ReasonTargetProtected},
		{"undeclared agent", request("ghost", contracts.CmdModifyRouting, "edge-1"), contracts.ReasonAgentUnknown},
		{"approval required", request("agent-a", contracts.CmdUpdateCert, "prod-1"), contracts.ReasonApprovalRequired},
		{"tampered payload", func() contracts.ExecutionRequest {
			r := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			r.Proposal.Payload = `{"k":"tampered"}`
			return r
		}(), contracts.ReasonRequestHashMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var infra *workflows.InfrastructureActivities
			h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil).Never()
			h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Never()

			h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, tc.req)

			if !h.env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			requireRevalidationDenied(t, h.env.GetWorkflowError())
			if h.ran("DispatchConfig") || h.ran("RevertStateCompensation") {
				t.Fatal("infrastructure activity ran after re-validation denial")
			}
			h.env.AssertExpectations(t)

			recs := h.decisions.All()
			if len(recs) != 1 || recs[0].Stage != contracts.StageWorkflowRevalidation ||
				recs[0].Verdict == contracts.VerdictAllow || !slices.Contains(recs[0].Reasons, tc.reason) {
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
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil).Never()
	h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, req)

	requireRevalidationDenied(t, h.env.GetWorkflowError())
	if h.ran("DispatchConfig") {
		t.Fatal("dispatch ran under a stricter policy")
	}
	recs := h.decisions.All()
	if len(recs) != 1 || recs[0].Verdict != contracts.VerdictDeny || recs[0].PolicyVersion != "wf-v2" ||
		!slices.Equal(recs[0].Reasons, []contracts.ReasonCode{contracts.ReasonTargetProtected}) {
		t.Fatalf("unexpected re-validation record: %+v", recs)
	}
}

func TestMissingPolicyFailsClosed(t *testing.T) {
	h := newHarness(t)
	h.src.p.Store(nil)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil).Never()

	h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, request("agent-a", contracts.CmdModifyRouting, "edge-1"))

	requireRevalidationDenied(t, h.env.GetWorkflowError())
	if recs := h.decisions.All(); len(recs) != 1 || !slices.Equal(recs[0].Reasons, []contracts.ReasonCode{contracts.ReasonPolicyUnavailable}) {
		t.Fatalf("expected POLICY_UNAVAILABLE record, got %+v", recs)
	}
}

func TestDispatchFailureRunsCompensation(t *testing.T) {
	h := newHarness(t)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(contracts.NonRetryableInfraError).Once()
	h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil).Once()

	h.env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, request("agent-a", contracts.CmdUpdateCert, contracts.FailingNodeID))

	if !h.env.IsWorkflowCompleted() || h.env.GetWorkflowError() == nil {
		t.Fatal("expected workflow to fail after dispatch error")
	}
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

// failingDecisionStore makes the re-validation record write fail.
type failingDecisionStore struct{}

func (failingDecisionStore) AppendDecision(context.Context, contracts.DecisionRecord) error {
	return errors.New("disk full")
}

func (failingDecisionStore) ListDecisionsByProposal(context.Context, string) ([]contracts.DecisionRecord, error) {
	return nil, nil
}

func TestAllowedWorkflowDoesNotDispatchWithoutDecisionRecord(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(&workflows.DecisionActivities{
		Policy: policy.StaticSource(parsePolicy(t, workflowPolicy)),
		Store:  failingDecisionStore{},
	})
	env.RegisterActivity(&workflows.AuditActivities{Store: workflows.NewMemoryAuditStore()})
	var infra *workflows.InfrastructureActivities
	env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(nil).Never()

	env.ExecuteWorkflow(workflows.SentryGateSagaWorkflow, request("agent-a", contracts.CmdModifyRouting, "edge-1"))

	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected failure when the re-validation decision cannot be recorded")
	}
	env.AssertExpectations(t)
}
