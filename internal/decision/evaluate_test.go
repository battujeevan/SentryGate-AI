package decision_test

import (
	"slices"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

const testPolicy = `version: v1
max_parallel_tasks: 4
environments:
  staging: allow
  production: require_approval
  lab: deny
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - id: agent-a
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
  - id: agent-routing-only
    commands: [MODIFY_ROUTING]
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: stage-protected, environment: staging, protected: true}
  - {id: edge-1, environment: staging, protected: false}
  - {id: prod-1, environment: production, protected: false}
  - {id: lab-1, environment: lab, protected: false}
`

func mustSnapshot(t *testing.T) *policy.Snapshot {
	t.Helper()
	s, err := policy.Parse([]byte(testPolicy))
	if err != nil {
		t.Fatalf("parse test policy: %v", err)
	}
	return s
}

func TestEvaluate(t *testing.T) {
	s := mustSnapshot(t)
	cases := []struct {
		name    string
		agent   string
		cmd     contracts.CommandType
		target  string
		verdict contracts.Verdict
		reasons []contracts.ReasonCode
	}{
		{"allowed staging", "agent-a", contracts.CmdModifyRouting, "edge-1",
			contracts.VerdictAllow, []contracts.ReasonCode{contracts.ReasonEnvironmentAllowed}},
		{"production requires approval", "agent-a", contracts.CmdUpdateCert, "prod-1",
			contracts.VerdictRequireApproval, []contracts.ReasonCode{contracts.ReasonApprovalRequired}},
		{"environment deny", "agent-a", contracts.CmdModifyRouting, "lab-1",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonEnvironmentDenied}},
		{"unknown command", "agent-a", "REBOOT_ALL", "edge-1",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonCommandUnknown}},
		{"unknown target", "agent-a", contracts.CmdModifyRouting, "nowhere",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonTargetUnregistered}},
		{"protected delete", "agent-a", contracts.CmdDeletePolicy, contracts.RootCoreEdgeID,
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonTargetProtected}},
		{"protected modify routing", "agent-a", contracts.CmdModifyRouting, contracts.RootCoreEdgeID,
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonTargetProtected}},
		{"protected update certificate", "agent-a", contracts.CmdUpdateCert, contracts.RootCoreEdgeID,
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonTargetProtected}},
		{"protected in allow environment", "agent-a", contracts.CmdModifyRouting, "stage-protected",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonTargetProtected}},
		{"undeclared agent", "ghost", contracts.CmdModifyRouting, "edge-1",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonAgentUnknown}},
		{"command not permitted for agent", "agent-routing-only", contracts.CmdUpdateCert, "edge-1",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonCommandNotPermitted}},
		{"reasons collected in precedence order", "ghost", "REBOOT_ALL", contracts.RootCoreEdgeID,
			contracts.VerdictDeny, []contracts.ReasonCode{
				contracts.ReasonAgentUnknown, contracts.ReasonCommandUnknown, contracts.ReasonTargetProtected}},
		{"deny beats approval", "agent-routing-only", contracts.CmdDeletePolicy, "prod-1",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonCommandNotPermitted}},
		{"unregistered target and unknown command", "agent-a", "", "",
			contracts.VerdictDeny, []contracts.ReasonCode{contracts.ReasonCommandUnknown, contracts.ReasonTargetUnregistered}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := decision.Evaluate(s, tc.agent, contracts.AgentProposal{ID: "p", Type: tc.cmd, TargetID: tc.target})
			if d.Verdict != tc.verdict {
				t.Fatalf("verdict = %s, want %s (reasons %v)", d.Verdict, tc.verdict, d.Reasons)
			}
			if !slices.Equal(d.Reasons, tc.reasons) {
				t.Fatalf("reasons = %v, want %v", d.Reasons, tc.reasons)
			}
			if d.PolicyVersion != "v1" || d.PolicyDigest != s.Digest() {
				t.Fatalf("policy identity not attached: %+v", d)
			}
		})
	}
}

func TestEvaluateNilSnapshotFailsClosed(t *testing.T) {
	d := decision.Evaluate(nil, "agent-a", contracts.AgentProposal{ID: "p", Type: contracts.CmdModifyRouting, TargetID: "edge-1"})
	if d.Verdict != contracts.VerdictDeny || !slices.Equal(d.Reasons, []contracts.ReasonCode{contracts.ReasonPolicyUnavailable}) {
		t.Fatalf("expected POLICY_UNAVAILABLE deny, got %+v", d)
	}
}

func TestRevalidateDetectsHashMismatch(t *testing.T) {
	s := mustSnapshot(t)
	prop := contracts.AgentProposal{ID: "p", Type: contracts.CmdModifyRouting, TargetID: "edge-1", Payload: `{"a":1}`}
	req := contracts.ExecutionRequest{AgentID: "agent-a", Proposal: prop, RequestHash: decision.RequestHash(prop)}

	if d := decision.Revalidate(s, req); d.Verdict != contracts.VerdictAllow {
		t.Fatalf("expected ALLOW with matching hash, got %+v", d)
	}

	req.Proposal.Payload = `{"a":2}`
	d := decision.Revalidate(s, req)
	if d.Verdict != contracts.VerdictDeny || !slices.Equal(d.Reasons, []contracts.ReasonCode{contracts.ReasonRequestHashMismatch}) {
		t.Fatalf("expected REQUEST_HASH_MISMATCH deny, got %+v", d)
	}

	req.Proposal.TargetID = contracts.RootCoreEdgeID
	d = decision.Revalidate(s, req)
	want := []contracts.ReasonCode{contracts.ReasonTargetProtected, contracts.ReasonRequestHashMismatch}
	if d.Verdict != contracts.VerdictDeny || !slices.Equal(d.Reasons, want) {
		t.Fatalf("expected %v, got %+v", want, d)
	}
}

func TestNewDecisionIDIsUnique(t *testing.T) {
	a, b := decision.NewDecisionID(), decision.NewDecisionID()
	if a == b || len(a) < 20 {
		t.Fatalf("weak decision IDs: %q %q", a, b)
	}
}
