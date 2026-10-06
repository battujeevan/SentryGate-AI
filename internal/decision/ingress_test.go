package decision

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func ingressFixture() (contracts.DecisionRecord, contracts.ExecutionRequest, string) {
	prop := contracts.AgentProposal{ID: "p-1", Type: contracts.CmdModifyRouting, TargetID: "edge-1", Payload: `{"k":"v"}`}
	req := contracts.ExecutionRequest{
		AgentID:           "agent-a",
		Proposal:          prop,
		IngressDecisionID: "dec_1",
		RequestHash:       RequestHash(prop),
	}
	rec := contracts.DecisionRecord{
		DecisionID:  "dec_1",
		Stage:       contracts.StageIngress,
		ProposalID:  "p-1",
		AgentID:     "agent-a",
		RequestHash: RequestHash(prop),
		Verdict:     contracts.VerdictAllow,
		WorkflowID:  "saga-p-1",
		RecordedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	return rec, req, "saga-p-1"
}

func TestVerifyIngress(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(rec *contracts.DecisionRecord, req *contracts.ExecutionRequest, wf *string)
		want   error
		reason contracts.ReasonCode
	}{
		{"valid", func(*contracts.DecisionRecord, *contracts.ExecutionRequest, *string) {}, nil, ""},
		{"record ID differs from request", func(_ *contracts.DecisionRecord, req *contracts.ExecutionRequest, _ *string) {
			req.IngressDecisionID = "dec_2"
		}, ErrIngressDecisionNotFound, contracts.ReasonIngressDecisionNotFound},
		{"empty record", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			*rec = contracts.DecisionRecord{}
		}, ErrIngressDecisionNotFound, contracts.ReasonIngressDecisionNotFound},
		{"wrong stage", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			rec.Stage = contracts.StageWorkflowRevalidation
		}, ErrIngressWrongStage, contracts.ReasonIngressDecisionWrongStage},
		{"deny verdict", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			rec.Verdict = contracts.VerdictDeny
		}, ErrIngressNotAllowed, contracts.ReasonIngressDecisionNotAllow},
		{"require approval verdict", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			rec.Verdict = contracts.VerdictRequireApproval
		}, ErrIngressNotAllowed, contracts.ReasonIngressDecisionNotAllow},
		{"agent mismatch", func(_ *contracts.DecisionRecord, req *contracts.ExecutionRequest, _ *string) {
			req.AgentID = "agent-b"
		}, ErrIngressAgentMismatch, contracts.ReasonIngressAgentMismatch},
		{"payload changed with consistent request hash", func(_ *contracts.DecisionRecord, req *contracts.ExecutionRequest, _ *string) {
			req.Proposal.Payload = `{"k":"x"}`
			req.RequestHash = RequestHash(req.Proposal)
		}, ErrIngressHashMismatch, contracts.ReasonIngressHashMismatch},
		{"recorded hash differs", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			rec.RequestHash = "abc"
		}, ErrIngressHashMismatch, contracts.ReasonIngressHashMismatch},
		{"proposal ID mismatch", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, _ *string) {
			rec.ProposalID = "p-2"
		}, ErrIngressIdentityMismatch, contracts.ReasonIngressIdentityMismatch},
		{"workflow ID mismatch", func(_ *contracts.DecisionRecord, _ *contracts.ExecutionRequest, wf *string) {
			*wf = "saga-other"
		}, ErrIngressIdentityMismatch, contracts.ReasonIngressIdentityMismatch},
		{"recorded workflow ID empty", func(rec *contracts.DecisionRecord, _ *contracts.ExecutionRequest, wf *string) {
			rec.WorkflowID = ""
			*wf = ""
		}, ErrIngressIdentityMismatch, contracts.ReasonIngressIdentityMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, req, wf := ingressFixture()
			tc.mutate(&rec, &req, &wf)
			err := VerifyIngress(rec, req, wf)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("expected success, got %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if got, ok := IngressReason(err); !ok || got != tc.reason {
				t.Fatalf("IngressReason = %q, %v; want %q", got, ok, tc.reason)
			}
		})
	}
}

type errStore struct{ *MemoryStore }

func (errStore) GetDecision(context.Context, string) (contracts.DecisionRecord, error) {
	return contracts.DecisionRecord{}, errors.New("io failure")
}

func TestCheckIngress(t *testing.T) {
	ctx := context.Background()
	rec, req, wf := ingressFixture()
	s := NewMemoryStore()

	if err := CheckIngress(ctx, s, req, wf); !errors.Is(err, ErrIngressDecisionNotFound) {
		t.Fatalf("unrecorded decision: err = %v", err)
	}
	empty := req
	empty.IngressDecisionID = ""
	if err := CheckIngress(ctx, s, empty, wf); !errors.Is(err, ErrIngressDecisionNotFound) {
		t.Fatalf("empty decision ID: err = %v", err)
	}

	if err := s.AppendDecision(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := CheckIngress(ctx, s, req, wf); err != nil {
		t.Fatalf("recorded ALLOW: err = %v", err)
	}

	err := CheckIngress(ctx, errStore{s}, req, wf)
	if err == nil {
		t.Fatal("store failure must not verify")
	}
	if _, ok := IngressReason(err); ok {
		t.Fatalf("store failure misreported as a verification result: %v", err)
	}
}
