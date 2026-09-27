package decision_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func sampleRecord() contracts.DecisionRecord {
	return contracts.DecisionRecord{
		DecisionID:    "dec_1",
		Stage:         contracts.StageIngress,
		ProposalID:    "p-1",
		AgentID:       "agent-a",
		RequestHash:   "abc",
		Command:       contracts.CmdModifyRouting,
		TargetID:      "edge-1",
		Environment:   "staging",
		Verdict:       contracts.VerdictAllow,
		Reasons:       []contracts.ReasonCode{contracts.ReasonEnvironmentAllowed},
		PolicyVersion: "v1",
		PolicyDigest:  "digest",
		WorkflowID:    "saga-p-1",
		RecordedAt:    time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
	}
}

func TestMemoryStoreIdempotentAppend(t *testing.T) {
	ctx := context.Background()
	s := decision.NewMemoryStore()
	rec := sampleRecord()
	if err := s.AppendDecision(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDecision(ctx, rec); err != nil {
		t.Fatalf("identical re-append must succeed: %v", err)
	}
	changed := rec
	changed.Verdict = contracts.VerdictDeny
	if err := s.AppendDecision(ctx, changed); !errors.Is(err, contracts.ErrDecisionConflict) {
		t.Fatalf("expected ErrDecisionConflict, got %v", err)
	}
	rows, err := s.ListDecisionsByProposal(ctx, "p-1")
	if err != nil || len(rows) != 1 || !rows[0].Equal(rec) {
		t.Fatalf("unexpected rows %+v err=%v", rows, err)
	}
}

func TestMemoryStoreRejectsIncompleteRecord(t *testing.T) {
	rec := sampleRecord()
	rec.RequestHash = ""
	if err := decision.NewMemoryStore().AppendDecision(context.Background(), rec); !errors.Is(err, contracts.ErrInvalidDecisionRecord) {
		t.Fatalf("expected ErrInvalidDecisionRecord, got %v", err)
	}
}
