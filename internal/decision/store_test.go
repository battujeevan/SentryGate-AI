package decision_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/decision/decisiontest"
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

func TestMemoryStoreGetDecision(t *testing.T) {
	ctx := context.Background()
	s := decision.NewMemoryStore()
	if _, err := s.GetDecision(ctx, "dec_1"); !errors.Is(err, contracts.ErrDecisionNotFound) {
		t.Fatalf("err = %v, want ErrDecisionNotFound", err)
	}
	rec := sampleRecord()
	if err := s.AppendDecision(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDecision(ctx, rec.DecisionID)
	if err != nil || !got.Equal(rec) {
		t.Fatalf("GetDecision = %+v, %v", got, err)
	}
	got.Reasons[0] = "MUTATED"
	if again, _ := s.GetDecision(ctx, rec.DecisionID); !again.Equal(rec) {
		t.Fatal("GetDecision exposed the stored reasons slice")
	}
}

func TestMemoryStoreClaims(t *testing.T) {
	decisiontest.RunClaimStoreTests(t, func(*testing.T) decisiontest.Stores { return decision.NewMemoryStore() })
}

func TestExecutionKeyDependsOnlyOnDecision(t *testing.T) {
	if decision.ExecutionKey("dec_1") != decision.ExecutionKey("dec_1") {
		t.Fatal("key is not deterministic")
	}
	if decision.ExecutionKey("dec_1") == decision.ExecutionKey("dec_2") {
		t.Fatal("different decisions share a key")
	}
	if got, want := decision.ExecutionKey("dec_1"), "sentrygate.exec.v1:dec_1"; got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

func TestMemoryStoreRejectsIncompleteRecord(t *testing.T) {
	rec := sampleRecord()
	rec.RequestHash = ""
	if err := decision.NewMemoryStore().AppendDecision(context.Background(), rec); !errors.Is(err, contracts.ErrInvalidDecisionRecord) {
		t.Fatalf("expected ErrInvalidDecisionRecord, got %v", err)
	}
}
