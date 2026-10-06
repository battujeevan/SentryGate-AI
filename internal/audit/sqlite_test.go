package audit_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func openStore(t *testing.T) *audit.Store {
	t.Helper()
	store, err := audit.Open(filepath.Join(t.TempDir(), "audit.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestSQLiteStore_AppendAndList(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	rec := contracts.AuditRecord{
		RecordID:   "r1",
		ProposalID: "p1",
		WorkflowID: "w1",
		RunID:      "run1",
		Phase:      contracts.AuditPhaseDispatch,
		Verdict:    contracts.AuditVerdictPass,
		TargetID:   "edge",
		Command:    contracts.CmdModifyRouting,
		Detail:     "ok",
		RecordedAt: time.Now(),
	}
	if err := store.Append(ctx, rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := store.Append(ctx, rec); err != nil {
		t.Fatalf("identical re-append must succeed: %v", err)
	}
	changed := rec
	changed.Detail = "different"
	if err := store.Append(ctx, changed); !errors.Is(err, contracts.ErrAuditPersistFailed) {
		t.Fatalf("expected conflict on changed content, got %v", err)
	}
	rows, err := store.ListByProposal(ctx, "p1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].RecordID != "r1" || rows[0].Detail != "ok" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
}

func TestSQLiteStore_DecisionRoundTripAndIdempotency(t *testing.T) {
	store := openStore(t)
	ctx := context.Background()
	rec := contracts.DecisionRecord{
		DecisionID:    "dec_1",
		Stage:         contracts.StageIngress,
		ProposalID:    "p-1",
		AgentID:       "agent-a",
		RequestHash:   strings.Repeat("ab", 32),
		Command:       contracts.CmdDeletePolicy,
		TargetID:      contracts.RootCoreEdgeID,
		Environment:   "production",
		Verdict:       contracts.VerdictDeny,
		Reasons:       []contracts.ReasonCode{contracts.ReasonAgentUnknown, contracts.ReasonTargetProtected},
		PolicyVersion: "v1",
		PolicyDigest:  strings.Repeat("cd", 32),
		TraceID:       "trace-1",
		RecordedAt:    time.Now(),
	}
	if err := store.AppendDecision(ctx, rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := store.AppendDecision(ctx, rec); err != nil {
		t.Fatalf("identical re-append must succeed: %v", err)
	}
	changed := rec
	changed.Verdict = contracts.VerdictAllow
	if err := store.AppendDecision(ctx, changed); !errors.Is(err, contracts.ErrDecisionConflict) {
		t.Fatalf("expected ErrDecisionConflict, got %v", err)
	}

	second := rec
	second.DecisionID = "wf_saga-p-1_run"
	second.Stage = contracts.StageWorkflowRevalidation
	if err := store.AppendDecision(ctx, second); err != nil {
		t.Fatal(err)
	}

	rows, err := store.ListDecisionsByProposal(ctx, "p-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || !rows[0].Equal(rec) || !rows[1].Equal(second) {
		t.Fatalf("round trip mismatch: %+v", rows)
	}

	got, err := store.GetDecision(ctx, rec.DecisionID)
	if err != nil || !got.Equal(rec) {
		t.Fatalf("GetDecision = %+v, %v", got, err)
	}
	if _, err := store.GetDecision(ctx, "dec_missing"); !errors.Is(err, contracts.ErrDecisionNotFound) {
		t.Fatalf("GetDecision(missing) err = %v, want ErrDecisionNotFound", err)
	}
	if empty, err := store.ListDecisionsByProposal(ctx, "none"); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("expected empty non-nil slice, got %#v err=%v", empty, err)
	}
}

func TestSQLiteStore_RejectsIncompleteDecision(t *testing.T) {
	store := openStore(t)
	err := store.AppendDecision(context.Background(), contracts.DecisionRecord{DecisionID: "d"})
	if !errors.Is(err, contracts.ErrInvalidDecisionRecord) {
		t.Fatalf("expected ErrInvalidDecisionRecord, got %v", err)
	}
}

func TestSQLiteStore_UsesWALAndBusyTimeout(t *testing.T) {
	store := openStore(t)
	mode, timeout, err := store.Pragmas(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(mode, "wal") {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
	if timeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", timeout)
	}
}
