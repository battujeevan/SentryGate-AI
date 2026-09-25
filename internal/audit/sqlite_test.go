package audit_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func TestSQLiteStore_AppendAndList(t *testing.T) {
	dir := t.TempDir()
	store, err := audit.Open(filepath.Join(dir, "audit.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()

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
		RecordedAt: time.Now().UTC(),
	}
	if err := store.Append(ctx, rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	rows, err := store.ListByProposal(ctx, "p1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 || rows[0].RecordID != "r1" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
