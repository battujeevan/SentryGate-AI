package workflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func TestMemoryAuditStore_AppendAndList(t *testing.T) {
	store := workflows.NewMemoryAuditStore()
	ctx := context.Background()

	rec := contracts.AuditRecord{
		RecordID:   "r-1",
		ProposalID: "prop-9",
		WorkflowID: "wf-9",
		RunID:      "run-9",
		Phase:      contracts.AuditPhaseIngressValidation,
		Verdict:    contracts.AuditVerdictPass,
		TargetID:   "edge-a",
		Command:    contracts.CmdModifyRouting,
		Detail:     "accepted",
		RecordedAt: time.Now().UTC(),
	}
	if err := store.Append(ctx, rec); err != nil {
		t.Fatalf("append: %v", err)
	}

	rows, err := store.ListByProposal(ctx, "prop-9")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Phase != contracts.AuditPhaseIngressValidation {
		t.Fatalf("unexpected phase %s", rows[0].Phase)
	}
}

func TestAuditActivities_RecordAuditTrail(t *testing.T) {
	store := workflows.NewMemoryAuditStore()
	acts := &workflows.AuditActivities{Store: store}

	err := acts.RecordAuditTrail(context.Background(), contracts.AuditRecord{
		RecordID:   "r-2",
		ProposalID: "prop-10",
		Phase:      contracts.AuditPhaseDispatch,
		Verdict:    contracts.AuditVerdictPass,
	})
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if got := len(store.All()); got != 1 {
		t.Fatalf("expected 1 audit row, got %d", got)
	}
}
