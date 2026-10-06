package audit_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/internal/decision/decisiontest"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func TestSQLiteStore_Claims(t *testing.T) {
	decisiontest.RunClaimStoreTests(t, func(t *testing.T) decisiontest.Stores { return openStore(t) })
}

// Each Store has its own connection pool, like the proxy and worker
// processes, so only SQLite's own locking can serialize these claims.
func TestSQLiteStore_ClaimIsAtomicAcrossConnections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.db")
	const n = 8
	stores := make([]*audit.Store, n)
	for i := range stores {
		s, err := audit.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		stores[i] = s
	}
	ctx := context.Background()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	for round := range 5 {
		id := fmt.Sprintf("dec_%d", round)
		if err := stores[0].AppendDecision(ctx, contracts.DecisionRecord{
			DecisionID: id, Stage: contracts.StageIngress, ProposalID: "p", AgentID: "agent-a",
			RequestHash: "h", Verdict: contracts.VerdictAllow, WorkflowID: "saga-p", RecordedAt: at,
		}); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, n)
		for i, s := range stores {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				o := contracts.ClaimOwner{WorkflowID: "saga-p", RunID: fmt.Sprintf("run-%d", i), Token: fmt.Sprintf("exe_%d", i)}
				errs[i] = s.ClaimExecution(ctx, id, o, at)
			}()
		}
		close(start)
		wg.Wait()
		decisiontest.RequireOneWinner(t, errs)
	}
}

func TestSQLiteStore_ClaimSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.db")
	ctx := context.Background()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	o := contracts.ClaimOwner{WorkflowID: "saga-p", RunID: "run-1", Token: "exe_1"}

	s, err := audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendDecision(ctx, contracts.DecisionRecord{
		DecisionID: "dec_1", Stage: contracts.StageIngress, ProposalID: "p", AgentID: "agent-a",
		RequestHash: "h", Verdict: contracts.VerdictAllow, WorkflowID: "saga-p", RecordedAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimExecution(ctx, "dec_1", o, at); err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceClaim(ctx, "dec_1", o, contracts.ClaimStateExecuting, at); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()

	s2, err := audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	c, err := s2.GetClaim(ctx, "dec_1")
	if err != nil || c.State != contracts.ClaimStateExecuting || c.Owner != o || !c.ClaimedAt.Equal(at) {
		t.Fatalf("claim after reopen = %+v, %v", c, err)
	}
	other := contracts.ClaimOwner{WorkflowID: "saga-p", RunID: "run-2", Token: "exe_2"}
	if err := s2.ClaimExecution(ctx, "dec_1", other, at); err == nil {
		t.Fatal("decision claimed twice across a restart")
	}
}
