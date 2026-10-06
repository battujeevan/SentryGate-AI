// Package decisiontest holds tests shared by decision.ClaimStore
// implementations.
package decisiontest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Stores is a decision store that also enforces execution claims.
type Stores interface {
	decision.Store
	decision.ClaimStore
}

var at = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func seedIngress(t *testing.T, s Stores, id string) {
	t.Helper()
	rec := contracts.DecisionRecord{
		DecisionID: id, Stage: contracts.StageIngress, ProposalID: "p-" + id, AgentID: "agent-a",
		RequestHash: "hash-" + id, Verdict: contracts.VerdictAllow, WorkflowID: "saga-p-" + id, RecordedAt: at,
	}
	if err := s.AppendDecision(context.Background(), rec); err != nil {
		t.Fatalf("seed %s: %v", id, err)
	}
}

func owner(run string) contracts.ClaimOwner {
	return contracts.ClaimOwner{WorkflowID: "saga-p-dec_1", RunID: run, Token: "exe_" + run}
}

func requireState(t *testing.T, s Stores, id string, want contracts.ClaimState, wantOwner contracts.ClaimOwner) {
	t.Helper()
	c, err := s.GetClaim(context.Background(), id)
	if err != nil {
		t.Fatalf("GetClaim(%s): %v", id, err)
	}
	if c.State != want || c.Owner != wantOwner || c.DecisionID != id {
		t.Fatalf("claim = %+v, want state %s owner %+v", c, want, wantOwner)
	}
}

// RunClaimStoreTests checks the claim state machine against newStore.
func RunClaimStoreTests(t *testing.T, newStore func(t *testing.T) Stores) {
	ctx := context.Background()

	t.Run("unknown decision cannot be claimed", func(t *testing.T) {
		s := newStore(t)
		if err := s.ClaimExecution(ctx, "dec_missing", owner("run-1"), at); !errors.Is(err, contracts.ErrDecisionNotFound) {
			t.Fatalf("err = %v, want ErrDecisionNotFound", err)
		}
		if _, err := s.GetClaim(ctx, "dec_missing"); !errors.Is(err, contracts.ErrClaimNotFound) {
			t.Fatalf("GetClaim err = %v, want ErrClaimNotFound", err)
		}
	})

	t.Run("incomplete owner is rejected", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		bad := owner("run-1")
		bad.Token = ""
		if err := s.ClaimExecution(ctx, "dec_1", bad, at); err == nil {
			t.Fatal("claim with empty token succeeded")
		}
	})

	t.Run("full lifecycle with idempotent retries", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a := owner("run-1")

		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatalf("same-owner re-claim must be idempotent: %v", err)
		}
		requireState(t, s, "dec_1", contracts.ClaimStateClaimed, a)

		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateCompleted, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("CLAIMED -> COMPLETED must be refused, got %v", err)
		}
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateExecuting, at); err != nil {
			t.Fatalf("-> EXECUTING: %v", err)
		}
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateExecuting, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("the execution fence must not be acquired twice, even by the owner, got %v", err)
		}
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReleased, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("EXECUTING -> RELEASED must be refused, got %v", err)
		}
		if err := s.ClaimExecution(ctx, "dec_1", a, at); !errors.Is(err, contracts.ErrClaimHeld) {
			t.Fatalf("re-claim after EXECUTING must be refused even for the owner, got %v", err)
		}
		for range 2 {
			if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateCompleted, at); err != nil {
				t.Fatalf("-> COMPLETED: %v", err)
			}
		}
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateFailed, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("COMPLETED -> FAILED must be refused, got %v", err)
		}
		requireState(t, s, "dec_1", contracts.ClaimStateCompleted, a)
		if err := s.ClaimExecution(ctx, "dec_1", owner("run-2"), at); !errors.Is(err, contracts.ErrClaimHeld) {
			t.Fatalf("claim of a COMPLETED decision must be refused, got %v", err)
		}
	})

	t.Run("failed dispatch is terminal", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a := owner("run-1")
		mustAdvance(t, s, a, contracts.ClaimStateExecuting, contracts.ClaimStateFailed)
		if err := s.ClaimExecution(ctx, "dec_1", owner("run-2"), at); !errors.Is(err, contracts.ErrClaimHeld) {
			t.Fatalf("claim of a FAILED decision must be refused, got %v", err)
		}
		requireState(t, s, "dec_1", contracts.ClaimStateFailed, a)
	})

	t.Run("unknown outcome is held until reconciled", func(t *testing.T) {
		for _, final := range []contracts.ClaimState{contracts.ClaimStateCompleted, contracts.ClaimStateFailed} {
			t.Run(string(final), func(t *testing.T) {
				s := newStore(t)
				seedIngress(t, s, "dec_1")
				a := owner("run-1")
				if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
					t.Fatal(err)
				}
				if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReconciliationRequired, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("CLAIMED -> RECONCILIATION_REQUIRED must be refused, got %v", err)
				}
				mustTransition(t, s, a, contracts.ClaimStateExecuting)
				for range 2 {
					mustTransition(t, s, a, contracts.ClaimStateReconciliationRequired)
				}

				if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReleased, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("RECONCILIATION_REQUIRED -> RELEASED must be refused, got %v", err)
				}
				for _, o := range []contracts.ClaimOwner{a, owner("run-2")} {
					if err := s.ClaimExecution(ctx, "dec_1", o, at); !errors.Is(err, contracts.ErrClaimHeld) {
						t.Fatalf("claim of an unreconciled decision by %s must be refused, got %v", o.RunID, err)
					}
				}
				if err := s.AdvanceClaim(ctx, "dec_1", owner("run-2"), final, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("another execution resolved the claim: %v", err)
				}
				requireState(t, s, "dec_1", contracts.ClaimStateReconciliationRequired, a)

				for range 2 {
					mustTransition(t, s, a, final)
				}
				if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReconciliationRequired, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("%s -> RECONCILIATION_REQUIRED must be refused, got %v", final, err)
				}
				requireState(t, s, "dec_1", final, a)
			})
		}
	})

	t.Run("execution fence is entered only from CLAIMED", func(t *testing.T) {
		a := owner("run-1")
		cases := map[string][]contracts.ClaimState{
			"RELEASED":                {contracts.ClaimStateReleased},
			"EXECUTING":               {contracts.ClaimStateExecuting},
			"RECONCILIATION_REQUIRED": {contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired},
			"COMPLETED":               {contracts.ClaimStateExecuting, contracts.ClaimStateCompleted},
			"FAILED":                  {contracts.ClaimStateExecuting, contracts.ClaimStateFailed},
		}
		for name, path := range cases {
			t.Run(name, func(t *testing.T) {
				s := newStore(t)
				seedIngress(t, s, "dec_1")
				mustAdvance(t, s, a, path...)
				if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateExecuting, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("%s -> EXECUTING err = %v, want ErrClaimNotHeld", name, err)
				}
				requireState(t, s, "dec_1", path[len(path)-1], a)
			})
		}
	})

	t.Run("concurrent fences by the owner: exactly one wins", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a := owner("run-1")
		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatal(err)
		}
		const n = 16
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs[i] = s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateExecuting, at)
			}()
		}
		close(start)
		wg.Wait()
		winners := 0
		for i, err := range errs {
			switch {
			case err == nil:
				winners++
			case !errors.Is(err, contracts.ErrClaimNotHeld):
				t.Fatalf("fence %d: unexpected error %v", i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("%d concurrent fences succeeded, want exactly 1", winners)
		}
		requireState(t, s, "dec_1", contracts.ClaimStateExecuting, a)
	})

	t.Run("other executions cannot claim or advance", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a := owner("run-1")
		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatal(err)
		}
		otherRun := owner("run-2")
		sameRunOtherToken := a
		sameRunOtherToken.Token = "exe_other"
		resetRun := a // a Temporal reset keeps history (and the token) but gets a new run ID
		resetRun.RunID = "run-reset"
		for name, o := range map[string]contracts.ClaimOwner{
			"other run": otherRun, "same run, other token": sameRunOtherToken, "reset run": resetRun,
		} {
			if err := s.ClaimExecution(ctx, "dec_1", o, at); !errors.Is(err, contracts.ErrClaimHeld) {
				t.Fatalf("%s: claim err = %v, want ErrClaimHeld", name, err)
			}
			for _, to := range []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateReleased} {
				if err := s.AdvanceClaim(ctx, "dec_1", o, to, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
					t.Fatalf("%s: -> %s err = %v, want ErrClaimNotHeld", name, to, err)
				}
			}
		}
		requireState(t, s, "dec_1", contracts.ClaimStateClaimed, a)
	})

	t.Run("released claim can be taken by another execution", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a, b := owner("run-1"), owner("run-2")
		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReleased, at); err != nil {
				t.Fatalf("release: %v", err)
			}
		}
		if err := s.ClaimExecution(ctx, "dec_1", b, at); err != nil {
			t.Fatalf("claim after release: %v", err)
		}
		requireState(t, s, "dec_1", contracts.ClaimStateClaimed, b)
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateExecuting, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("previous owner advanced a claim it released: %v", err)
		}
		if err := s.AdvanceClaim(ctx, "dec_1", a, contracts.ClaimStateReleased, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
			t.Fatalf("previous owner released the new owner's claim: %v", err)
		}
	})

	t.Run("invalid target state", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		a := owner("run-1")
		if err := s.ClaimExecution(ctx, "dec_1", a, at); err != nil {
			t.Fatal(err)
		}
		for _, to := range []contracts.ClaimState{contracts.ClaimStateClaimed, "BOGUS"} {
			if err := s.AdvanceClaim(ctx, "dec_1", a, to, at); !errors.Is(err, contracts.ErrClaimNotHeld) {
				t.Fatalf("-> %q err = %v, want ErrClaimNotHeld", to, err)
			}
		}
	})

	t.Run("independent decisions are claimed independently", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		seedIngress(t, s, "dec_2")
		if err := s.ClaimExecution(ctx, "dec_1", owner("run-1"), at); err != nil {
			t.Fatal(err)
		}
		if err := s.ClaimExecution(ctx, "dec_2", owner("run-2"), at); err != nil {
			t.Fatalf("claim on dec_1 blocked dec_2: %v", err)
		}
	})

	t.Run("concurrent claims: exactly one wins", func(t *testing.T) {
		s := newStore(t)
		seedIngress(t, s, "dec_1")
		const n = 32
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make([]error, n)
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				errs[i] = s.ClaimExecution(ctx, "dec_1", owner(fmt.Sprintf("run-%d", i)), at)
			}()
		}
		close(start)
		wg.Wait()
		RequireOneWinner(t, errs)
	})
}

func mustAdvance(t *testing.T, s Stores, o contracts.ClaimOwner, states ...contracts.ClaimState) {
	t.Helper()
	ctx := context.Background()
	if err := s.ClaimExecution(ctx, "dec_1", o, at); err != nil {
		t.Fatal(err)
	}
	for _, st := range states {
		if err := s.AdvanceClaim(ctx, "dec_1", o, st, at); err != nil {
			t.Fatalf("-> %s: %v", st, err)
		}
	}
}

func mustTransition(t *testing.T, s Stores, o contracts.ClaimOwner, to contracts.ClaimState) {
	t.Helper()
	if err := s.AdvanceClaim(context.Background(), "dec_1", o, to, at); err != nil {
		t.Fatalf("-> %s: %v", to, err)
	}
}

// RequireOneWinner asserts that exactly one error is nil and every other is
// contracts.ErrClaimHeld.
func RequireOneWinner(t *testing.T, errs []error) {
	t.Helper()
	winners := 0
	for i, err := range errs {
		switch {
		case err == nil:
			winners++
		case !errors.Is(err, contracts.ErrClaimHeld):
			t.Fatalf("claim %d: unexpected error %v", i, err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d concurrent claims succeeded, want exactly 1", winners)
	}
}
