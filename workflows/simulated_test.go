package workflows_test

import (
	"context"
	"sync"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func simRequest(target string) contracts.DispatchRequest {
	return contracts.DispatchRequest{
		IdempotencyKey: "sentrygate.exec.v1:dec_" + target,
		Proposal:       contracts.AgentProposal{ID: "p-" + target, Type: contracts.CmdModifyRouting, TargetID: target, Payload: "{}"},
	}
}

func TestSimulatedAdapterBehaviour(t *testing.T) {
	cases := []struct {
		target    string
		dispatch  contracts.OutcomeStatus
		partial   bool
		reconcile contracts.OutcomeStatus
		mutations int
	}{
		{"edge-1", contracts.OutcomeSuccess, false, contracts.OutcomeSuccess, 1},
		{contracts.FailingNodeID, contracts.OutcomeFailure, true, contracts.OutcomeFailure, 1},
		{contracts.LostResponseNodeID, contracts.OutcomeUnknown, false, contracts.OutcomeSuccess, 1},
		{contracts.UnreachableNodeID, contracts.OutcomeUnknown, false, contracts.OutcomeFailure, 0},
		{contracts.PartitionedNodeID, contracts.OutcomeUnknown, false, contracts.OutcomeUnknown, 0},
	}
	ctx := context.Background()
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			sim := workflows.NewSimulatedAdapter()
			req := simRequest(tc.target)

			if o := sim.Dispatch(ctx, req); o.Status != tc.dispatch || o.PartiallyApplied != tc.partial {
				t.Fatalf("dispatch = %+v, want %s partial=%t", o, tc.dispatch, tc.partial)
			}
			for range 3 {
				if o := sim.Reconcile(ctx, req); o.Status != tc.reconcile {
					t.Fatalf("reconcile = %+v, want %s", o, tc.reconcile)
				}
			}
			if n := sim.Mutations(req.IdempotencyKey); n != tc.mutations {
				t.Fatalf("mutations = %d, want %d", n, tc.mutations)
			}
		})
	}
}

func TestSimulatedAdapterAppliesEachKeyOnce(t *testing.T) {
	ctx := context.Background()
	sim := workflows.NewSimulatedAdapter()
	req := simRequest(contracts.LostResponseNodeID)

	if o := sim.Dispatch(ctx, req); o.Status != contracts.OutcomeUnknown {
		t.Fatalf("first dispatch = %+v, want UNKNOWN", o)
	}
	if o := sim.Dispatch(ctx, req); o.Status != contracts.OutcomeSuccess {
		t.Fatalf("duplicate dispatch = %+v, want the recorded SUCCESS", o)
	}
	other := simRequest(contracts.LostResponseNodeID)
	other.IdempotencyKey += "-other"
	sim.Dispatch(ctx, other)

	if sim.Mutations(req.IdempotencyKey) != 1 || sim.Mutations(other.IdempotencyKey) != 1 {
		t.Fatalf("mutations = %d and %d, want 1 each", sim.Mutations(req.IdempotencyKey), sim.Mutations(other.IdempotencyKey))
	}
}

// A FAILURE from reconciliation must stay true: the key is fenced, so a late
// or duplicate dispatch with it cannot take effect afterwards.
func TestSimulatedReconcileFencesUnseenKeys(t *testing.T) {
	ctx := context.Background()
	for _, target := range []string{"edge-1", contracts.UnreachableNodeID} {
		t.Run(target, func(t *testing.T) {
			sim := workflows.NewSimulatedAdapter()
			req := simRequest(target)
			if o := sim.Reconcile(ctx, req); o.Status != contracts.OutcomeFailure {
				t.Fatalf("reconcile of an unseen key = %+v, want FAILURE", o)
			}
			if o := sim.Dispatch(ctx, req); o.Status != contracts.OutcomeFailure {
				t.Fatalf("dispatch after fencing = %+v, want FAILURE", o)
			}
			if n := sim.Mutations(req.IdempotencyKey); n != 0 {
				t.Fatalf("fenced key took effect %d times", n)
			}
		})
	}
}

func TestSimulatedDispatchWithEndedContextIsNotSent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sim := workflows.NewSimulatedAdapter()
	req := simRequest("edge-1")

	if o := sim.Dispatch(ctx, req); o.Status != contracts.OutcomeFailure {
		t.Fatalf("dispatch = %+v, want FAILURE", o)
	}
	if o := sim.Dispatch(context.Background(), req); o.Status != contracts.OutcomeFailure {
		t.Fatalf("later dispatch of the same key = %+v, want FAILURE", o)
	}
	if n := sim.Mutations(req.IdempotencyKey); n != 0 {
		t.Fatalf("mutations = %d, want 0", n)
	}
}

// Concurrent dispatches and reconciliations of one key agree on a single
// effect, and that effect is applied at most once.
func TestSimulatedAdapterConcurrentSameKey(t *testing.T) {
	ctx := context.Background()
	sim := workflows.NewSimulatedAdapter()
	req := simRequest("edge-1")
	const n = 16
	results := make([]contracts.DispatchOutcome, 2*n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(2)
		go func() { defer wg.Done(); <-start; results[2*i] = sim.Dispatch(ctx, req) }()
		go func() { defer wg.Done(); <-start; results[2*i+1] = sim.Reconcile(ctx, req) }()
	}
	close(start)
	wg.Wait()

	for _, o := range results[1:] {
		if o.Status != results[0].Status {
			t.Fatalf("callers saw different effects: %+v vs %+v", o, results[0])
		}
	}
	want := map[contracts.OutcomeStatus]int{contracts.OutcomeSuccess: 1, contracts.OutcomeFailure: 0}[results[0].Status]
	if got := sim.Mutations(req.IdempotencyKey); got != want {
		t.Fatalf("mutations = %d with effect %s, want %d", got, results[0].Status, want)
	}
}

// garbledAdapter reports a status SentryGate does not recognise.
type garbledAdapter struct{ workflows.SimulatedAdapter }

func (*garbledAdapter) Dispatch(context.Context, contracts.DispatchRequest) contracts.DispatchOutcome {
	return contracts.DispatchOutcome{Status: "OK"}
}

func (*garbledAdapter) Reconcile(context.Context, contracts.DispatchRequest) contracts.DispatchOutcome {
	return contracts.DispatchOutcome{}
}

func TestInfrastructureActivitiesGuardTheAdapter(t *testing.T) {
	ctx := context.Background()
	exec := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	const token = "exe_guard"
	fenced := fencedRequest(exec, token)
	req := fenced.Dispatch
	claimed := func(t *testing.T) *decision.MemoryStore {
		s := decision.NewMemoryStore()
		seedInto(t, s, ingressAllow(exec))
		holdClaim(t, s, exec.IngressDecisionID, contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: token})
		return s
	}

	s := claimed(t)
	none := &workflows.InfrastructureActivities{Claims: s}
	if _, err := runDispatch(none, fenced); !isFenceError(err, contracts.ExecutionFenceRejectedErrorType) {
		t.Fatalf("dispatch without adapter = %v; want %s", err, contracts.ExecutionFenceRejectedErrorType)
	}
	requireClaim(t, s, exec.IngressDecisionID, contracts.ClaimStateClaimed)
	if o, err := none.ReconcileDispatch(ctx, req); err != nil || o.Status != contracts.OutcomeUnknown {
		t.Fatalf("reconcile without adapter = %+v, %v; want UNKNOWN", o, err)
	}
	if err := none.RevertStateCompensation(ctx, req); err == nil {
		t.Fatal("compensation without adapter succeeded")
	}

	sim := workflows.NewSimulatedAdapter()
	if _, err := runDispatch(&workflows.InfrastructureActivities{Adapter: sim}, fenced); !isFenceError(err, contracts.ExecutionFenceRejectedErrorType) {
		t.Fatalf("dispatch without claim store = %v; want %s", err, contracts.ExecutionFenceRejectedErrorType)
	}
	acts := &workflows.InfrastructureActivities{Adapter: sim, Claims: s}
	keyless := fenced
	keyless.Dispatch.IdempotencyKey = ""
	if _, err := runDispatch(acts, keyless); !isFenceError(err, contracts.ExecutionFenceRejectedErrorType) {
		t.Fatalf("dispatch without key = %v; want %s", err, contracts.ExecutionFenceRejectedErrorType)
	}
	if o, _ := acts.ReconcileDispatch(ctx, keyless.Dispatch); o.Status != contracts.OutcomeUnknown {
		t.Fatalf("reconcile without key = %+v, want UNKNOWN", o)
	}
	if n := sim.Mutations("") + sim.Mutations(req.IdempotencyKey); n != 0 {
		t.Fatalf("a rejected request reached the adapter")
	}
	requireClaim(t, s, exec.IngressDecisionID, contracts.ClaimStateClaimed)

	garbled := &workflows.InfrastructureActivities{Adapter: &garbledAdapter{}, Claims: claimed(t)}
	if o, err := runDispatch(garbled, fenced); err != nil || o.Status != contracts.OutcomeUnknown {
		t.Fatalf("unrecognised dispatch status = %+v, %v; want UNKNOWN", o, err)
	}
	if o, _ := garbled.ReconcileDispatch(ctx, req); o.Status != contracts.OutcomeUnknown {
		t.Fatalf("empty reconcile status = %+v, want UNKNOWN", o)
	}
}
