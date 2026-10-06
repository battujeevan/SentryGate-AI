package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

// Temporal's activity test environment runs every activity as this workflow
// execution, and the workflow test environment uses the same run ID.
const (
	testWorkflowID = "default-test-workflow-id"
	testRunID      = "default-test-run-id"
)

// countingAdapter reports outcome for every dispatch and counts its
// invocations in n. If observe is set, it runs first on every dispatch.
type countingAdapter struct {
	n       *atomic.Int64
	outcome contracts.DispatchOutcome
	observe func(contracts.DispatchRequest)
}

func (a countingAdapter) Dispatch(_ context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	if a.observe != nil {
		a.observe(req)
	}
	a.n.Add(1)
	return a.outcome
}

func (countingAdapter) Reconcile(context.Context, contracts.DispatchRequest) contracts.DispatchOutcome {
	return contracts.UnknownOutcome("test adapter does not reconcile")
}

func (countingAdapter) Compensate(context.Context, contracts.DispatchRequest) error { return nil }

// countedAdapter counts the Dispatch calls that reach the wrapped adapter.
type countedAdapter struct {
	workflows.Adapter
	n *atomic.Int64
}

func (a countedAdapter) Dispatch(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	a.n.Add(1)
	return a.Adapter.Dispatch(ctx, req)
}

// fencedDispatch stands in for DispatchConfig in workflow tests. It runs the
// real activity, execution fence included, over adapter. If err is set and the
// adapter was called, it then returns err instead of the outcome, as when the
// response is lost after dispatch.
func fencedDispatch(claims decision.ClaimStore, adapter workflows.Adapter, err error) func(context.Context, contracts.FencedDispatch) (contracts.DispatchOutcome, error) {
	acts := &workflows.InfrastructureActivities{Adapter: adapter, Claims: claims}
	return func(ctx context.Context, req contracts.FencedDispatch) (contracts.DispatchOutcome, error) {
		o, ferr := acts.DispatchConfig(ctx, req)
		if ferr == nil && err != nil {
			return contracts.DispatchOutcome{}, err
		}
		return o, ferr
	}
}

// runDispatch runs DispatchConfig in Temporal's activity test environment, as
// the run testWorkflowID / testRunID.
func runDispatch(acts *workflows.InfrastructureActivities, req contracts.FencedDispatch) (contracts.DispatchOutcome, error) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(acts)
	val, err := env.ExecuteActivity(acts.DispatchConfig, req)
	if err != nil {
		return contracts.DispatchOutcome{}, err
	}
	var o contracts.DispatchOutcome
	if err := val.Get(&o); err != nil {
		return contracts.DispatchOutcome{}, fmt.Errorf("decode outcome: %w", err)
	}
	return o, nil
}

func fencedRequest(req contracts.ExecutionRequest, token string) contracts.FencedDispatch {
	return contracts.FencedDispatch{
		DecisionID: req.IngressDecisionID,
		Token:      token,
		Dispatch:   contracts.DispatchRequest{IdempotencyKey: decision.ExecutionKey(req.IngressDecisionID), Proposal: req.Proposal},
	}
}

func isFenceError(err error, want string) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == want && appErr.NonRetryable()
}

func requireFenceError(t *testing.T, err error, want string) {
	t.Helper()
	if !isFenceError(err, want) {
		t.Fatalf("expected non-retryable %s, got %v", want, err)
	}
}

// asRun presents every claim owner with runID. Temporal's test environments
// give every run the same run ID; this stands in for the distinct run ID that
// the original run of a workflow that is later reset would have had.
type asRun struct {
	decision.ClaimStore
	runID string
}

func (r asRun) ClaimExecution(ctx context.Context, id string, o contracts.ClaimOwner, at time.Time) error {
	o.RunID = r.runID
	return r.ClaimStore.ClaimExecution(ctx, id, o, at)
}

func (r asRun) AdvanceClaim(ctx context.Context, id string, o contracts.ClaimOwner, to contracts.ClaimState, at time.Time) error {
	o.RunID = r.runID
	return r.ClaimStore.AdvanceClaim(ctx, id, o, to, at)
}

// unreadableClaims fails every claim read.
type unreadableClaims struct{ decision.ClaimStore }

func (unreadableClaims) GetClaim(context.Context, string) (contracts.ExecutionClaim, error) {
	return contracts.ExecutionClaim{}, errStoreDown
}

func holdClaim(t *testing.T, s decision.ClaimStore, id string, o contracts.ClaimOwner, path ...contracts.ClaimState) {
	t.Helper()
	ctx := context.Background()
	if err := s.ClaimExecution(ctx, id, o, ingressTime); err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, st := range path {
		if err := s.AdvanceClaim(ctx, id, o, st, ingressTime); err != nil {
			t.Fatalf("-> %s: %v", st, err)
		}
	}
}

// DispatchConfig calls the adapter only after it has moved the claim from
// CLAIMED to EXECUTING for the run named by Temporal's activity info. In every
// other case the adapter is not called and the claim is not changed.
func TestDispatchConfigFence(t *testing.T) {
	const token = "exe_fence"
	me := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: token}
	resetRun := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: "run-before-reset", Token: token}
	otherWorkflow := contracts.ClaimOwner{WorkflowID: "saga-other", RunID: testRunID, Token: token}
	otherToken := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: "exe_other"}
	executing := []contracts.ClaimState{contracts.ClaimStateExecuting}
	reconciling := []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired}
	completed := []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateCompleted}
	failed := []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateFailed}
	released := []contracts.ClaimState{contracts.ClaimStateReleased}
	const (
		rejected = contracts.ExecutionFenceRejectedErrorType
		unknown  = contracts.ExecutionFenceUnknownErrorType
	)

	cases := []struct {
		name   string
		holder *contracts.ClaimOwner // nil: never claimed
		path   []contracts.ClaimState
		mutate func(*contracts.FencedDispatch)
		want   string // "": the fence is acquired and the adapter called
	}{
		{name: "valid owner", holder: &me},
		{name: "claimed by the run before a reset", holder: &resetRun, want: rejected},
		{name: "claimed under another workflow ID", holder: &otherWorkflow, want: rejected},
		{name: "claimed with another token", holder: &otherToken, want: rejected},
		{name: "never claimed", want: rejected},
		{name: "EXECUTING by another run", holder: &resetRun, path: executing, want: rejected},
		{name: "COMPLETED by another run", holder: &resetRun, path: completed, want: rejected},
		{name: "EXECUTING by this run", holder: &me, path: executing, want: unknown},
		{name: "RECONCILIATION_REQUIRED", holder: &me, path: reconciling, want: unknown},
		{name: "COMPLETED", holder: &me, path: completed, want: unknown},
		{name: "FAILED", holder: &me, path: failed, want: unknown},
		{name: "RELEASED", holder: &me, path: released, want: rejected},
		{name: "idempotency key of another decision", holder: &me, want: rejected,
			mutate: func(r *contracts.FencedDispatch) { r.Dispatch.IdempotencyKey = decision.ExecutionKey("dec_other") }},
		{name: "fence for another decision", holder: &me, want: rejected,
			mutate: func(r *contracts.FencedDispatch) {
				r.DecisionID = "dec_other"
				r.Dispatch.IdempotencyKey = decision.ExecutionKey("dec_other")
			}},
		{name: "missing token", holder: &me, want: rejected,
			mutate: func(r *contracts.FencedDispatch) { r.Token = "" }},
	}

	for _, kind := range storeKinds {
		for _, tc := range cases {
			t.Run(kind.name+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()
				s := kind.open(t)
				req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
				seedInto(t, s, ingressAllow(req))
				id := req.IngressDecisionID
				if tc.holder != nil {
					holdClaim(t, s, id, *tc.holder, tc.path...)
				}
				before, beforeErr := s.GetClaim(ctx, id)

				var calls atomic.Int64
				var seen []contracts.ExecutionClaim
				adapter := countingAdapter{n: &calls, outcome: succeeded, observe: func(contracts.DispatchRequest) {
					c, _ := s.GetClaim(ctx, id)
					seen = append(seen, c)
				}}
				fd := fencedRequest(req, token)
				if tc.mutate != nil {
					tc.mutate(&fd)
				}
				o, err := runDispatch(&workflows.InfrastructureActivities{Adapter: adapter, Claims: s}, fd)

				if tc.want == "" {
					if err != nil || o.Status != contracts.OutcomeSuccess {
						t.Fatalf("dispatch = %+v, %v; want SUCCESS", o, err)
					}
					if calls.Load() != 1 {
						t.Fatalf("adapter calls = %d, want 1", calls.Load())
					}
					if len(seen) != 1 || seen[0].State != contracts.ClaimStateExecuting || seen[0].Owner != me {
						t.Fatalf("adapter saw claim %+v; it must be called only after the fence committed", seen)
					}
					requireClaim(t, s, id, contracts.ClaimStateExecuting)
					return
				}
				if n := calls.Load(); n != 0 {
					t.Fatalf("adapter calls = %d after a failed fence, want 0", n)
				}
				requireFenceError(t, err, tc.want)
				after, afterErr := s.GetClaim(ctx, id)
				if after != before || !errors.Is(afterErr, beforeErr) {
					t.Fatalf("failed fence changed the claim: %+v (%v) -> %+v (%v)", before, beforeErr, after, afterErr)
				}
			})
		}
	}
}

// A store failure during the fence never falls through to the adapter, whether
// or not the write committed.
func TestDispatchConfigFenceStoreFailures(t *testing.T) {
	const token = "exe_fence"
	me := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: token}
	other := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: "run-before-reset", Token: token}
	failFence := func(commit bool) func(op contracts.ClaimState, _ int) (error, bool) {
		return func(op contracts.ClaimState, _ int) (error, bool) {
			if op == contracts.ClaimStateExecuting {
				return errStoreDown, commit
			}
			return nil, false
		}
	}
	cases := []struct {
		name   string
		holder contracts.ClaimOwner
		claims func(decision.ClaimStore) decision.ClaimStore
		state  contracts.ClaimState
	}{
		{"fence write fails", me,
			func(s decision.ClaimStore) decision.ClaimStore { return newFaultyClaims(s, failFence(false)) },
			contracts.ClaimStateClaimed},
		{"fence write commits but its response is lost", me,
			func(s decision.ClaimStore) decision.ClaimStore { return newFaultyClaims(s, failFence(true)) },
			contracts.ClaimStateExecuting},
		{"fence rejected and the claim cannot be read", other,
			func(s decision.ClaimStore) decision.ClaimStore { return unreadableClaims{s} },
			contracts.ClaimStateClaimed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := storeKinds[1].open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			holdClaim(t, s, req.IngressDecisionID, tc.holder)
			var calls atomic.Int64

			_, err := runDispatch(&workflows.InfrastructureActivities{
				Adapter: countingAdapter{n: &calls, outcome: succeeded},
				Claims:  tc.claims(s),
			}, fencedRequest(req, token))

			if n := calls.Load(); n != 0 {
				t.Fatalf("adapter calls = %d, want 0", n)
			}
			requireFenceError(t, err, contracts.ExecutionFenceUnknownErrorType)
			requireClaim(t, s, req.IngressDecisionID, tc.state)
		})
	}
}

// Dispatch attempts for one decision from independent database connections,
// as from separate worker processes, some presenting the owner's token and
// some not: exactly one acquires the fence and calls the adapter.
func TestConcurrentDispatchAttemptsAcquireOneFence(t *testing.T) {
	const token = "exe_fence"
	me := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: token}
	path := filepath.Join(t.TempDir(), "audit.db")
	open := func() *audit.Store {
		s, err := audit.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	s := open()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	holdClaim(t, s, req.IngressDecisionID, me)

	const n = 12
	var calls atomic.Int64
	outcomes := make([]contracts.DispatchOutcome, n)
	errs := make([]error, n)
	conns := make([]*audit.Store, n)
	for i := range conns {
		conns[i] = open()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		tok := token
		if i%3 == 2 {
			tok = fmt.Sprintf("exe_other_%d", i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			outcomes[i], errs[i] = runDispatch(&workflows.InfrastructureActivities{
				Adapter: countingAdapter{n: &calls, outcome: succeeded},
				Claims:  conns[i],
			}, fencedRequest(req, tok))
		}()
	}
	close(start)
	wg.Wait()

	winners := 0
	for i, err := range errs {
		switch {
		case err == nil && outcomes[i].Status == contracts.OutcomeSuccess:
			winners++
		case isFenceError(err, contracts.ExecutionFenceRejectedErrorType), isFenceError(err, contracts.ExecutionFenceUnknownErrorType):
		default:
			t.Fatalf("attempt %d: outcome %+v, error %v", i, outcomes[i], err)
		}
	}
	if winners != 1 {
		t.Fatalf("%d attempts acquired the fence, want exactly 1", winners)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("adapter calls = %d, want 1", got)
	}
	if c := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateExecuting); c.Owner != me {
		t.Fatalf("claim owner = %+v, want %+v", c.Owner, me)
	}
}

// Adversarial: a Temporal reset creates a new run of the workflow with the
// same workflow ID and the history up to the reset point, including the claim
// token, but a new run ID. Before the fence moved into DispatchConfig, a reset
// to a point after the workflow had marked the claim EXECUTING let the new run
// schedule DispatchConfig and call the adapter a second time. Wherever the
// reset point is, the new run must not be able to call the adapter.
func TestTemporalResetCannotDispatchAgain(t *testing.T) {
	cases := []struct {
		name    string
		resetAt string
		stateAt contracts.ClaimState // claim when the reset run reaches DispatchConfig
	}{
		{"reset after the claim, before the fence", "revalidation", contracts.ClaimStateClaimed},
		{"reset while the original dispatch is in flight", "dispatch", contracts.ClaimStateExecuting},
		{"reset after the original run completed", "end", contracts.ClaimStateCompleted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s := storeKinds[1].open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			rec := ingressAllow(req)
			rec.WorkflowID = testWorkflowID
			seedInto(t, s, rec)
			id := req.IngressDecisionID

			var originalCalls, resetCalls atomic.Int64
			var resetErr error
			var atReset contracts.ExecutionClaim
			resetRan := false
			// The reset run reaches DispatchConfig with the original run's
			// token from history, under the same workflow ID and its own run
			// ID (testRunID).
			resetRun := func() {
				held, err := s.GetClaim(ctx, id)
				if err != nil {
					resetErr = err
					return
				}
				atReset, resetRan = held, true
				_, resetErr = runDispatch(&workflows.InfrastructureActivities{
					Adapter: countingAdapter{n: &resetCalls, outcome: succeeded},
					Claims:  s,
				}, fencedRequest(req, held.Owner.Token))
			}

			adapter := countingAdapter{n: &originalCalls, outcome: succeeded}
			if tc.resetAt == "dispatch" {
				adapter.observe = func(contracts.DispatchRequest) { resetRun() }
			}
			original := newAdapterExecution(t, s, asRun{ClaimStore: s, runID: "run-A"}, adapter)
			if tc.resetAt == "revalidation" {
				var decisions *workflows.DecisionActivities
				original.env.OnActivity(decisions.EvaluateExecution, mock.Anything, mock.Anything).Return(
					func(_ context.Context, r contracts.ExecutionRequest) (contracts.Decision, error) {
						resetRun()
						return decision.Revalidate(original.src.Current(), r), nil
					})
			}
			original.run(testWorkflowID, req)
			if err := original.env.GetWorkflowError(); err != nil {
				t.Fatalf("original run failed: %v", err)
			}
			if tc.resetAt == "end" {
				resetRun()
			}

			if !resetRan {
				t.Fatalf("reset run did not reach DispatchConfig: %v", resetErr)
			}
			if atReset.State != tc.stateAt || atReset.Owner.RunID != "run-A" || atReset.Owner.WorkflowID != testWorkflowID {
				t.Fatalf("claim when the reset run dispatched = %+v, want %s owned by run-A", atReset, tc.stateAt)
			}
			if n := resetCalls.Load(); n != 0 {
				t.Fatalf("reset run called the adapter %d times, want 0", n)
			}
			requireFenceError(t, resetErr, contracts.ExecutionFenceRejectedErrorType)
			if n := originalCalls.Load(); n != 1 {
				t.Fatalf("original run called the adapter %d times, want 1", n)
			}
			if c := requireClaim(t, s, id, contracts.ClaimStateCompleted); c.Owner.RunID != "run-A" {
				t.Fatalf("claim owner = %+v, want run-A", c.Owner)
			}
		})
	}
}

// The reset run as a whole workflow. Its history already holds the original
// run's successful claim, so it goes on to re-validation and dispatch without
// claiming. (In the test environment it also gets a new token; the test above
// covers a reset run presenting the original token.) It must stop at the
// fence, record that the adapter was not called, and leave the claim alone.
func TestResetRunWorkflowStopsAtTheFence(t *testing.T) {
	original := contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: "run-A", Token: "exe_original"}
	for _, path := range [][]contracts.ClaimState{
		nil,
		{contracts.ClaimStateExecuting},
		{contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired},
		{contracts.ClaimStateExecuting, contracts.ClaimStateCompleted},
	} {
		state := contracts.ClaimStateClaimed
		if len(path) > 0 {
			state = path[len(path)-1]
		}
		t.Run(string(state), func(t *testing.T) {
			s := storeKinds[1].open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			rec := ingressAllow(req)
			rec.WorkflowID = testWorkflowID
			seedInto(t, s, rec)
			holdClaim(t, s, req.IngressDecisionID, original, path...)
			before := requireClaim(t, s, req.IngressDecisionID, state)

			var calls atomic.Int64
			h := newAdapterExecution(t, s, s, countingAdapter{n: &calls, outcome: succeeded})
			var decisions *workflows.DecisionActivities
			h.env.OnActivity(decisions.ClaimExecution, mock.Anything, mock.Anything).Return(contracts.ClaimResult{Claimed: true}, nil)
			h.run(testWorkflowID, req)

			if n := calls.Load(); n != 0 {
				t.Fatalf("reset run called the adapter %d times, want 0", n)
			}
			requireRejected(t, h.env.GetWorkflowError(), contracts.ExecutionClaimRejectedErrorType)
			if h.count("DispatchConfig") != 1 || h.ran("ReconcileDispatch") || h.ran("RevertStateCompensation") {
				t.Fatalf("activities = %v", h.order())
			}
			want := []string{"WORKFLOW_REVALIDATION PASS", "EXECUTION_FENCE FAIL"}
			if got := phases(h); !slices.Equal(got, want) {
				t.Fatalf("audit = %v, want %v", got, want)
			}
			if after := requireClaim(t, s, req.IngressDecisionID, before.State); after != before {
				t.Fatalf("reset run changed the claim: %+v -> %+v", before, after)
			}
		})
	}
}

// The fence commits but its response is lost. The adapter was not called, but
// the claim is EXECUTING and can no longer be released, so the workflow
// reconciles instead of dispatching; the simulated adapter has never seen the
// key, fences it and confirms FAILURE.
func TestLostFenceResponseIsReconciledNotDispatched(t *testing.T) {
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, contracts.LostResponseNodeID)
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, call int) (error, bool) {
		if op == contracts.ClaimStateExecuting && call == 1 {
			return errResponseLost, true
		}
		return nil, false
	})
	sim := workflows.NewSimulatedAdapter()
	var calls atomic.Int64

	h := newAdapterExecution(t, s, claims, countedAdapter{Adapter: sim, n: &calls})
	h.run(workflowIDFor(req), req)

	requireRejected(t, h.env.GetWorkflowError(), contracts.DispatchFailedErrorType)
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateFailed)
	key := decision.ExecutionKey(req.IngressDecisionID)
	if calls.Load() != 0 || sim.Mutations(key) != 0 {
		t.Fatalf("adapter calls = %d, mutations = %d; want 0 and 0", calls.Load(), sim.Mutations(key))
	}
	want := []string{"WORKFLOW_REVALIDATION PASS", "EXECUTION_FENCE UNKNOWN", "DISPATCH_RECONCILIATION FAIL", "WORKFLOW_FAILED FAIL"}
	if got := phases(h); !slices.Equal(got, want) {
		t.Fatalf("audit = %v, want %v", got, want)
	}
	if h.count("DispatchConfig") != 1 || h.ran("RevertStateCompensation") {
		t.Fatalf("activities = %v", h.order())
	}

	replay := newAdapterExecution(t, s, s, countedAdapter{Adapter: sim, n: &calls})
	replay.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, replay)
	if calls.Load() != 0 {
		t.Fatalf("adapter calls = %d after replay, want 0", calls.Load())
	}
}

// If neither the fence nor the release can be written, the workflow cannot
// tell whether the fence committed. It never calls the adapter; the claim
// stays CLAIMED, so no other execution can claim the decision either.
func TestFenceAndReleaseFailureNeverDispatches(t *testing.T) {
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == contracts.ClaimStateExecuting || op == contracts.ClaimStateReleased {
			return errStoreDown, false
		}
		return nil, false
	})
	sim := workflows.NewSimulatedAdapter()
	var calls atomic.Int64

	h := newAdapterExecution(t, s, claims, countedAdapter{Adapter: sim, n: &calls})
	h.run(workflowIDFor(req), req)

	if h.env.GetWorkflowError() == nil {
		t.Fatal("workflow succeeded without dispatching")
	}
	if calls.Load() != 0 || sim.Mutations(decision.ExecutionKey(req.IngressDecisionID)) != 0 {
		t.Fatalf("adapter calls = %d; want 0", calls.Load())
	}
	held := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateClaimed)
	if got := auditRow(t, h, contracts.AuditPhaseExecutionFence).Verdict; got != contracts.AuditVerdictUnknown {
		t.Fatalf("fence audit verdict = %s, want UNKNOWN", got)
	}
	if slices.ContainsFunc(h.audit.All(), func(a contracts.AuditRecord) bool { return a.Phase == contracts.AuditPhaseDispatch }) {
		t.Fatalf("a dispatch was recorded although the adapter was not called: %v", phases(h))
	}

	next := newAdapterExecution(t, s, s, countedAdapter{Adapter: sim, n: &calls})
	next.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, next)
	if after := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateClaimed); after != held || calls.Load() != 0 {
		t.Fatalf("claim %+v -> %+v, adapter calls = %d", held, after, calls.Load())
	}
}
