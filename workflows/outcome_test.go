package workflows_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

// dispatchRecorder is an adapter whose Dispatch reports a fixed outcome and
// records the idempotency key of every call. In newOutcomeExecution, the
// DispatchConfig activity then reports err in place of the outcome, if set.
type dispatchRecorder struct {
	outcome contracts.DispatchOutcome
	err     error

	mu   sync.Mutex
	keys []string
}

func (d *dispatchRecorder) Dispatch(_ context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.keys = append(d.keys, req.IdempotencyKey)
	return d.outcome
}

func (d *dispatchRecorder) Reconcile(context.Context, contracts.DispatchRequest) contracts.DispatchOutcome {
	return contracts.UnknownOutcome("dispatchRecorder does not reconcile")
}

func (d *dispatchRecorder) Compensate(context.Context, contracts.DispatchRequest) error { return nil }

func (d *dispatchRecorder) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.keys)
}

// scriptedReconcile answers ReconcileDispatch from a script whose last outcome
// repeats, and records the idempotency key of every call. Calls listed in
// lost (1-based) fail as an activity error, as when the worker loses the
// response.
type scriptedReconcile struct {
	outcomes []contracts.DispatchOutcome
	lost     map[int]bool

	mu       sync.Mutex
	keys     []string
	answered int
}

func reconcileWith(outcomes ...contracts.DispatchOutcome) *scriptedReconcile {
	return &scriptedReconcile{outcomes: outcomes, lost: map[int]bool{}}
}

func (s *scriptedReconcile) answer(_ context.Context, req contracts.DispatchRequest) (contracts.DispatchOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = append(s.keys, req.IdempotencyKey)
	if s.lost[len(s.keys)] {
		return contracts.DispatchOutcome{}, errResponseLost
	}
	o := s.outcomes[min(s.answered, len(s.outcomes)-1)]
	s.answered++
	return o, nil
}

func (s *scriptedReconcile) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}

// newOutcomeExecution prepares one execution against shared storage whose
// dispatch, after the real execution fence, and reconciliation are answered by
// d and r.
func newOutcomeExecution(t *testing.T, s decision.Store, claims decision.ClaimStore, d *dispatchRecorder, r *scriptedReconcile) *harness {
	t.Helper()
	own := decision.NewMemoryStore()
	h := newHarnessWithStores(t, executionStore{Store: s, own: own}, claims)
	h.decisions = own
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(fencedDispatch(claims, d, d.err))
	h.env.OnActivity(infra.ReconcileDispatch, mock.Anything, mock.Anything).Return(r.answer)
	h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil)
	return h
}

// newAdapterExecution prepares one execution against shared storage that runs
// the real infrastructure activities over adapter.
func newAdapterExecution(t *testing.T, s decision.Store, claims decision.ClaimStore, adapter workflows.Adapter) *harness {
	t.Helper()
	own := decision.NewMemoryStore()
	h := newHarnessWithStores(t, executionStore{Store: s, own: own}, claims)
	h.decisions = own
	h.env.RegisterActivity(&workflows.InfrastructureActivities{Adapter: adapter, Claims: claims})
	return h
}

func phases(h *harness) []string {
	var out []string
	for _, a := range h.audit.All() {
		out = append(out, string(a.Phase)+" "+string(a.Verdict))
	}
	return out
}

func auditRow(t *testing.T, h *harness, phase contracts.AuditPhase) contracts.AuditRecord {
	t.Helper()
	for _, a := range h.audit.All() {
		if a.Phase == phase {
			return a
		}
	}
	t.Fatalf("no %s audit row in %v", phase, phases(h))
	return contracts.AuditRecord{}
}

func requireOutcomeError(t *testing.T, h *harness, wantType string) {
	t.Helper()
	err := h.env.GetWorkflowError()
	if wantType == "" {
		if err != nil {
			t.Fatalf("workflow failed: %v", err)
		}
		return
	}
	requireRejected(t, err, wantType)
}

// A, B, C: each dispatch result maps to one claim state. Only a confirmed
// outcome is final; anything ambiguous is UNKNOWN and needs reconciliation.
func TestDispatchOutcomeDecidesClaimState(t *testing.T) {
	cases := []struct {
		name        string
		outcome     contracts.DispatchOutcome
		err         error
		state       contracts.ClaimState
		errType     string
		verdict     contracts.AuditVerdict
		compensated bool
	}{
		{"confirmed success", succeeded, nil,
			contracts.ClaimStateCompleted, "", contracts.AuditVerdictPass, false},
		{"confirmed failure", failedOutright, nil,
			contracts.ClaimStateFailed, contracts.DispatchFailedErrorType, contracts.AuditVerdictFail, false},
		{"confirmed failure after partial apply", failedPartly, nil,
			contracts.ClaimStateFailed, contracts.DispatchFailedErrorType, contracts.AuditVerdictFail, true},
		{"adapter reports unknown", unknown, nil,
			contracts.ClaimStateReconciliationRequired, contracts.DispatchOutcomeUnknownErrorType, contracts.AuditVerdictUnknown, false},
		{"transport error", contracts.DispatchOutcome{}, errors.New("read tcp 10.0.0.1:443: connection reset by peer"),
			contracts.ClaimStateReconciliationRequired, contracts.DispatchOutcomeUnknownErrorType, contracts.AuditVerdictUnknown, false},
		{"unrecognised status", contracts.DispatchOutcome{Status: "DONE"}, nil,
			contracts.ClaimStateReconciliationRequired, contracts.DispatchOutcomeUnknownErrorType, contracts.AuditVerdictUnknown, false},
		{"no status", contracts.DispatchOutcome{}, nil,
			contracts.ClaimStateReconciliationRequired, contracts.DispatchOutcomeUnknownErrorType, contracts.AuditVerdictUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := decision.NewMemoryStore()
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			d := &dispatchRecorder{outcome: tc.outcome, err: tc.err}
			r := reconcileWith(unknown)

			h := newOutcomeExecution(t, s, s, d, r)
			h.run(workflowIDFor(req), req)

			requireOutcomeError(t, h, tc.errType)
			requireClaim(t, s, req.IngressDecisionID, tc.state)
			if d.calls() != 1 {
				t.Fatalf("dispatched %d times, want 1", d.calls())
			}
			if got := auditRow(t, h, contracts.AuditPhaseDispatch).Verdict; got != tc.verdict {
				t.Fatalf("dispatch audit verdict = %s, want %s", got, tc.verdict)
			}
			if got := h.count("RevertStateCompensation"); got != map[bool]int{false: 0, true: 1}[tc.compensated] {
				t.Fatalf("compensation ran %d times (compensated=%t): %v", got, tc.compensated, h.order())
			}
			if reconciled := r.calls() > 0; reconciled != (tc.state == contracts.ClaimStateReconciliationRequired) {
				t.Fatalf("reconciliation calls = %d for final state %s", r.calls(), tc.state)
			}
		})
	}
}

// C: a dispatch that times out is UNKNOWN, not FAILED, and is not retried.
// The test environment does not enforce activity timeouts on mocks, so the
// mock returns the error Temporal delivers when StartToClose expires.
func TestDispatchTimeoutIsUnknownNotFailure(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	d := &dispatchRecorder{err: temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)}

	h := newOutcomeExecution(t, s, s, d, reconcileWith(unknown))
	h.run(workflowIDFor(req), req)

	requireOutcomeError(t, h, contracts.DispatchOutcomeUnknownErrorType)
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReconciliationRequired)
	row := auditRow(t, h, contracts.AuditPhaseDispatch)
	if row.Verdict != contracts.AuditVerdictUnknown || !strings.Contains(strings.ToLower(row.Detail), "timeout") {
		t.Fatalf("dispatch audit row = %s %q, want UNKNOWN caused by a timeout", row.Verdict, row.Detail)
	}
	if n := h.count("DispatchConfig"); n != 1 {
		t.Fatalf("dispatch was attempted %d times, want 1", n)
	}
	if h.ran("RevertStateCompensation") {
		t.Fatal("compensation ran for an unknown outcome")
	}
}

// D, E: an unknown outcome keeps the claim and is never dispatched again,
// neither by the owning run nor by a later execution of the same decision.
func TestUnknownOutcomeHoldsClaimAndNeverRedispatches(t *testing.T) {
	for _, kind := range storeKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			s := kind.open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			d := &dispatchRecorder{outcome: unknown}
			r := reconcileWith(unknown)

			first := newOutcomeExecution(t, s, s, d, r)
			first.run(workflowIDFor(req), req)

			requireOutcomeError(t, first, contracts.DispatchOutcomeUnknownErrorType)
			held := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReconciliationRequired)
			if r.calls() < 2 {
				t.Fatalf("reconciliation ran %d times; it should keep asking while the outcome is unknown", r.calls())
			}
			if d.calls() != 1 || first.ran("RevertStateCompensation") {
				t.Fatalf("dispatches = %d, activities = %v", d.calls(), first.order())
			}
			if err := s.AdvanceClaim(ctx, req.IngressDecisionID, held.Owner, contracts.ClaimStateReleased, time.Now()); !errors.Is(err, contracts.ErrClaimNotHeld) {
				t.Fatalf("an unreconciled claim was released: %v", err)
			}

			var dispatches atomic.Int64
			replay := newExecution(t, s, s, &dispatches, succeeded)
			replay.run(workflowIDFor(req), req)
			requireAlreadyClaimed(t, replay)

			if dispatches.Load() != 0 || d.calls() != 1 {
				t.Fatalf("decision with an unknown outcome was dispatched again: %d, %d", dispatches.Load(), d.calls())
			}
			if after := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReconciliationRequired); after.Owner != held.Owner {
				t.Fatalf("claim owner changed: %+v -> %+v", held.Owner, after.Owner)
			}
		})
	}
}

// F, G: reconciliation turns an unknown outcome into a confirmed one, and only
// a confirmed partial application is compensated.
func TestReconciliationResolvesUnknownOutcome(t *testing.T) {
	cases := []struct {
		name    string
		script  []contracts.DispatchOutcome
		state   contracts.ClaimState
		errType string
		phases  []string
	}{
		{"confirms success", []contracts.DispatchOutcome{unknown, unknown, succeeded},
			contracts.ClaimStateCompleted, "",
			[]string{"WORKFLOW_REVALIDATION PASS", "DISPATCH_CONFIG UNKNOWN", "DISPATCH_RECONCILIATION PASS", "WORKFLOW_COMPLETE PASS"}},
		{"confirms failure", []contracts.DispatchOutcome{unknown, failedOutright},
			contracts.ClaimStateFailed, contracts.DispatchFailedErrorType,
			[]string{"WORKFLOW_REVALIDATION PASS", "DISPATCH_CONFIG UNKNOWN", "DISPATCH_RECONCILIATION FAIL", "WORKFLOW_FAILED FAIL"}},
		{"confirms partial failure", []contracts.DispatchOutcome{failedPartly},
			contracts.ClaimStateFailed, contracts.DispatchFailedErrorType,
			[]string{"WORKFLOW_REVALIDATION PASS", "DISPATCH_CONFIG UNKNOWN", "DISPATCH_RECONCILIATION FAIL", "COMPENSATION_ROLLBACK PASS", "WORKFLOW_FAILED FAIL"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := decision.NewMemoryStore()
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			d := &dispatchRecorder{outcome: unknown}
			r := reconcileWith(tc.script...)

			h := newOutcomeExecution(t, s, s, d, r)
			h.run(workflowIDFor(req), req)

			requireOutcomeError(t, h, tc.errType)
			requireClaim(t, s, req.IngressDecisionID, tc.state)
			if got := phases(h); !slices.Equal(got, tc.phases) {
				t.Fatalf("audit = %v, want %v", got, tc.phases)
			}
			if r.calls() != len(tc.script) || d.calls() != 1 {
				t.Fatalf("reconciliations = %d (want %d), dispatches = %d (want 1)", r.calls(), len(tc.script), d.calls())
			}

			var dispatches atomic.Int64
			replay := newExecution(t, s, s, &dispatches, succeeded)
			replay.run(workflowIDFor(req), req)
			requireAlreadyClaimed(t, replay)
			if dispatches.Load() != 0 {
				t.Fatal("reconciled decision was dispatched again")
			}
		})
	}
}

// H: if reconciliation never gets an answer, the workflow keeps asking for the
// reconciliation window and then stops with the claim still unresolved.
func TestUnresolvedOutcomeStaysReconciliationRequired(t *testing.T) {
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	d := &dispatchRecorder{outcome: unknown}
	r := reconcileWith(unknown)

	h := newOutcomeExecution(t, s, s, d, r)
	start := h.env.Now()
	h.run(workflowIDFor(req), req)
	elapsed := h.env.Now().Sub(start)

	key := requireRejected(t, h.env.GetWorkflowError(), contracts.DispatchOutcomeUnknownErrorType)
	if string(key) != decision.ExecutionKey(req.IngressDecisionID) {
		t.Fatalf("error details = %q, want the idempotency key", key)
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReconciliationRequired)
	if elapsed > 24*time.Hour || elapsed < 23*time.Hour {
		t.Fatalf("reconciliation stopped after %s, want just under the 24h window", elapsed)
	}
	if r.calls() < 10 {
		t.Fatalf("reconciliation asked only %d times in %s", r.calls(), elapsed)
	}
	want := []string{"WORKFLOW_REVALIDATION PASS", "DISPATCH_CONFIG UNKNOWN", "DISPATCH_RECONCILIATION UNKNOWN"}
	if got := phases(h); !slices.Equal(got, want) {
		t.Fatalf("audit = %v, want %v", got, want)
	}
	if d.calls() != 1 || h.ran("RevertStateCompensation") {
		t.Fatalf("dispatches = %d, activities = %v", d.calls(), h.order())
	}
}

// I: the idempotency key is derived from the ingress decision only. It is the
// same for dispatch, every reconciliation attempt and every activity retry,
// and for any run that executes the decision.
func TestIdempotencyKeyIsStableAcrossAttemptsAndRuns(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	key := decision.ExecutionKey(req.IngressDecisionID)

	// An earlier run was denied at re-validation and released the claim
	// without dispatching.
	var none atomic.Int64
	denied := newExecution(t, s, s, &none, succeeded)
	denied.src.p.Store(parsePolicy(t, tightenedPolicy))
	denied.run(workflowIDFor(req), req)
	requireRevalidationDenied(t, denied.env.GetWorkflowError())
	released := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReleased)

	// The next run dispatches with an unknown outcome; the response to its
	// first reconciliation is lost and the activity is retried.
	d := &dispatchRecorder{outcome: unknown}
	r := reconcileWith(unknown, unknown, succeeded)
	r.lost[1] = true
	h := newOutcomeExecution(t, s, s, d, r)
	h.run(workflowIDFor(req), req)

	requireOutcomeError(t, h, "")
	done := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)
	if done.Owner.Token == released.Owner.Token {
		t.Fatal("precondition: the two runs should hold different claim tokens")
	}
	if len(r.keys) != 4 {
		t.Fatalf("reconciliation calls = %d, want 4 (one lost and retried)", len(r.keys))
	}
	for i, k := range append(slices.Clone(d.keys), r.keys...) {
		if k != key {
			t.Fatalf("call %d used key %q, want %q", i, k, key)
		}
	}
	if strings.Contains(key, done.Owner.Token) || strings.Contains(key, done.Owner.RunID) {
		t.Fatalf("key %q depends on the run", key)
	}
}

// stubbornAdapter asks the wrapped adapter on every reconciliation but reports
// UNKNOWN for the first unknownFor of them, as if status queries timed out.
type stubbornAdapter struct {
	*workflows.SimulatedAdapter
	unknownFor int

	mu    sync.Mutex
	calls int
}

func (a *stubbornAdapter) Reconcile(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	o := a.SimulatedAdapter.Reconcile(ctx, req)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls <= a.unknownFor {
		return contracts.UnknownOutcome("simulated: status query timed out")
	}
	return o
}

// J: repeated and duplicate reconciliation, later executions and a duplicate
// dispatch with the same key never apply the mutation a second time.
func TestDuplicateReconciliationCannotMutateTwice(t *testing.T) {
	ctx := context.Background()
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, contracts.LostResponseNodeID)
	seedInto(t, s, ingressAllow(req))
	key := decision.ExecutionKey(req.IngressDecisionID)
	sim := workflows.NewSimulatedAdapter()
	adapter := &stubbornAdapter{SimulatedAdapter: sim, unknownFor: 3}

	h := newAdapterExecution(t, s, s, adapter)
	h.run(workflowIDFor(req), req)

	requireOutcomeError(t, h, "")
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)
	if n := h.count("ReconcileDispatch"); n != 4 {
		t.Fatalf("reconciliations = %d, want 4", n)
	}
	if n := h.count("DispatchConfig"); n != 1 {
		t.Fatalf("dispatches = %d, want 1", n)
	}

	for range 2 {
		replay := newAdapterExecution(t, s, s, sim)
		replay.run(workflowIDFor(req), req)
		requireAlreadyClaimed(t, replay)
	}

	dreq := contracts.DispatchRequest{IdempotencyKey: key, Proposal: req.Proposal}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() { defer wg.Done(); sim.Reconcile(ctx, dreq) }()
		go func() { defer wg.Done(); sim.Dispatch(ctx, dreq) }()
	}
	wg.Wait()

	if n := sim.Mutations(key); n != 1 {
		t.Fatalf("mutation applied %d times, want 1", n)
	}
	if o := sim.Reconcile(ctx, dreq); o.Status != contracts.OutcomeSuccess {
		t.Fatalf("reconciliation after duplicates = %+v, want SUCCESS", o)
	}
}

// The real infrastructure activities over the simulated adapter, without
// mocks, for each simulated target behaviour.
func TestSimulatedAdapterOutcomesEndToEnd(t *testing.T) {
	cases := []struct {
		target        string
		state         contracts.ClaimState
		errType       string
		mutations     int
		compensations int
		reconciled    bool
	}{
		{"edge-1", contracts.ClaimStateCompleted, "", 1, 0, false},
		{contracts.FailingNodeID, contracts.ClaimStateFailed, contracts.DispatchFailedErrorType, 1, 1, false},
		{contracts.LostResponseNodeID, contracts.ClaimStateCompleted, "", 1, 0, true},
		{contracts.UnreachableNodeID, contracts.ClaimStateFailed, contracts.DispatchFailedErrorType, 0, 0, true},
		{contracts.PartitionedNodeID, contracts.ClaimStateReconciliationRequired, contracts.DispatchOutcomeUnknownErrorType, 0, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.target, func(t *testing.T) {
			s := decision.NewMemoryStore()
			req := request("agent-a", contracts.CmdModifyRouting, tc.target)
			seedInto(t, s, ingressAllow(req))
			key := decision.ExecutionKey(req.IngressDecisionID)
			sim := workflows.NewSimulatedAdapter()

			h := newAdapterExecution(t, s, s, sim)
			h.run(workflowIDFor(req), req)

			requireOutcomeError(t, h, tc.errType)
			requireClaim(t, s, req.IngressDecisionID, tc.state)
			if sim.Mutations(key) != tc.mutations || sim.Compensations(key) != tc.compensations {
				t.Fatalf("mutations = %d (want %d), compensations = %d (want %d)",
					sim.Mutations(key), tc.mutations, sim.Compensations(key), tc.compensations)
			}
			if h.count("DispatchConfig") != 1 || h.ran("ReconcileDispatch") != tc.reconciled {
				t.Fatalf("activities = %v", h.order())
			}
		})
	}
}

// If RECONCILIATION_REQUIRED cannot be written, the claim stays EXECUTING,
// which is equally unclaimable and unreleasable, and reconciliation still
// resolves it.
func TestReconciliationRequiredWriteFailureStillResolves(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == contracts.ClaimStateReconciliationRequired {
			return errStoreDown, false
		}
		return nil, false
	})
	d := &dispatchRecorder{outcome: unknown}

	h := newOutcomeExecution(t, s, claims, d, reconcileWith(unknown, succeeded))
	h.run(workflowIDFor(req), req)

	requireOutcomeError(t, h, "")
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)
	if d.calls() != 1 {
		t.Fatalf("dispatches = %d, want 1", d.calls())
	}
}

// A workflow cancelled while dispatch is in flight does not learn the outcome:
// it records UNKNOWN, keeps the claim and does not compensate.
func TestCancellationDuringDispatchIsUnknown(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	var calls atomic.Int64

	own := decision.NewMemoryStore()
	h := newHarnessWithStores(t, executionStore{Store: s, own: own}, s)
	h.decisions = own
	var infra *workflows.InfrastructureActivities
	inFlight := fencedDispatch(s, countingAdapter{n: &calls, outcome: succeeded}, nil)
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(
		func(ctx context.Context, fd contracts.FencedDispatch) (contracts.DispatchOutcome, error) {
			// The fence is acquired and the adapter called, and the workflow
			// is cancelled before the response arrives.
			o, err := inFlight(ctx, fd)
			h.env.CancelWorkflow()
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			return o, err
		})
	h.env.OnActivity(infra.ReconcileDispatch, mock.Anything, mock.Anything).Return(reconcileWith(unknown).answer)
	h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil)
	h.run(workflowIDFor(req), req)

	requireOutcomeError(t, h, contracts.DispatchOutcomeUnknownErrorType)
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReconciliationRequired)
	if calls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want 1", calls.Load())
	}
	want := []string{"WORKFLOW_REVALIDATION PASS", "DISPATCH_CONFIG UNKNOWN", "DISPATCH_RECONCILIATION UNKNOWN"}
	if got := phases(h); !slices.Equal(got, want) {
		t.Fatalf("audit = %v, want %v", got, want)
	}
	if h.count("DispatchConfig") != 1 || h.ran("RevertStateCompensation") {
		t.Fatalf("activities = %v", h.order())
	}
}
