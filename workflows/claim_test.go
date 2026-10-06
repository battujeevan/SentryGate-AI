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

// sharedStores is the decision and claim storage that several workflow
// executions use, as worker processes share one database.
type sharedStores interface {
	decision.Store
	decision.ClaimStore
}

var storeKinds = []struct {
	name string
	open func(t *testing.T) sharedStores
}{
	{"memory", func(*testing.T) sharedStores { return decision.NewMemoryStore() }},
	{"sqlite", func(t *testing.T) sharedStores {
		s, err := audit.Open(filepath.Join(t.TempDir(), "audit.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}},
}

func seedInto(t *testing.T, s decision.Store, rec contracts.DecisionRecord) {
	t.Helper()
	if err := s.AppendDecision(context.Background(), rec); err != nil {
		t.Fatalf("seed decision: %v", err)
	}
}

// executionStore reads ingress records from shared storage and keeps this
// execution's workflow-stage records apart. Temporal's test environment gives
// every execution the same run ID, so those records' IDs (wf_<workflow>_<run>)
// would collide in shared storage; real runs have distinct run IDs.
type executionStore struct {
	decision.Store
	own *decision.MemoryStore
}

func (e executionStore) AppendDecision(ctx context.Context, rec contracts.DecisionRecord) error {
	if rec.Stage == contracts.StageWorkflowRevalidation {
		return e.own.AppendDecision(ctx, rec)
	}
	return e.Store.AppendDecision(ctx, rec)
}

// newExecution prepares one workflow execution against shared storage. Its
// DispatchConfig runs the real execution fence on claims; every adapter call
// increments dispatches and reports outcome.
func newExecution(t *testing.T, s decision.Store, claims decision.ClaimStore, dispatches *atomic.Int64, outcome contracts.DispatchOutcome) *harness {
	t.Helper()
	own := decision.NewMemoryStore()
	h := newHarnessWithStores(t, executionStore{Store: s, own: own}, claims)
	h.decisions = own
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(
		fencedDispatch(claims, countingAdapter{n: dispatches, outcome: outcome}, nil))
	h.env.OnActivity(infra.RevertStateCompensation, mock.Anything, mock.Anything).Return(nil)
	return h
}

// requireRejected asserts a non-retryable application error of errType and
// returns its reason code ("" when it carries none).
func requireRejected(t *testing.T, err error, errType string) contracts.ReasonCode {
	t.Helper()
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != errType {
		t.Fatalf("expected %s application error, got %v", errType, err)
	}
	if !appErr.NonRetryable() {
		t.Fatalf("%s must be non-retryable", errType)
	}
	var reason contracts.ReasonCode
	if appErr.HasDetails() {
		if err := appErr.Details(&reason); err != nil {
			t.Fatalf("decode error details: %v", err)
		}
	}
	return reason
}

func requireAlreadyClaimed(t *testing.T, h *harness) {
	t.Helper()
	if got := requireRejected(t, h.env.GetWorkflowError(), contracts.ExecutionClaimRejectedErrorType); got != contracts.ReasonIngressDecisionAlreadyClaimed {
		t.Fatalf("reason = %q, want %s", got, contracts.ReasonIngressDecisionAlreadyClaimed)
	}
	requireNoInfrastructure(t, h)
	if h.ran("EvaluateExecution") {
		t.Fatal("re-validation ran for an execution that does not hold the claim")
	}
}

func requireClaim(t *testing.T, s decision.ClaimStore, id string, want contracts.ClaimState) contracts.ExecutionClaim {
	t.Helper()
	c, err := s.GetClaim(context.Background(), id)
	if err != nil || c.State != want {
		t.Fatalf("claim on %s = %+v, %v; want state %s", id, c, err, want)
	}
	return c
}

// execute(D1); execute(D1) must dispatch exactly once.
func TestReplayAfterCompletionDispatchesOnce(t *testing.T) {
	for _, kind := range storeKinds {
		t.Run(kind.name, func(t *testing.T) {
			s := kind.open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			var dispatches atomic.Int64

			first := newExecution(t, s, s, &dispatches, succeeded)
			first.run(workflowIDFor(req), req)
			if err := first.env.GetWorkflowError(); err != nil {
				t.Fatalf("first execution failed: %v", err)
			}
			done := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)

			second := newExecution(t, s, s, &dispatches, succeeded)
			second.run(workflowIDFor(req), req)
			requireAlreadyClaimed(t, second)

			if n := dispatches.Load(); n != 1 {
				t.Fatalf("dispatches = %d, want 1", n)
			}
			if after := requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted); after.Owner != done.Owner {
				t.Fatalf("replay changed the claim owner: %+v -> %+v", done.Owner, after.Owner)
			}
			audits := second.audit.All()
			if len(audits) != 1 || audits[0].Phase != contracts.AuditPhaseRevalidation || audits[0].Verdict != contracts.AuditVerdictFail {
				t.Fatalf("expected one FAIL audit row for the replay, got %+v", audits)
			}
		})
	}
}

// A replay rejected at the claim step leaves a DENY decision record.
func TestClaimRejectionIsRecorded(t *testing.T) {
	h := newHarness(t)
	var infra *workflows.InfrastructureActivities
	h.env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()

	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	h.seed(t, ingressAllow(req))
	previous := contracts.ClaimOwner{WorkflowID: workflowIDFor(req), RunID: "earlier-run", Token: "exe_earlier"}
	ctx := context.Background()
	if err := h.decisions.ClaimExecution(ctx, req.IngressDecisionID, previous, ingressTime); err != nil {
		t.Fatal(err)
	}
	for _, to := range []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateCompleted} {
		if err := h.decisions.AdvanceClaim(ctx, req.IngressDecisionID, previous, to, ingressTime); err != nil {
			t.Fatal(err)
		}
	}

	h.run(workflowIDFor(req), req)

	requireAlreadyClaimed(t, h)
	h.env.AssertExpectations(t)
	recs := h.workflowRecords()
	if len(recs) != 1 || recs[0].Verdict != contracts.VerdictDeny ||
		len(recs[0].Reasons) != 1 || recs[0].Reasons[0] != contracts.ReasonIngressDecisionAlreadyClaimed {
		t.Fatalf("expected a recorded DENY with %s, got %+v", contracts.ReasonIngressDecisionAlreadyClaimed, recs)
	}
	if c := requireClaim(t, h.decisions, req.IngressDecisionID, contracts.ClaimStateCompleted); c.Owner != previous {
		t.Fatalf("claim owner changed: %+v", c.Owner)
	}
}

// claimBarrier holds every ClaimExecution call until parties callers have
// arrived, so concurrent executions race on the claim itself.
type claimBarrier struct {
	decision.ClaimStore
	parties  int32
	arrived  atomic.Int32
	ready    chan struct{}
	timedOut atomic.Bool
}

func newClaimBarrier(s decision.ClaimStore, parties int) *claimBarrier {
	return &claimBarrier{ClaimStore: s, parties: int32(parties), ready: make(chan struct{})}
}

func (b *claimBarrier) ClaimExecution(ctx context.Context, id string, o contracts.ClaimOwner, at time.Time) error {
	if b.arrived.Add(1) == b.parties {
		close(b.ready)
	}
	select {
	case <-b.ready:
	case <-time.After(2 * time.Second):
		b.timedOut.Store(true)
	}
	return b.ClaimStore.ClaimExecution(ctx, id, o, at)
}

// Concurrent execute(D1) calls must dispatch exactly once.
func TestConcurrentExecutionsDispatchOnce(t *testing.T) {
	for _, kind := range storeKinds {
		for _, n := range []int{2, 4} {
			t.Run(fmt.Sprintf("%s/%d executions", kind.name, n), func(t *testing.T) {
				s := kind.open(t)
				req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
				seedInto(t, s, ingressAllow(req))
				barrier := newClaimBarrier(s, n)
				var dispatches atomic.Int64

				execs := make([]*harness, n)
				for i := range execs {
					execs[i] = newExecution(t, s, barrier, &dispatches, succeeded)
				}
				var wg sync.WaitGroup
				for _, h := range execs {
					wg.Add(1)
					go func() {
						defer wg.Done()
						h.run(workflowIDFor(req), req)
					}()
				}
				wg.Wait()

				if barrier.timedOut.Load() {
					t.Fatal("executions did not reach the claim step together")
				}
				succeeded := 0
				for _, h := range execs {
					if h.env.GetWorkflowError() == nil {
						succeeded++
						continue
					}
					requireAlreadyClaimed(t, h)
				}
				if succeeded != 1 {
					t.Fatalf("%d executions succeeded, want exactly 1", succeeded)
				}
				if got := dispatches.Load(); got != 1 {
					t.Fatalf("dispatches = %d, want 1", got)
				}
				requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)
			})
		}
	}
}

// Changing the workflow ID cannot be used to get a second claim: the ingress
// binding rejects it before the claim step, before or after the real run.
func TestDifferentWorkflowIDCannotBypassClaim(t *testing.T) {
	for _, kind := range storeKinds {
		t.Run(kind.name, func(t *testing.T) {
			s := kind.open(t)
			req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			seedInto(t, s, ingressAllow(req))
			var dispatches atomic.Int64

			shadow := func() {
				t.Helper()
				h := newExecution(t, s, s, &dispatches, succeeded)
				h.run("saga-shadow", req)
				if got := requireIngressInvalid(t, h.env.GetWorkflowError()); got != contracts.ReasonIngressIdentityMismatch {
					t.Fatalf("reason = %q, want %s", got, contracts.ReasonIngressIdentityMismatch)
				}
				if h.ran("ClaimExecution") {
					t.Fatal("execution under another workflow ID reached the claim step")
				}
			}

			shadow()
			if _, err := s.GetClaim(context.Background(), req.IngressDecisionID); !errors.Is(err, contracts.ErrClaimNotFound) {
				t.Fatalf("rejected execution claimed the decision: %v", err)
			}

			real := newExecution(t, s, s, &dispatches, succeeded)
			real.run(workflowIDFor(req), req)
			if err := real.env.GetWorkflowError(); err != nil {
				t.Fatalf("legitimate execution failed: %v", err)
			}

			shadow()
			if got := dispatches.Load(); got != 1 {
				t.Fatalf("dispatches = %d, want 1", got)
			}
		})
	}
}

// Another agent cannot use agent-a's decision, and a failed attempt does not
// consume the claim.
func TestOtherAgentCannotUseDecision(t *testing.T) {
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	stolen := req
	stolen.AgentID = "agent-b"
	var dispatches atomic.Int64

	attempt := func() {
		t.Helper()
		h := newExecution(t, s, s, &dispatches, succeeded)
		h.run(workflowIDFor(req), stolen)
		if got := requireIngressInvalid(t, h.env.GetWorkflowError()); got != contracts.ReasonIngressAgentMismatch {
			t.Fatalf("reason = %q, want %s", got, contracts.ReasonIngressAgentMismatch)
		}
		if h.ran("ClaimExecution") {
			t.Fatal("another agent's execution reached the claim step")
		}
	}

	attempt()
	owner := newExecution(t, s, s, &dispatches, succeeded)
	owner.run(workflowIDFor(req), req)
	if err := owner.env.GetWorkflowError(); err != nil {
		t.Fatalf("owning agent's execution failed after a rejected attempt: %v", err)
	}
	attempt()

	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
}

// Each legitimate decision authorizes its own single execution, including a
// second decision for the same proposal ID (a fresh submission to the proxy).
func TestIndependentDecisionsEachExecuteOnce(t *testing.T) {
	for _, kind := range storeKinds {
		t.Run(kind.name, func(t *testing.T) {
			s := kind.open(t)
			d1 := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			d2 := request("agent-a", contracts.CmdModifyRouting, "edge-1")
			d2.Proposal.ID = "prop-edge-1-second"
			d2.IngressDecisionID = "dec_ingress_prop-edge-1-second"
			d2.RequestHash = decision.RequestHash(d2.Proposal)
			d3 := d1
			d3.IngressDecisionID = "dec_ingress_resubmitted"
			reqs := []contracts.ExecutionRequest{d1, d2, d3}
			for _, r := range reqs {
				seedInto(t, s, ingressAllow(r))
			}
			var dispatches atomic.Int64

			tokens := map[string]bool{}
			for _, r := range reqs {
				h := newExecution(t, s, s, &dispatches, succeeded)
				h.run(workflowIDFor(r), r)
				if err := h.env.GetWorkflowError(); err != nil {
					t.Fatalf("%s: %v", r.IngressDecisionID, err)
				}
				tokens[requireClaim(t, s, r.IngressDecisionID, contracts.ClaimStateCompleted).Owner.Token] = true
			}
			if len(tokens) != len(reqs) {
				t.Fatalf("executions shared claim tokens: %v", tokens)
			}
			for _, r := range reqs {
				h := newExecution(t, s, s, &dispatches, succeeded)
				h.run(workflowIDFor(r), r)
				requireAlreadyClaimed(t, h)
			}
			if got := dispatches.Load(); got != int64(len(reqs)) {
				t.Fatalf("dispatches = %d, want %d", got, len(reqs))
			}
		})
	}
}

// faultyClaims injects claim-store failures. A fault with commit set runs the
// real operation first and then reports an error, as when a worker crashes or
// loses the response after the database committed.
type faultyClaims struct {
	decision.ClaimStore
	mu      sync.Mutex
	calls   map[contracts.ClaimState]int // "" counts ClaimExecution
	faultAt func(op contracts.ClaimState, call int) (err error, commit bool)
}

func newFaultyClaims(s decision.ClaimStore, faultAt func(op contracts.ClaimState, call int) (error, bool)) *faultyClaims {
	return &faultyClaims{ClaimStore: s, calls: map[contracts.ClaimState]int{}, faultAt: faultAt}
}

func (f *faultyClaims) fault(op contracts.ClaimState, real func() error) error {
	f.mu.Lock()
	f.calls[op]++
	call := f.calls[op]
	f.mu.Unlock()
	err, commit := f.faultAt(op, call)
	if err == nil {
		return real()
	}
	if commit {
		_ = real()
	}
	return err
}

func (f *faultyClaims) ClaimExecution(ctx context.Context, id string, o contracts.ClaimOwner, at time.Time) error {
	return f.fault("", func() error { return f.ClaimStore.ClaimExecution(ctx, id, o, at) })
}

func (f *faultyClaims) AdvanceClaim(ctx context.Context, id string, o contracts.ClaimOwner, to contracts.ClaimState, at time.Time) error {
	return f.fault(to, func() error { return f.ClaimStore.AdvanceClaim(ctx, id, o, to, at) })
}

var (
	errStoreDown    = errors.New("disk I/O error")
	errResponseLost = errors.New("connection reset after commit")
)

func TestClaimStoreFailureFailsClosed(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == "" {
			return errStoreDown, false
		}
		return nil, false
	})
	var dispatches atomic.Int64
	h := newExecution(t, s, claims, &dispatches, succeeded)

	h.run(workflowIDFor(req), req)

	requireRejected(t, h.env.GetWorkflowError(), contracts.ExecutionClaimRejectedErrorType)
	requireNoInfrastructure(t, h)
	if h.ran("EvaluateExecution") {
		t.Fatal("re-validation ran without a claim")
	}
	if dispatches.Load() != 0 {
		t.Fatal("dispatched without a claim")
	}
	if _, err := s.GetClaim(context.Background(), req.IngressDecisionID); !errors.Is(err, contracts.ErrClaimNotFound) {
		t.Fatalf("claim = %v, want none", err)
	}
}

func TestMissingClaimStoreFailsClosed(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	env.RegisterActivity(&workflows.DecisionActivities{Policy: &swappableSource{}, Store: s})
	env.RegisterActivity(&workflows.AuditActivities{Store: workflows.NewMemoryAuditStore()})
	var infra *workflows.InfrastructureActivities
	env.OnActivity(infra.DispatchConfig, mock.Anything, mock.Anything).Return(succeeded, nil).Never()

	h := &harness{env: env}
	h.run(workflowIDFor(req), req)

	requireRejected(t, env.GetWorkflowError(), contracts.ExecutionClaimRejectedErrorType)
	env.AssertExpectations(t)
}

// If the execution fence cannot be written, the adapter is not called. The
// claim is still CLAIMED, so the workflow releases it and the same decision
// can still be executed later.
func TestFenceWriteFailureReleasesClaim(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == contracts.ClaimStateExecuting {
			return errStoreDown, false
		}
		return nil, false
	})
	var dispatches atomic.Int64

	h := newExecution(t, s, claims, &dispatches, succeeded)
	h.run(workflowIDFor(req), req)
	requireRejected(t, h.env.GetWorkflowError(), contracts.ExecutionClaimRejectedErrorType)
	if dispatches.Load() != 0 {
		t.Fatal("adapter called without the execution fence")
	}
	if h.count("DispatchConfig") != 1 || h.ran("ReconcileDispatch") {
		t.Fatalf("activities = %v", h.order())
	}
	if got, want := phases(h), []string{"WORKFLOW_REVALIDATION PASS", "EXECUTION_FENCE FAIL"}; !slices.Equal(got, want) {
		t.Fatalf("audit = %v, want %v", got, want)
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReleased)

	retry := newExecution(t, s, s, &dispatches, succeeded)
	retry.run(workflowIDFor(req), req)
	if err := retry.env.GetWorkflowError(); err != nil {
		t.Fatalf("released decision could not be executed: %v", err)
	}
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
}

// A claim committed by an attempt whose result was lost (worker crash or
// network failure after commit) is retried by Temporal under the same owner
// and succeeds; the execution still dispatches exactly once. (A lost fence
// response is not retried: see TestLostFenceResponseIsReconciledNotDispatched.)
func TestLostClaimResponseIsRetriedIdempotently(t *testing.T) {
	s := storeKinds[1].open(t)
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, call int) (error, bool) {
		if op == "" && call == 1 {
			return errResponseLost, true
		}
		return nil, false
	})
	var dispatches atomic.Int64

	h := newExecution(t, s, claims, &dispatches, succeeded)
	h.run(workflowIDFor(req), req)

	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatalf("execution failed after a retried claim: %v", err)
	}
	if h.count("ClaimExecution") < 2 {
		t.Fatalf("ClaimExecution was not retried: %v", h.order())
	}
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateCompleted)
}

// The workflow cannot move a claim to EXECUTING itself: only the dispatch
// activity acquires the fence.
func TestAdvanceExecutionClaimRefusesExecuting(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	const token = "exe_fence"
	holdClaim(t, s, req.IngressDecisionID, contracts.ClaimOwner{WorkflowID: testWorkflowID, RunID: testRunID, Token: token})

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	acts := &workflows.DecisionActivities{Store: s, Claims: s}
	env.RegisterActivity(acts)
	_, err := env.ExecuteActivity(acts.AdvanceExecutionClaim,
		contracts.ClaimTransition{DecisionID: req.IngressDecisionID, Token: token, To: contracts.ClaimStateExecuting})

	requireFenceError(t, err, contracts.ExecutionClaimRejectedErrorType)
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateClaimed)
}

// A denial at re-validation happens before EXECUTING, so the claim is
// released. A later execution of the same decision still needs to pass
// re-validation, and still dispatches at most once.
func TestRevalidationDenialReleasesClaim(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	var dispatches atomic.Int64

	denied := newExecution(t, s, s, &dispatches, succeeded)
	denied.src.p.Store(parsePolicy(t, tightenedPolicy))
	denied.run(workflowIDFor(req), req)
	requireRevalidationDenied(t, denied.env.GetWorkflowError())
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReleased)

	allowed := newExecution(t, s, s, &dispatches, succeeded)
	allowed.run(workflowIDFor(req), req)
	if err := allowed.env.GetWorkflowError(); err != nil {
		t.Fatalf("execution after release failed: %v", err)
	}

	replay := newExecution(t, s, s, &dispatches, succeeded)
	replay.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, replay)

	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
}

// If the release itself cannot be written, the claim stays CLAIMED: the
// decision cannot be executed again, which fails closed.
func TestReleaseFailureLeavesDecisionUnusable(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == contracts.ClaimStateReleased {
			return errStoreDown, false
		}
		return nil, false
	})
	var dispatches atomic.Int64

	denied := newExecution(t, s, claims, &dispatches, succeeded)
	denied.src.p.Store(parsePolicy(t, tightenedPolicy))
	denied.run(workflowIDFor(req), req)
	requireRevalidationDenied(t, denied.env.GetWorkflowError())
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateClaimed)

	next := newExecution(t, s, s, &dispatches, succeeded)
	next.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, next)
	if dispatches.Load() != 0 {
		t.Fatal("dispatched after a failed release")
	}
}

// A confirmed dispatch failure is terminal: the decision is never dispatched
// again.
func TestFailedDispatchIsNotRetriedByReplay(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdUpdateCert, contracts.FailingNodeID)
	seedInto(t, s, ingressAllow(req))
	var dispatches atomic.Int64

	first := newExecution(t, s, s, &dispatches, failedPartly)
	first.run(workflowIDFor(req), req)
	requireRejected(t, first.env.GetWorkflowError(), contracts.DispatchFailedErrorType)
	if first.count("RevertStateCompensation") != 1 {
		t.Fatalf("compensation did not run once: %v", first.order())
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateFailed)

	replay := newExecution(t, s, s, &dispatches, succeeded)
	replay.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, replay)
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
}

// If COMPLETED cannot be written after a successful dispatch, the claim stays
// EXECUTING, which still blocks every later execution.
func TestCompletionWriteFailureStillBlocksReplay(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	claims := newFaultyClaims(s, func(op contracts.ClaimState, _ int) (error, bool) {
		if op == contracts.ClaimStateCompleted {
			return errStoreDown, false
		}
		return nil, false
	})
	var dispatches atomic.Int64

	first := newExecution(t, s, claims, &dispatches, succeeded)
	first.run(workflowIDFor(req), req)
	if first.env.GetWorkflowError() == nil {
		t.Fatal("expected the workflow to report the failed claim write")
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateExecuting)

	replay := newExecution(t, s, s, &dispatches, succeeded)
	replay.run(workflowIDFor(req), req)
	requireAlreadyClaimed(t, replay)
	if got := dispatches.Load(); got != 1 {
		t.Fatalf("dispatches = %d, want 1", got)
	}
}

// A workflow cancelled while it holds the claim, before dispatch, releases it
// from a disconnected context.
func TestCancellationBeforeDispatchReleasesClaim(t *testing.T) {
	s := decision.NewMemoryStore()
	req := request("agent-a", contracts.CmdModifyRouting, "edge-1")
	seedInto(t, s, ingressAllow(req))
	var dispatches atomic.Int64

	h := newExecution(t, s, s, &dispatches, succeeded)
	var decisions *workflows.DecisionActivities
	h.env.OnActivity(decisions.EvaluateExecution, mock.Anything, mock.Anything).
		After(time.Hour).Return(contracts.Decision{Verdict: contracts.VerdictAllow}, nil)
	h.env.RegisterDelayedCallback(h.env.CancelWorkflow, time.Minute)

	h.run(workflowIDFor(req), req)

	var canceled *temporal.CanceledError
	if err := h.env.GetWorkflowError(); !errors.As(err, &canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if dispatches.Load() != 0 {
		t.Fatal("dispatched after cancellation")
	}
	requireClaim(t, s, req.IngressDecisionID, contracts.ClaimStateReleased)
}
