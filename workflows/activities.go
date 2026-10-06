package workflows

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Adapter applies proposals to infrastructure. The only implementation today is
// SimulatedAdapter.
//
// Dispatch applies req.Proposal and reports what it knows about the effect.
// SUCCESS and FAILURE must be confirmed by the target. Anything ambiguous, such
// as a timeout, a reset connection or a response that does not say whether the
// change was made, must be reported as UNKNOWN: an unknown outcome is not
// equivalent to failure. A given req.IdempotencyKey must take effect at most
// once.
//
// Reconcile reports the effect of the earlier dispatch with the same
// idempotency key, without applying anything. It may report FAILURE only if
// that mutation has not taken effect and can no longer take effect, for
// example because the target now rejects the key; otherwise a request still in
// flight could apply it after FAILED was recorded. It reports UNKNOWN when it
// cannot tell.
//
// Compensate reverts a mutation that the target confirmed took effect, in full
// or in part.
type Adapter interface {
	Dispatch(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome
	Reconcile(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome
	Compensate(ctx context.Context, req contracts.DispatchRequest) error
}

// InfrastructureActivities exposes an Adapter as Temporal activities. The
// workflow schedules DispatchConfig at most once per execution and never
// retries it; ReconcileDispatch may run many times. Claims is the execution
// claim store that DispatchConfig fences on.
type InfrastructureActivities struct {
	Adapter Adapter
	Claims  decision.ClaimStore
}

// DispatchConfig acquires the execution fence for req.DecisionID and, only if
// that succeeds, calls the adapter once.
//
// The fence moves the claim from CLAIMED to EXECUTING with a compare-and-set
// that matches the owner: the workflow and run IDs from this activity's
// Temporal execution info and req.Token. A run created by a Temporal reset has
// a new run ID, and a second attempt by the owner finds the claim already
// EXECUTING, so neither can acquire it.
//
// Whenever the adapter was not called, DispatchConfig returns a non-retryable
// application error and no outcome:
//   - ExecutionFenceRejectedErrorType when the claim was not changed: the run
//     does not hold it in CLAIMED, or the request cannot be dispatched.
//   - ExecutionFenceUnknownErrorType when the fence write failed and may have
//     committed, or the claim shows that this run already acquired the fence.
//
// Once the adapter has been called, DispatchConfig reports its outcome and
// never returns an error.
func (a *InfrastructureActivities) DispatchConfig(ctx context.Context, req contracts.FencedDispatch) (contracts.DispatchOutcome, error) {
	switch {
	case a.Adapter == nil:
		return contracts.DispatchOutcome{}, fenceRejected("no infrastructure adapter configured", nil)
	case a.Claims == nil:
		return contracts.DispatchOutcome{}, fenceRejected("execution claim store not configured", nil)
	case req.DecisionID == "" || req.Token == "":
		return contracts.DispatchOutcome{}, fenceRejected("request is missing the decision ID or claim token", nil)
	case req.Dispatch.IdempotencyKey != decision.ExecutionKey(req.DecisionID):
		return contracts.DispatchOutcome{}, fenceRejected("idempotency key does not belong to the fenced decision", nil)
	}
	owner := claimOwner(ctx, req.Token)
	if err := a.Claims.AdvanceClaim(ctx, req.DecisionID, owner, contracts.ClaimStateExecuting, time.Now()); err != nil {
		if !errors.Is(err, contracts.ErrClaimNotHeld) {
			return contracts.DispatchOutcome{}, fenceUnknown("execution fence write failed and may have committed", err)
		}
		// The claim was not CLAIMED by this run. If it belongs to this run in
		// a later state, an earlier attempt acquired the fence and may have
		// called the adapter; that attempt's outcome is unknown.
		held, gerr := a.Claims.GetClaim(ctx, req.DecisionID)
		switch {
		case errors.Is(gerr, contracts.ErrClaimNotFound):
		case gerr != nil:
			return contracts.DispatchOutcome{}, fenceUnknown("execution fence rejected and the claim could not be read", gerr)
		case held.Owner == owner && held.State != contracts.ClaimStateReleased:
			return contracts.DispatchOutcome{}, fenceUnknown("this run already acquired the execution fence", err)
		}
		return contracts.DispatchOutcome{}, fenceRejected("execution fence is not held by this run", err)
	}
	return a.Adapter.Dispatch(ctx, req.Dispatch).Checked(), nil
}

func fenceRejected(msg string, cause error) error {
	return temporal.NewNonRetryableApplicationError("adapter not called: "+msg, contracts.ExecutionFenceRejectedErrorType, cause)
}

func fenceUnknown(msg string, cause error) error {
	return temporal.NewNonRetryableApplicationError("adapter not called: "+msg, contracts.ExecutionFenceUnknownErrorType, cause)
}

// ReconcileDispatch asks the adapter for the effect of the dispatch identified
// by req.IdempotencyKey. It never dispatches.
func (a *InfrastructureActivities) ReconcileDispatch(ctx context.Context, req contracts.DispatchRequest) (contracts.DispatchOutcome, error) {
	switch {
	case a.Adapter == nil:
		return contracts.UnknownOutcome("cannot reconcile: no infrastructure adapter configured"), nil
	case req.IdempotencyKey == "":
		return contracts.UnknownOutcome("cannot reconcile: request has no idempotency key"), nil
	}
	return a.Adapter.Reconcile(ctx, req).Checked(), nil
}

// RevertStateCompensation reverts the mutation identified by req.
func (a *InfrastructureActivities) RevertStateCompensation(ctx context.Context, req contracts.DispatchRequest) error {
	if a.Adapter == nil {
		return errors.New("cannot compensate: no infrastructure adapter configured")
	}
	return a.Adapter.Compensate(ctx, req)
}
