package contracts

import (
	"errors"
	"time"
)

// ClaimState is the execution state of an ingress ALLOW decision. A decision
// with no claim is unclaimed. Transitions:
//
//	(unclaimed) ──claim──► CLAIMED ──fence──► EXECUTING ──SUCCESS──► COMPLETED
//	                         │  ▲                 │  └─────FAILURE──► FAILED
//	                 release │  │ claim           │ UNKNOWN
//	                         ▼  │                 ▼
//	                       RELEASED   RECONCILIATION_REQUIRED ──SUCCESS──► COMPLETED
//	                                                       └───FAILURE──► FAILED
//
// The fence is the move from CLAIMED to EXECUTING. Only the dispatch activity
// makes it, as a compare-and-set immediately before it calls the
// infrastructure adapter, and it calls the adapter only if that write
// succeeded. CLAIMED and RELEASED therefore guarantee that the adapter has not
// been called for the decision, and EXECUTING means the adapter call may have
// started. From EXECUTING on, the decision can never be claimed again or
// released, whatever the outcome. COMPLETED and FAILED record a confirmed
// outcome. RECONCILIATION_REQUIRED records that the outcome is not known: the
// mutation may or may not have taken effect. It is not terminal; it moves to
// COMPLETED or FAILED once reconciliation confirms an outcome. Every state
// except EXECUTING also accepts a repeat of the transition that produced it
// from the same owner, so activity retries are idempotent. The fence is not
// repeatable: a second attempt to enter EXECUTING fails, even from the owner.
type ClaimState string

const (
	ClaimStateClaimed                ClaimState = "CLAIMED"
	ClaimStateExecuting              ClaimState = "EXECUTING"
	ClaimStateReconciliationRequired ClaimState = "RECONCILIATION_REQUIRED"
	ClaimStateCompleted              ClaimState = "COMPLETED"
	ClaimStateFailed                 ClaimState = "FAILED"
	ClaimStateReleased               ClaimState = "RELEASED"
)

// ClaimOwner identifies the single workflow execution that holds a claim.
// WorkflowID and RunID come from Temporal; Token is generated once per run and
// stored in workflow history, so it survives worker restarts and replay.
type ClaimOwner struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Token      string `json:"token"`
}

// Valid reports whether every owner field is set.
func (o ClaimOwner) Valid() bool {
	return o.WorkflowID != "" && o.RunID != "" && o.Token != ""
}

// ExecutionClaim is the stored claim on one ingress decision.
type ExecutionClaim struct {
	DecisionID string     `json:"decision_id"`
	Owner      ClaimOwner `json:"owner"`
	State      ClaimState `json:"state"`
	ClaimedAt  time.Time  `json:"claimed_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

var (
	// ErrClaimHeld is returned when a decision is claimed, executing or
	// finished under a different owner.
	ErrClaimHeld = errors.New("ingress decision is already claimed by another execution")
	// ErrClaimNotHeld is returned when a transition is requested by an
	// execution that does not own the claim, or from a state that does not
	// permit it.
	ErrClaimNotHeld = errors.New("execution does not hold the claim in a state that permits this transition")
	// ErrClaimNotFound is returned when a decision has never been claimed.
	ErrClaimNotFound = errors.New("execution claim not found")
)

// ClaimRequest asks to claim an ingress decision for the calling workflow run.
type ClaimRequest struct {
	DecisionID string `json:"decision_id"`
	Token      string `json:"token"`
}

// ClaimResult reports whether a claim was acquired. The zero value is not a
// claim.
type ClaimResult struct {
	Claimed bool       `json:"claimed"`
	Reason  ReasonCode `json:"reason,omitempty"`
	Detail  string     `json:"detail,omitempty"`
}

// ClaimTransition moves a claim owned by the calling workflow run to To.
type ClaimTransition struct {
	DecisionID string     `json:"decision_id"`
	Token      string     `json:"token"`
	To         ClaimState `json:"to"`
}

// ExecutionClaimRejectedErrorType is the Temporal application error type
// returned when the workflow cannot claim its ingress decision. When the
// decision is already claimed, the error's details carry
// ReasonIngressDecisionAlreadyClaimed.
const ExecutionClaimRejectedErrorType = "EXECUTION_CLAIM_REJECTED"

// Error types returned by the dispatch activity when it did not call the
// infrastructure adapter.
const (
	// ExecutionFenceRejectedErrorType: the fence was not acquired. The calling
	// run does not hold the claim in CLAIMED, or the request could not be
	// dispatched at all, so the claim was not changed.
	ExecutionFenceRejectedErrorType = "EXECUTION_FENCE_REJECTED"
	// ExecutionFenceUnknownErrorType: the fence write failed and may or may
	// not have committed, or an earlier attempt by the same run may already
	// have acquired it.
	ExecutionFenceUnknownErrorType = "EXECUTION_FENCE_UNKNOWN"
)
