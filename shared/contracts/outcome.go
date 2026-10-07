package contracts

// OutcomeStatus is what an infrastructure adapter knows about the effect of a
// dispatched mutation on its target.
//
// An unknown outcome is not equivalent to failure. A timeout, a reset
// connection or a crashed worker means the caller did not learn the result;
// the mutation may still have reached the target and taken effect. SentryGate
// therefore never treats UNKNOWN as FAILURE: it does not release the execution
// claim, does not dispatch again and does not compensate. It asks the adapter
// to reconcile with the same idempotency key, and the claim stays in
// RECONCILIATION_REQUIRED until a confirmed outcome is known.
type OutcomeStatus string

const (
	// OutcomeSuccess: the target confirmed that the mutation took effect.
	OutcomeSuccess OutcomeStatus = "SUCCESS"
	// OutcomeFailure: the target confirmed that the mutation did not take
	// effect and can no longer take effect, except for the part reported by
	// DispatchOutcome.PartiallyApplied.
	OutcomeFailure OutcomeStatus = "FAILURE"
	// OutcomeUnknown: whether the mutation took effect is not known.
	OutcomeUnknown OutcomeStatus = "UNKNOWN"
)

// DispatchOutcome is the result of a dispatch or reconciliation attempt.
type DispatchOutcome struct {
	Status OutcomeStatus `json:"status"`
	// PartiallyApplied is meaningful only with OutcomeFailure: the target
	// confirmed that part of the mutation took effect before it failed, so the
	// saga must compensate.
	PartiallyApplied bool   `json:"partially_applied,omitempty"`
	Detail           string `json:"detail,omitempty"`
}

// UnknownOutcome returns an UNKNOWN outcome carrying detail.
func UnknownOutcome(detail string) DispatchOutcome {
	return DispatchOutcome{Status: OutcomeUnknown, Detail: detail}
}

// Checked returns o with an unrecognised or empty status turned into UNKNOWN,
// so an adapter that does not report a status is never read as success or
// failure. PartiallyApplied is kept only with FAILURE.
func (o DispatchOutcome) Checked() DispatchOutcome {
	switch o.Status {
	case OutcomeSuccess, OutcomeUnknown:
		o.PartiallyApplied = false
		return o
	case OutcomeFailure:
		return o
	}
	return UnknownOutcome("adapter reported unrecognised status " + string(o.Status) + ": " + o.Detail)
}

// DispatchRequest is what the workflow passes to an infrastructure adapter for
// both dispatch and reconciliation. IdempotencyKey identifies the single
// execution authorized by one ingress decision; it is the same for every
// attempt, retry and reconciliation of that execution. AgentID is the agent
// the ingress decision was recorded for; the workflow has verified it against
// that record before dispatch.
type DispatchRequest struct {
	IdempotencyKey string        `json:"idempotency_key"`
	AgentID        string        `json:"agent_id,omitempty"`
	Proposal       AgentProposal `json:"proposal"`
}

// FencedDispatch is the input of the dispatch activity. DecisionID and Token
// name the execution claim that the activity must move from CLAIMED to
// EXECUTING before it may call the adapter; the workflow and run that own the
// claim come from Temporal's activity info, not from this input. Only Dispatch
// is passed to the adapter, and its IdempotencyKey must be the one derived
// from DecisionID.
type FencedDispatch struct {
	DecisionID string          `json:"decision_id"`
	Token      string          `json:"token"`
	Dispatch   DispatchRequest `json:"dispatch"`
}

// DispatchFailedErrorType is the Temporal application error type returned
// when the adapter confirmed that the mutation failed.
const DispatchFailedErrorType = "DISPATCH_FAILED"

// DispatchOutcomeUnknownErrorType is the Temporal application error type
// returned when the workflow ends without a confirmed outcome. The execution
// claim is left in RECONCILIATION_REQUIRED.
const DispatchOutcomeUnknownErrorType = "DISPATCH_OUTCOME_UNKNOWN"
