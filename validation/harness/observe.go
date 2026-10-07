package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Run outcome statuses.
const (
	RunCompleted  = "COMPLETED"
	RunFailed     = "FAILED"
	RunTerminated = "TERMINATED"
	RunOpen       = "STILL_OPEN"
	RunUnknown    = "UNKNOWN"
)

// RunOutcome is how a workflow run ended, as reported by Temporal.
type RunOutcome struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
	ErrorType  string `json:"error_type,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
}

// WaitRun waits up to timeout for the run to close. A run that is still open
// afterwards is reported as STILL_OPEN, not as a failure.
func (st *Stack) WaitRun(ctx context.Context, workflowID, runID string, timeout time.Duration) RunOutcome {
	o := RunOutcome{WorkflowID: workflowID, RunID: runID}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := st.Temporal.GetWorkflow(wctx, workflowID, runID).Get(wctx, nil)
	if err == nil {
		o.Status = RunCompleted
		return o
	}
	if wctx.Err() != nil {
		o.Status = RunOpen
		o.Message = "run did not close within " + timeout.String()
		return o
	}
	var terminated *temporal.TerminatedError
	var appErr *temporal.ApplicationError
	switch {
	case errors.As(err, &terminated):
		o.Status = RunTerminated
	case errors.As(err, &appErr):
		o.Status = RunFailed
		o.ErrorType = appErr.Type()
		o.Message = appErr.Error()
		var reason string
		if appErr.HasDetails() && appErr.Details(&reason) == nil {
			o.Reason = reason
		}
	default:
		o.Status = RunUnknown
		o.Message = err.Error()
	}
	// Temporal may report a continued or reset run; record the real status.
	if d, derr := st.Temporal.DescribeWorkflowExecution(ctx, workflowID, runID); derr == nil {
		if s := d.GetWorkflowExecutionInfo().GetStatus(); s == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED {
			o.Status = RunTerminated
		}
	}
	return o
}

// HistoryEvent is the part of a workflow history event the evidence needs.
type HistoryEvent struct {
	EventID      int64  `json:"event_id"`
	Type         string `json:"type"`
	Activity     string `json:"activity,omitempty"`
	ClaimTo      string `json:"claim_transition_to,omitempty"`
	FailureType  string `json:"failure_type,omitempty"`
	FailureCause string `json:"failure,omitempty"`
}

// RunHistory is a summary of one run's history.
type RunHistory struct {
	WorkflowID string         `json:"workflow_id"`
	RunID      string         `json:"run_id"`
	Events     []HistoryEvent `json:"events"`
	// ClaimResetPoint is the workflow task completed right after the
	// ClaimExecution activity completed (0 if there is none).
	ClaimResetPoint int64 `json:"claim_reset_point,omitempty"`
}

// History reads and summarises a run's history: activities, claim transitions
// the workflow requested, activity failures and the close event.
func (st *Stack) History(ctx context.Context, workflowID, runID string) (RunHistory, error) {
	h := RunHistory{WorkflowID: workflowID, RunID: runID, Events: []HistoryEvent{}}
	scheduled := map[int64]string{}
	claimDone := false
	dc := converter.GetDefaultDataConverter()
	it := st.Temporal.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			return h, err
		}
		e := HistoryEvent{EventID: ev.GetEventId(), Type: ev.GetEventType().String()}
		keep := true
		switch ev.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			a := ev.GetActivityTaskScheduledEventAttributes()
			e.Activity = a.GetActivityType().GetName()
			scheduled[e.EventID] = e.Activity
			if e.Activity == "AdvanceExecutionClaim" {
				var t contracts.ClaimTransition
				if dc.FromPayloads(a.GetInput(), &t) == nil {
					e.ClaimTo = string(t.To)
				}
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			e.Activity = scheduled[ev.GetActivityTaskCompletedEventAttributes().GetScheduledEventId()]
			if e.Activity == "ClaimExecution" {
				claimDone = true
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
			a := ev.GetActivityTaskFailedEventAttributes()
			e.Activity = scheduled[a.GetScheduledEventId()]
			e.FailureType, e.FailureCause = a.GetFailure().GetApplicationFailureInfo().GetType(), a.GetFailure().GetMessage()
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
			a := ev.GetActivityTaskTimedOutEventAttributes()
			e.Activity = scheduled[a.GetScheduledEventId()]
			e.FailureCause = a.GetFailure().GetMessage()
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED:
			if claimDone && h.ClaimResetPoint == 0 {
				h.ClaimResetPoint = e.EventID
			}
			keep = false
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
			f := ev.GetWorkflowExecutionFailedEventAttributes().GetFailure()
			e.FailureType, e.FailureCause = f.GetApplicationFailureInfo().GetType(), f.GetMessage()
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, enumspb.EVENT_TYPE_MARKER_RECORDED:
		default:
			keep = false
		}
		if keep {
			h.Events = append(h.Events, e)
		}
	}
	return h, nil
}

// WaitHistoryEvent polls the run's history until pred matches an event.
func (st *Stack) WaitHistoryEvent(ctx context.Context, workflowID, runID string, timeout time.Duration, pred func(RunHistory) bool) (RunHistory, error) {
	deadline := time.Now().Add(timeout)
	for {
		h, err := st.History(ctx, workflowID, runID)
		if err == nil && pred(h) {
			return h, nil
		}
		if time.Now().After(deadline) {
			return h, fmt.Errorf("history condition not met within %s (last error: %v)", timeout, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ClaimView is an execution claim without its owner token.
type ClaimView struct {
	DecisionID      string `json:"decision_id"`
	State           string `json:"state"`
	OwnerWorkflowID string `json:"owner_workflow_id,omitempty"`
	OwnerRunID      string `json:"owner_run_id,omitempty"`
	ClaimedAt       string `json:"claimed_at,omitempty"`
	UpdatedAt       string `json:"updated_at,omitempty"`
}

// Claim returns the claim on decisionID, with State "NONE" if there is none.
func (st *Stack) Claim(ctx context.Context, decisionID string) ClaimView {
	c, err := st.Audit.GetClaim(ctx, decisionID)
	if errors.Is(err, contracts.ErrClaimNotFound) {
		return ClaimView{DecisionID: decisionID, State: "NONE"}
	}
	if err != nil {
		return ClaimView{DecisionID: decisionID, State: "UNREADABLE: " + err.Error()}
	}
	return ClaimView{
		DecisionID: decisionID, State: string(c.State),
		OwnerWorkflowID: c.Owner.WorkflowID, OwnerRunID: c.Owner.RunID,
		ClaimedAt: c.ClaimedAt.UTC().Format(time.RFC3339Nano), UpdatedAt: c.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
}

// SentryGateObservation is SentryGate's record of one workflow run, assembled
// from the audit store, the decision records, the execution claim and the
// run's Temporal history.
type SentryGateObservation struct {
	WorkflowID            string        `json:"workflow_id"`
	RunID                 string        `json:"run_id"`
	AgentIdentity         string        `json:"agent_identity"`
	Command               string        `json:"command"`
	Tool                  string        `json:"tool"`
	Target                string        `json:"target"`
	Environment           string        `json:"environment"`
	PolicyVersion         string        `json:"policy_version"`
	PolicyDigest          string        `json:"policy_digest"`
	RequestHash           string        `json:"request_hash"`
	IngressDecisionID     string        `json:"ingress_decision_id"`
	AuthorizationDecision string        `json:"authorization_decision"`
	RevalidationDecision  string        `json:"revalidation_decision"`
	ClaimState            string        `json:"claim_state"`
	ClaimOwnedByThisRun   bool          `json:"claim_owned_by_this_run"`
	ExecutionState        string        `json:"execution_state"`
	FenceResult           string        `json:"fence_result"`
	ReconciliationState   string        `json:"reconciliation_state"`
	FinalOutcome          string        `json:"final_outcome"`
	Claim                 ClaimView     `json:"claim"`
	History               RunHistory    `json:"history"`
	AuditTrail            []AuditEntry  `json:"audit_trail"`
	DecisionRecords       []DecisionRef `json:"decision_records"`
}

// AuditEntry is one audit record of the run.
type AuditEntry struct {
	Phase      string `json:"phase"`
	Verdict    string `json:"verdict"`
	Detail     string `json:"detail"`
	RecordedAt string `json:"recorded_at"`
}

// DecisionRef is a decision record relevant to the run.
type DecisionRef struct {
	DecisionID    string   `json:"decision_id"`
	Stage         string   `json:"stage"`
	AgentID       string   `json:"agent_id"`
	Command       string   `json:"command"`
	TargetID      string   `json:"target_id"`
	Environment   string   `json:"environment"`
	Verdict       string   `json:"verdict"`
	Reasons       []string `json:"reasons"`
	PolicyVersion string   `json:"policy_version"`
	PolicyDigest  string   `json:"policy_digest"`
	RequestHash   string   `json:"request_hash"`
	WorkflowID    string   `json:"workflow_id"`
	RecordedAt    string   `json:"recorded_at"`
}

func decisionRef(d contracts.DecisionRecord) DecisionRef {
	reasons := make([]string, len(d.Reasons))
	for i, r := range d.Reasons {
		reasons[i] = string(r)
	}
	return DecisionRef{
		DecisionID: d.DecisionID, Stage: string(d.Stage), AgentID: d.AgentID, Command: string(d.Command),
		TargetID: d.TargetID, Environment: d.Environment, Verdict: string(d.Verdict), Reasons: reasons,
		PolicyVersion: d.PolicyVersion, PolicyDigest: d.PolicyDigest, RequestHash: d.RequestHash,
		WorkflowID: d.WorkflowID, RecordedAt: d.RecordedAt.UTC().Format(time.RFC3339Nano),
	}
}

// Decision returns a decision record, or false if it does not exist.
func (st *Stack) Decision(ctx context.Context, id string) (DecisionRef, bool) {
	d, err := st.Audit.GetDecision(ctx, id)
	if err != nil {
		return DecisionRef{}, false
	}
	return decisionRef(d), true
}

// Decisions returns every decision recorded for a proposal ID.
func (st *Stack) Decisions(ctx context.Context, proposalID string) []DecisionRef {
	rows, err := st.Audit.ListDecisionsByProposal(ctx, proposalID)
	out := []DecisionRef{}
	if err != nil {
		return out
	}
	for _, d := range rows {
		out = append(out, decisionRef(d))
	}
	return out
}

// Observe assembles the SentryGate observation of a run that executed
// req, the execution request the run was started with.
func (st *Stack) Observe(ctx context.Context, outcome RunOutcome, req contracts.ExecutionRequest) SentryGateObservation {
	prop := req.Proposal
	o := SentryGateObservation{
		WorkflowID: outcome.WorkflowID, RunID: outcome.RunID, AgentIdentity: req.AgentID,
		Command: string(prop.Type), Tool: strings.ToLower(string(prop.Type)), Target: prop.TargetID,
		IngressDecisionID: req.IngressDecisionID, AuditTrail: []AuditEntry{}, DecisionRecords: []DecisionRef{},
	}
	if d, ok := st.Decision(ctx, req.IngressDecisionID); ok {
		o.AuthorizationDecision = fmt.Sprintf("%s %s %v (policy %s)", d.Stage, d.Verdict, d.Reasons, d.PolicyVersion)
		o.Environment, o.PolicyVersion, o.PolicyDigest, o.RequestHash = d.Environment, d.PolicyVersion, d.PolicyDigest, d.RequestHash
		o.DecisionRecords = append(o.DecisionRecords, d)
	} else {
		o.AuthorizationDecision = "NO_SUCH_DECISION"
	}
	if d, ok := st.Decision(ctx, "wf_"+outcome.WorkflowID+"_"+outcome.RunID); ok {
		o.RevalidationDecision = fmt.Sprintf("%s %v (policy %s)", d.Verdict, d.Reasons, d.PolicyVersion)
		o.Environment, o.PolicyVersion, o.PolicyDigest = d.Environment, d.PolicyVersion, d.PolicyDigest
		o.DecisionRecords = append(o.DecisionRecords, d)
	} else {
		o.RevalidationDecision = "NOT_RECORDED"
	}
	o.Claim = st.Claim(ctx, req.IngressDecisionID)
	o.ClaimState = o.Claim.State
	o.ClaimOwnedByThisRun = o.Claim.OwnerWorkflowID == outcome.WorkflowID && o.Claim.OwnerRunID == outcome.RunID

	if recs, err := st.Audit.ListByProposal(ctx, prop.ID); err == nil {
		for _, r := range recs {
			if r.WorkflowID == outcome.WorkflowID && r.RunID == outcome.RunID {
				o.AuditTrail = append(o.AuditTrail, AuditEntry{
					Phase: string(r.Phase), Verdict: string(r.Verdict), Detail: r.Detail,
					RecordedAt: r.RecordedAt.UTC().Format(time.RFC3339Nano),
				})
			}
		}
	}
	h, err := st.History(ctx, outcome.WorkflowID, outcome.RunID)
	if err != nil {
		h.Events = append(h.Events, HistoryEvent{Type: "HISTORY_UNREADABLE", FailureCause: err.Error()})
	}
	o.History = h

	o.FenceResult = "NOT_REACHED"
	o.ExecutionState = "NOT_DISPATCHED"
	o.ReconciliationState = "NOT_REQUIRED"
	completed := enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String()
	failed := enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED.String()
	timedOut := enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT.String()
	for _, e := range h.Events {
		switch {
		case e.Activity == "DispatchConfig" && e.Type == completed:
			o.FenceResult = "ACQUIRED (adapter called)"
		case e.Activity == "DispatchConfig" && e.Type == failed:
			switch e.FailureType {
			case contracts.ExecutionFenceRejectedErrorType:
				o.FenceResult = "REJECTED (adapter not called)"
			case contracts.ExecutionFenceUnknownErrorType:
				o.FenceResult = "UNKNOWN (adapter not called)"
			default:
				o.FenceResult = "ACTIVITY_FAILED: " + e.FailureType
			}
		case e.Activity == "DispatchConfig" && e.Type == timedOut:
			o.FenceResult = "ACTIVITY_TIMED_OUT (outcome unknown)"
		case e.ClaimTo == string(contracts.ClaimStateReconciliationRequired):
			o.ReconciliationState = "RECONCILIATION_REQUIRED"
		}
	}
	for _, a := range o.AuditTrail {
		switch a.Phase {
		case string(contracts.AuditPhaseDispatch):
			o.ExecutionState = "DISPATCH_" + a.Verdict
		case string(contracts.AuditPhaseReconciliation):
			o.ReconciliationState = "RECONCILIATION_REQUIRED -> RECONCILED_" + a.Verdict
		}
	}
	o.FinalOutcome = outcome.Status
	if outcome.ErrorType != "" {
		o.FinalOutcome += " " + outcome.ErrorType
	}
	if outcome.Reason != "" {
		o.FinalOutcome += " (" + outcome.Reason + ")"
	}
	return o
}

// WaitGateWaiting waits until n calls are held at the gated target.
func (st *Stack) WaitGateWaiting(ctx context.Context, n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		w, err := st.GateWaiting(ctx)
		if err == nil && w >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gate: %d call(s) waiting after %s, want %d (%v)", w, timeout, n, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
