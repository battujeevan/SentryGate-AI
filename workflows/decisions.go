package workflows

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// DecisionActivities re-evaluates proposals inside the workflow and records
// the result. Policy is read here, in an activity, so the workflow itself stays
// deterministic: the evaluated decision is stored in workflow history.
type DecisionActivities struct {
	Policy policy.Source
	Store  decision.Store
	Claims decision.ClaimStore
}

// EvaluateExecution re-evaluates an execution request against the policy that
// is active on the worker now. A missing policy yields DENY, never an error.
func (a *DecisionActivities) EvaluateExecution(ctx context.Context, req contracts.ExecutionRequest) (contracts.Decision, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Decision{}, err
	}
	var snap *policy.Snapshot
	if a.Policy != nil {
		snap = a.Policy.Current()
	}
	return decision.Revalidate(snap, req), nil
}

// VerifyIngressDecision checks that req is backed by a recorded ingress ALLOW
// for this agent, proposal content, proposal ID and workflow. The workflow ID
// comes from the activity's own execution info, never from req. A verification
// failure is returned as an invalid result with its reason; a store failure is
// returned as an error. Both mean the workflow must not dispatch.
func (a *DecisionActivities) VerifyIngressDecision(ctx context.Context, req contracts.ExecutionRequest) (contracts.IngressVerification, error) {
	if a.Store == nil {
		return contracts.IngressVerification{}, errors.New("decision store not configured")
	}
	workflowID := activity.GetInfo(ctx).WorkflowExecution.ID
	err := decision.CheckIngress(ctx, a.Store, req, workflowID)
	if err == nil {
		return contracts.IngressVerification{Valid: true}, nil
	}
	if reason, ok := decision.IngressReason(err); ok {
		return contracts.IngressVerification{Reason: reason, Detail: err.Error()}, nil
	}
	return contracts.IngressVerification{}, err
}

// claimOwner identifies the calling workflow run. Workflow and run IDs come
// from the activity's execution info, never from the workflow's input.
func claimOwner(ctx context.Context, token string) contracts.ClaimOwner {
	exec := activity.GetInfo(ctx).WorkflowExecution
	return contracts.ClaimOwner{WorkflowID: exec.ID, RunID: exec.RunID, Token: token}
}

// ClaimExecution claims the ingress decision for the calling workflow run. A
// decision claimed by another execution is returned as an unclaimed result
// with ReasonIngressDecisionAlreadyClaimed; a store failure is returned as an
// error. Both mean the workflow must not dispatch.
func (a *DecisionActivities) ClaimExecution(ctx context.Context, req contracts.ClaimRequest) (contracts.ClaimResult, error) {
	if a.Claims == nil {
		return contracts.ClaimResult{}, errors.New("execution claim store not configured")
	}
	if req.DecisionID == "" || req.Token == "" {
		return contracts.ClaimResult{}, temporal.NewNonRetryableApplicationError(
			"claim request is missing the decision ID or token", contracts.ExecutionClaimRejectedErrorType, nil)
	}
	err := a.Claims.ClaimExecution(ctx, req.DecisionID, claimOwner(ctx, req.Token), time.Now())
	if err == nil {
		return contracts.ClaimResult{Claimed: true}, nil
	}
	if errors.Is(err, contracts.ErrClaimHeld) {
		return contracts.ClaimResult{Reason: contracts.ReasonIngressDecisionAlreadyClaimed, Detail: err.Error()}, nil
	}
	return contracts.ClaimResult{}, err
}

// AdvanceExecutionClaim moves the calling run's claim to t.To. A claim that is
// not held by this run in a permitting state fails without retry. It never
// moves a claim to EXECUTING: only InfrastructureActivities.DispatchConfig
// acquires the execution fence, immediately before it calls the adapter.
func (a *DecisionActivities) AdvanceExecutionClaim(ctx context.Context, t contracts.ClaimTransition) error {
	if a.Claims == nil {
		return errors.New("execution claim store not configured")
	}
	if t.To == contracts.ClaimStateExecuting {
		return temporal.NewNonRetryableApplicationError(
			"EXECUTING is entered only by the dispatch activity", contracts.ExecutionClaimRejectedErrorType, nil)
	}
	err := a.Claims.AdvanceClaim(ctx, t.DecisionID, claimOwner(ctx, t.Token), t.To, time.Now())
	if errors.Is(err, contracts.ErrClaimNotHeld) {
		return temporal.NewNonRetryableApplicationError(err.Error(), contracts.ExecutionClaimRejectedErrorType, err)
	}
	return err
}

// RecordDecision persists a workflow re-validation decision. It is idempotent
// for activity retries because the record ID and content are deterministic.
func (a *DecisionActivities) RecordDecision(ctx context.Context, rec contracts.DecisionRecord) error {
	if a.Store == nil {
		return errors.New("decision store not configured")
	}
	return a.Store.AppendDecision(ctx, rec)
}
