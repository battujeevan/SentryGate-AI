package workflows

import (
	"context"
	"errors"

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

// RecordDecision persists a workflow re-validation decision. It is idempotent
// for activity retries because the record ID and content are deterministic.
func (a *DecisionActivities) RecordDecision(ctx context.Context, rec contracts.DecisionRecord) error {
	if a.Store == nil {
		return errors.New("decision store not configured")
	}
	return a.Store.AppendDecision(ctx, rec)
}
