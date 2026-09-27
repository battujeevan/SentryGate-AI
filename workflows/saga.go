package workflows

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// SentryGateSagaWorkflow executes one proposal that was allowed at ingress.
//
// Its first step re-evaluates the proposal against the worker's current policy
// (see DecisionActivities). Anything other than ALLOW is recorded and the
// workflow fails with a non-retryable REVALIDATION_DENIED error before any
// infrastructure activity runs. This covers direct submissions to Temporal
// and policy changes between ingress and execution. The agent ID in the input
// is not authenticated here: anyone who can start workflows on the task queue
// can claim any agent ID, so Temporal is part of the trusted computing base.
//
// After an ALLOW, the workflow dispatches the (simulated) change. If dispatch
// fails it runs a best-effort compensation activity; neither step is atomic or
// idempotent. Every phase is written to the audit store.
func SentryGateSagaWorkflow(ctx workflow.Context, req contracts.ExecutionRequest) error {
	logger := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID
	prop := req.Proposal

	// Dispatch and compensation run at most once because MaximumAttempts is 1.
	// The NonRetryableErrorTypes entry is inert: Temporal matches it against the
	// error's Go type name, and contracts.NonRetryableInfraError (errors.New) never
	// carries that type. Raising MaximumAttempts would retry infrastructure faults.
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        1 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        1,
			NonRetryableErrorTypes: []string{contracts.NonRetryableErrorType},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Re-validation and record writes are side-effect free or idempotent, so
	// they may retry.
	bookkeepingOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	}
	bookCtx := workflow.WithActivityOptions(ctx, bookkeepingOpts)

	var infra *InfrastructureActivities
	var audit *AuditActivities
	var decisions *DecisionActivities
	var auditSeq int64

	record := func(phase contracts.AuditPhase, verdict contracts.AuditVerdict, detail string) error {
		auditSeq++
		rec := newAuditRecord(
			prop.ID, workflowID, runID,
			phase, verdict, prop, detail,
			workflow.Now(ctx), auditSeq,
		)
		return workflow.ExecuteActivity(bookCtx, audit.RecordAuditTrail, rec).Get(bookCtx, nil)
	}

	// --- Re-validation: no infrastructure activity may run before this. ---
	var dec contracts.Decision
	if err := workflow.ExecuteActivity(bookCtx, decisions.EvaluateExecution, req).Get(bookCtx, &dec); err != nil {
		logger.Error("re-validation could not run; refusing to dispatch", "Error", err)
		return temporal.NewNonRetryableApplicationError(
			"re-validation unavailable; proposal not executed", contracts.RevalidationDeniedErrorType, err)
	}

	decRec := contracts.DecisionRecord{
		DecisionID:    "wf_" + workflowID + "_" + runID,
		Stage:         contracts.StageWorkflowRevalidation,
		ProposalID:    prop.ID,
		AgentID:       req.AgentID,
		RequestHash:   decision.RequestHash(prop),
		Command:       prop.Type,
		TargetID:      prop.TargetID,
		Environment:   dec.Environment,
		Verdict:       dec.Verdict,
		Reasons:       dec.Reasons,
		PolicyVersion: dec.PolicyVersion,
		PolicyDigest:  dec.PolicyDigest,
		WorkflowID:    workflowID,
		RecordedAt:    workflow.Now(ctx).UTC(),
	}
	recErr := workflow.ExecuteActivity(bookCtx, decisions.RecordDecision, decRec).Get(bookCtx, nil)
	detail := fmt.Sprintf("verdict=%s reasons=%s policy_version=%s ingress_decision_id=%s",
		dec.Verdict, joinReasons(dec.Reasons), dec.PolicyVersion, req.IngressDecisionID)

	if dec.Verdict != contracts.VerdictAllow {
		if recErr != nil {
			logger.Error("failed to record re-validation denial", "Error", recErr)
		}
		if err := record(contracts.AuditPhaseRevalidation, contracts.AuditVerdictFail, detail); err != nil {
			logger.Error("failed to audit re-validation denial", "Error", err)
		}
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("proposal denied at workflow re-validation: %s %s", dec.Verdict, joinReasons(dec.Reasons)),
			contracts.RevalidationDeniedErrorType, nil)
	}
	if recErr != nil {
		logger.Error("failed to record re-validation decision; refusing to dispatch", "Error", recErr)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, recErr)
	}
	if err := record(contracts.AuditPhaseRevalidation, contracts.AuditVerdictPass, detail); err != nil {
		logger.Error("audit trail write failed at re-validation", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	// --- Dispatch (simulated adapter). ---
	err := workflow.ExecuteActivity(ctx, infra.DispatchConfig, prop).Get(ctx, nil)
	if err != nil {
		logger.Error("dispatch failed; running compensation", "Error", err)

		_ = record(contracts.AuditPhaseDispatch, contracts.AuditVerdictFail, err.Error())

		// A disconnected context lets compensation start even if the workflow
		// was cancelled. It is attempted once and is not guaranteed to succeed.
		compCtx, _ := workflow.NewDisconnectedContext(ctx)
		compCtx = workflow.WithActivityOptions(compCtx, activityOpts)

		rollbackErr := workflow.ExecuteActivity(compCtx, infra.RevertStateCompensation, prop).Get(compCtx, nil)
		if rollbackErr != nil {
			_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictFail, rollbackErr.Error())
			_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail,
				fmt.Sprintf("compensation failed: %v", rollbackErr))
			return fmt.Errorf("compensation failed after dispatch error: %w", rollbackErr)
		}

		_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictPass,
			"compensation activity completed (simulated)")
		_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail,
			fmt.Sprintf("dispatch failed and compensation ran: %v", err))

		return fmt.Errorf("dispatch failed and compensation ran: %w", err)
	}

	if err := record(contracts.AuditPhaseDispatch, contracts.AuditVerdictPass,
		fmt.Sprintf("simulated dispatch succeeded on target %s", prop.TargetID)); err != nil {
		logger.Error("audit trail write failed after successful dispatch", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	if err := record(contracts.AuditPhaseWorkflowComplete, contracts.AuditVerdictPass,
		"workflow completed without compensation"); err != nil {
		logger.Error("audit trail write failed at workflow complete", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	return nil
}

func joinReasons(r []contracts.ReasonCode) string {
	s := make([]string, len(r))
	for i, c := range r {
		s[i] = string(c)
	}
	return strings.Join(s, ",")
}
