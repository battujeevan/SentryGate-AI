package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/sentrygate-ai/sentrygate/shared/contracts"
)

// SentryGateSagaWorkflow is the durable Temporal state machine that applies
// infrastructure mutations with atomic multi-step compensation on failure.
//
// Every validation and saga phase is recorded via AuditActivities into an
// immutable audit table for enterprise security reviews.
func SentryGateSagaWorkflow(ctx workflow.Context, prop contracts.AgentProposal) error {
	logger := workflow.GetLogger(ctx)
	info := workflow.GetInfo(ctx)
	workflowID := info.WorkflowExecution.ID
	runID := info.WorkflowExecution.RunID

	// Configure deterministic retry constraints to avoid infinite AI help loops.
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        1 * time.Second,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        1, // Force immediate failure loud to kickstart rollback
			NonRetryableErrorTypes: []string{contracts.NonRetryableErrorType},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Audit writes use a slightly longer timeout; failures here must not
	// silently drop compliance evidence when the primary mutation succeeds.
	auditOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    3,
		},
	}
	auditCtx := workflow.WithActivityOptions(ctx, auditOpts)

	var infra *InfrastructureActivities
	var audit *AuditActivities
	var auditSeq int64

	record := func(phase contracts.AuditPhase, verdict contracts.AuditVerdict, detail string) error {
		auditSeq++
		rec := newAuditRecord(
			prop.ID, workflowID, runID,
			phase, verdict, prop, detail,
			workflow.Now(ctx), auditSeq,
		)
		return workflow.ExecuteActivity(auditCtx, audit.RecordAuditTrail, rec).Get(auditCtx, nil)
	}

	// --- State tracker: ingress validation already passed at proxy edge ---
	if err := record(contracts.AuditPhaseIngressValidation, contracts.AuditVerdictPass,
		fmt.Sprintf("proposal accepted for saga dispatch; risk_score=%.2f", prop.RiskScore)); err != nil {
		logger.Error("audit trail write failed at ingress validation", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	// Stage 1: Try executing mutation
	err := workflow.ExecuteActivity(ctx, infra.DispatchConfig, prop).Get(ctx, nil)
	if err != nil {
		logger.Error("Execution pipeline broken. Initiating automated Saga compensation loops.", "Error", err)

		_ = record(contracts.AuditPhaseDispatch, contracts.AuditVerdictFail, err.Error())

		// Guarantee compensation executes using a disconnected context even if
		// the parent workflow is cancelled mid-flight.
		compCtx, _ := workflow.NewDisconnectedContext(ctx)
		compCtx = workflow.WithActivityOptions(compCtx, activityOpts)

		rollbackErr := workflow.ExecuteActivity(compCtx, infra.RevertStateCompensation, prop).Get(compCtx, nil)
		if rollbackErr != nil {
			_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictFail, rollbackErr.Error())
			_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail,
				fmt.Sprintf("nested failure during compensation: %v", rollbackErr))
			return fmt.Errorf("system panic: nested failure during compensation loop: %w", rollbackErr)
		}

		_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictPass,
			"compensation completed; infrastructure restored to last stable state")
		_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail,
			fmt.Sprintf("transaction safely isolated and rolled back: %v", err))

		return fmt.Errorf("transaction safely isolated and rolled back: %w", err)
	}

	if err := record(contracts.AuditPhaseDispatch, contracts.AuditVerdictPass,
		fmt.Sprintf("policy update applied on target %s", prop.TargetID)); err != nil {
		logger.Error("audit trail write failed after successful dispatch", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	if err := record(contracts.AuditPhaseWorkflowComplete, contracts.AuditVerdictPass,
		"saga completed without compensation"); err != nil {
		logger.Error("audit trail write failed at workflow complete", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	return nil
}
