package workflows

import (
	"crypto/rand"
	"errors"
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
// Before any infrastructure activity runs, these steps must succeed in order:
//
//  1. Ingress binding. The decision named by req.IngressDecisionID must be a
//     recorded INGRESS-stage ALLOW for the same agent, the same proposal
//     content (recomputed request hash), the same proposal ID and this
//     workflow ID. Otherwise the rejection is recorded and the workflow fails
//     with a non-retryable INGRESS_DECISION_INVALID error. A workflow started
//     directly in Temporal therefore cannot dispatch without an ALLOW that the
//     proxy recorded for an authenticated agent.
//  2. Execution claim. This run claims the decision (see
//     contracts.ClaimState). If another execution has claimed it, the
//     rejection is recorded and the workflow fails with a non-retryable
//     EXECUTION_CLAIM_REJECTED error. One ingress decision therefore
//     authorizes at most one dispatch.
//  3. Re-validation. The proposal is re-evaluated against the worker's current
//     policy (see DecisionActivities). Anything other than ALLOW is recorded
//     and the workflow fails with a non-retryable REVALIDATION_DENIED error.
//     This covers policy changes between ingress and execution.
//
// Any exit before dispatch is scheduled releases the claim, because nothing
// has been dispatched.
//
// The workflow then schedules DispatchConfig once, with an idempotency key
// derived from the ingress decision (decision.ExecutionKey). The activity
// moves the claim from CLAIMED to EXECUTING (the execution fence) and calls
// the adapter only if that compare-and-set succeeds for this run. If the fence
// is not acquired, the adapter was not called: the workflow releases the claim
// if it is still CLAIMED and fails with EXECUTION_CLAIM_REJECTED; if the fence
// may have committed, the claim can no longer be released and the outcome is
// treated as UNKNOWN. Otherwise the workflow acts on the reported outcome:
//
//   - SUCCESS: the claim moves to COMPLETED.
//   - FAILURE: the claim moves to FAILED. Compensation runs only if the
//     adapter confirmed that part of the change took effect.
//   - UNKNOWN, including any dispatch activity error: the claim moves to
//     RECONCILIATION_REQUIRED and the workflow asks the adapter to reconcile,
//     with the same key, until it confirms SUCCESS or FAILURE or the
//     reconciliation window passes. An unknown outcome is not equivalent to
//     failure, so it never releases the claim, never dispatches again and
//     never compensates. If it is still unknown, the workflow fails with
//     DISPATCH_OUTCOME_UNKNOWN and the claim stays in RECONCILIATION_REQUIRED.
//
// Every phase is written to the audit store.
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

	// auditCtx switches to a disconnected context once dispatch may happen,
	// so that its outcome is recorded even if the workflow is cancelled.
	auditCtx := bookCtx
	record := func(phase contracts.AuditPhase, verdict contracts.AuditVerdict, detail string) error {
		auditSeq++
		rec := newAuditRecord(
			prop.ID, workflowID, runID,
			phase, verdict, prop, detail,
			workflow.Now(ctx), auditSeq,
		)
		return workflow.ExecuteActivity(auditCtx, audit.RecordAuditTrail, rec).Get(auditCtx, nil)
	}

	recordDecision := func(dec contracts.Decision) error {
		rec := contracts.DecisionRecord{
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
		return workflow.ExecuteActivity(bookCtx, decisions.RecordDecision, rec).Get(bookCtx, nil)
	}

	// --- Ingress binding: no infrastructure activity may run before this. ---
	var ingress contracts.IngressVerification
	if err := workflow.ExecuteActivity(bookCtx, decisions.VerifyIngressDecision, req).Get(bookCtx, &ingress); err != nil {
		logger.Error("ingress decision could not be verified; refusing to dispatch", "Error", err)
		return temporal.NewNonRetryableApplicationError(
			"ingress decision could not be verified; proposal not executed",
			contracts.IngressDecisionInvalidErrorType, err)
	}
	// reject records a DENY for this execution and returns a non-retryable
	// error of errType carrying reason.
	reject := func(errType string, reason contracts.ReasonCode, detail string) error {
		rejection := contracts.Decision{Verdict: contracts.VerdictDeny, Reasons: []contracts.ReasonCode{reason}}
		if err := recordDecision(rejection); err != nil {
			logger.Error("failed to record rejection", "Reason", reason, "Error", err)
		}
		auditDetail := fmt.Sprintf("execution rejected: reason=%s detail=%q ingress_decision_id=%q",
			reason, detail, req.IngressDecisionID)
		if err := record(contracts.AuditPhaseRevalidation, contracts.AuditVerdictFail, auditDetail); err != nil {
			logger.Error("failed to audit rejection", "Reason", reason, "Error", err)
		}
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("execution rejected: %s", reason), errType, nil, reason)
	}

	if !ingress.Valid {
		return reject(contracts.IngressDecisionInvalidErrorType, ingress.Reason, ingress.Detail)
	}

	// --- Execution claim: at most one execution per ingress decision. ---
	// The token is generated once and stored in history, so this run presents
	// the same owner after a worker restart, and no other run can present it.
	var token string
	if err := workflow.SideEffect(ctx, func(workflow.Context) interface{} {
		return "exe_" + rand.Text()
	}).Get(&token); err != nil {
		return temporal.NewNonRetryableApplicationError(
			"execution token unavailable; proposal not executed", contracts.ExecutionClaimRejectedErrorType, err)
	}
	transition := func(c workflow.Context, to contracts.ClaimState) error {
		t := contracts.ClaimTransition{DecisionID: req.IngressDecisionID, Token: token, To: to}
		return workflow.ExecuteActivity(c, decisions.AdvanceExecutionClaim, t).Get(c, nil)
	}
	// Claim updates that must happen even if the workflow is cancelled.
	claimCtx, _ := workflow.NewDisconnectedContext(ctx)
	claimCtx = workflow.WithActivityOptions(claimCtx, bookkeepingOpts)

	// Until dispatch is scheduled, nothing can have been dispatched, so every
	// exit returns the claim. Release is a compare-and-set on CLAIMED, so it
	// is a no-op if the claim was never acquired or has already moved on.
	mayHoldClaim, dispatching := false, false
	defer func() {
		if !mayHoldClaim || dispatching {
			return
		}
		if err := transition(claimCtx, contracts.ClaimStateReleased); err != nil {
			logger.Error("failed to release execution claim; decision stays CLAIMED", "Error", err)
		}
	}()

	var claim contracts.ClaimResult
	mayHoldClaim = true
	if err := workflow.ExecuteActivity(bookCtx, decisions.ClaimExecution,
		contracts.ClaimRequest{DecisionID: req.IngressDecisionID, Token: token}).Get(bookCtx, &claim); err != nil {
		logger.Error("execution claim failed; refusing to dispatch", "Error", err)
		return temporal.NewNonRetryableApplicationError(
			"execution claim could not be acquired; proposal not executed",
			contracts.ExecutionClaimRejectedErrorType, err)
	}
	if !claim.Claimed {
		mayHoldClaim = false
		return reject(contracts.ExecutionClaimRejectedErrorType, claim.Reason, claim.Detail)
	}

	// --- Re-validation against the current policy. ---
	var dec contracts.Decision
	if err := workflow.ExecuteActivity(bookCtx, decisions.EvaluateExecution, req).Get(bookCtx, &dec); err != nil {
		logger.Error("re-validation could not run; refusing to dispatch", "Error", err)
		return temporal.NewNonRetryableApplicationError(
			"re-validation unavailable; proposal not executed", contracts.RevalidationDeniedErrorType, err)
	}

	recErr := recordDecision(dec)
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

	// --- Dispatch: scheduled once, never retried. ---
	// The claim is still CLAIMED. DispatchConfig itself moves it to EXECUTING
	// (the execution fence) and calls the adapter only if that succeeds, so
	// the deferred release no longer applies: whether the claim may be given
	// up now depends on whether the fence committed.
	dispatching = true
	auditCtx = claimCtx
	dreq := contracts.DispatchRequest{IdempotencyKey: decision.ExecutionKey(req.IngressDecisionID), AgentID: req.AgentID, Proposal: prop}
	fenced := contracts.FencedDispatch{DecisionID: req.IngressDecisionID, Token: token, Dispatch: dreq}
	var outcome contracts.DispatchOutcome
	var auditErr error
	dispatchErr := workflow.ExecuteActivity(ctx, infra.DispatchConfig, fenced).Get(ctx, &outcome)
	if fence := fenceFailure(dispatchErr); fence != "" {
		// The adapter was not called. Release is a compare-and-set on
		// CLAIMED, so it succeeds only if the fence never committed; then
		// nothing can have been dispatched and the decision may be used again.
		releaseErr := transition(claimCtx, contracts.ClaimStateReleased)
		if releaseErr == nil || fence == contracts.ExecutionFenceRejectedErrorType {
			if releaseErr != nil {
				logger.Info("execution claim not released; it is not held by this run in CLAIMED", "Error", releaseErr)
			}
			_ = record(contracts.AuditPhaseExecutionFence, contracts.AuditVerdictFail,
				fmt.Sprintf("adapter not called: released=%t ingress_decision_id=%s error=%q",
					releaseErr == nil, req.IngressDecisionID, dispatchErr.Error()))
			return temporal.NewNonRetryableApplicationError(
				"execution fence not acquired; proposal not executed", contracts.ExecutionClaimRejectedErrorType, dispatchErr)
		}
		// The fence may have committed, so the claim may be EXECUTING and can
		// no longer be released. Nothing was dispatched, but only the adapter
		// can confirm that the key never took effect, so reconcile.
		logger.Warn("execution fence outcome unknown; adapter was not called", "Error", dispatchErr, "ReleaseError", releaseErr)
		outcome = contracts.UnknownOutcome("execution fence may have committed; adapter not called: " + dispatchErr.Error())
		auditErr = record(contracts.AuditPhaseExecutionFence, contracts.AuditVerdictUnknown, outcomeDetail(dreq, outcome))
	} else {
		if dispatchErr != nil {
			// A timeout, a cancelled wait or a lost worker says nothing about
			// whether the target applied the change.
			outcome = contracts.UnknownOutcome("dispatch activity returned no outcome: " + dispatchErr.Error())
		}
		outcome = outcome.Checked()
		auditErr = record(contracts.AuditPhaseDispatch, auditVerdict(outcome), outcomeDetail(dreq, outcome))
	}

	// --- Unknown outcome: reconcile, never re-dispatch or release. ---
	if outcome.Status == contracts.OutcomeUnknown {
		logger.Warn("dispatch outcome unknown; reconciling", "IdempotencyKey", dreq.IdempotencyKey, "Detail", outcome.Detail)
		if err := transition(claimCtx, contracts.ClaimStateReconciliationRequired); err != nil {
			logger.Error("failed to mark execution claim RECONCILIATION_REQUIRED; it stays EXECUTING", "Error", err)
		}
		outcome = reconcileDispatch(ctx, bookCtx, infra, dreq)
		auditErr = record(contracts.AuditPhaseReconciliation, auditVerdict(outcome), outcomeDetail(dreq, outcome))
		if outcome.Status == contracts.OutcomeUnknown {
			logger.Error("dispatch outcome still unknown; execution claim left in RECONCILIATION_REQUIRED",
				"IdempotencyKey", dreq.IdempotencyKey)
			return temporal.NewNonRetryableApplicationError(
				"dispatch outcome unknown after reconciliation; execution claim left in RECONCILIATION_REQUIRED",
				contracts.DispatchOutcomeUnknownErrorType, nil, dreq.IdempotencyKey)
		}
	}

	// --- Confirmed failure. ---
	if outcome.Status == contracts.OutcomeFailure {
		if err := transition(claimCtx, contracts.ClaimStateFailed); err != nil {
			logger.Error("failed to mark execution claim FAILED", "Error", err)
		}
		failure := "dispatch failed: " + outcome.Detail
		// Compensation needs evidence that the target changed; a FAILURE
		// without PartiallyApplied means nothing took effect. It is attempted
		// once, from a disconnected context, and is not guaranteed to succeed.
		if outcome.PartiallyApplied {
			compCtx := workflow.WithActivityOptions(claimCtx, activityOpts)
			if err := workflow.ExecuteActivity(compCtx, infra.RevertStateCompensation, dreq).Get(compCtx, nil); err != nil {
				_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictFail, err.Error())
				failure = fmt.Sprintf("dispatch partly applied and compensation failed: %v", err)
				_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail, failure)
				return temporal.NewNonRetryableApplicationError(failure, contracts.DispatchFailedErrorType, nil)
			}
			_ = record(contracts.AuditPhaseCompensation, contracts.AuditVerdictPass, "compensation activity completed")
			failure = "dispatch partly applied and compensation ran: " + outcome.Detail
		}
		_ = record(contracts.AuditPhaseWorkflowFailed, contracts.AuditVerdictFail, failure)
		return temporal.NewNonRetryableApplicationError(failure, contracts.DispatchFailedErrorType, nil)
	}

	// --- Confirmed success. ---
	if err := transition(claimCtx, contracts.ClaimStateCompleted); err != nil {
		logger.Error("failed to mark execution claim COMPLETED", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}
	if auditErr != nil {
		logger.Error("audit trail write failed after successful dispatch", "Error", auditErr)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, auditErr)
	}
	if err := record(contracts.AuditPhaseWorkflowComplete, contracts.AuditVerdictPass,
		"workflow completed without compensation"); err != nil {
		logger.Error("audit trail write failed at workflow complete", "Error", err)
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}

	return nil
}

// Reconciliation schedule for an unknown dispatch outcome. The owning run asks
// the adapter at once, then after waits that double from reconcileFirstWait up
// to reconcileMaxWait, until reconcileWindow has passed. Waits are durable
// timers, so a worker restart neither repeats nor resets the schedule.
const (
	reconcileFirstWait = 10 * time.Second
	reconcileMaxWait   = 30 * time.Minute
	reconcileWindow    = 24 * time.Hour
)

// reconcileDispatch asks the adapter for the outcome of dreq until it is
// confirmed, the reconciliation window passes or ctx is cancelled. It never
// dispatches, and it returns UNKNOWN if no outcome was confirmed.
func reconcileDispatch(ctx, actCtx workflow.Context, infra *InfrastructureActivities, dreq contracts.DispatchRequest) contracts.DispatchOutcome {
	start := workflow.Now(ctx)
	wait := reconcileFirstWait
	for {
		var o contracts.DispatchOutcome
		if err := workflow.ExecuteActivity(actCtx, infra.ReconcileDispatch, dreq).Get(actCtx, &o); err != nil {
			o = contracts.UnknownOutcome("reconciliation activity returned no outcome: " + err.Error())
		}
		o = o.Checked()
		if o.Status != contracts.OutcomeUnknown || workflow.Now(ctx).Sub(start)+wait > reconcileWindow {
			return o
		}
		if err := workflow.Sleep(ctx, wait); err != nil {
			return o
		}
		wait = min(2*wait, reconcileMaxWait)
	}
}

// fenceFailure returns the error type of a DispatchConfig error that
// guarantees the adapter was not called, or "" for any other result.
func fenceFailure(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch t := appErr.Type(); t {
		case contracts.ExecutionFenceRejectedErrorType, contracts.ExecutionFenceUnknownErrorType:
			return t
		}
	}
	return ""
}

func auditVerdict(o contracts.DispatchOutcome) contracts.AuditVerdict {
	switch o.Status {
	case contracts.OutcomeSuccess:
		return contracts.AuditVerdictPass
	case contracts.OutcomeFailure:
		return contracts.AuditVerdictFail
	}
	return contracts.AuditVerdictUnknown
}

func outcomeDetail(dreq contracts.DispatchRequest, o contracts.DispatchOutcome) string {
	return fmt.Sprintf("outcome=%s partially_applied=%t idempotency_key=%s target=%s detail=%q",
		o.Status, o.PartiallyApplied, dreq.IdempotencyKey, dreq.Proposal.TargetID, o.Detail)
}

func joinReasons(r []contracts.ReasonCode) string {
	s := make([]string, len(r))
	for i, c := range r {
		s[i] = string(c)
	}
	return strings.Join(s, ",")
}
