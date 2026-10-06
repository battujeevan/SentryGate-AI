package decision

import (
	"context"
	"errors"
	"fmt"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Ingress verification failures. Each one means the execution is not backed by
// a valid recorded ingress ALLOW and must not dispatch.
var (
	ErrIngressDecisionNotFound = errors.New("ingress decision not found")
	ErrIngressWrongStage       = errors.New("ingress decision has wrong stage")
	ErrIngressNotAllowed       = errors.New("ingress decision verdict is not ALLOW")
	ErrIngressAgentMismatch    = errors.New("ingress decision agent does not match execution agent")
	ErrIngressHashMismatch     = errors.New("ingress decision request hash does not match proposal")
	ErrIngressIdentityMismatch = errors.New("ingress decision identity does not match execution")
)

// VerifyIngress checks that rec is a recorded ingress ALLOW for exactly this
// execution: the same agent, the same proposal content (by recomputed request
// hash), the same proposal ID and the same workflow ID. workflowID must come
// from the workflow runtime, not from req.
func VerifyIngress(rec contracts.DecisionRecord, req contracts.ExecutionRequest, workflowID string) error {
	if rec.DecisionID == "" || rec.DecisionID != req.IngressDecisionID {
		return ErrIngressDecisionNotFound
	}
	if rec.Stage != contracts.StageIngress {
		return fmt.Errorf("%w: got %s", ErrIngressWrongStage, rec.Stage)
	}
	if rec.Verdict != contracts.VerdictAllow {
		return fmt.Errorf("%w: got %s", ErrIngressNotAllowed, rec.Verdict)
	}
	if rec.AgentID != req.AgentID {
		return ErrIngressAgentMismatch
	}
	if rec.RequestHash != RequestHash(req.Proposal) {
		return ErrIngressHashMismatch
	}
	if rec.ProposalID != req.Proposal.ID {
		return fmt.Errorf("%w: proposal ID", ErrIngressIdentityMismatch)
	}
	if rec.WorkflowID == "" || rec.WorkflowID != workflowID {
		return fmt.Errorf("%w: workflow ID", ErrIngressIdentityMismatch)
	}
	return nil
}

// CheckIngress loads the ingress decision referenced by req and verifies it
// with VerifyIngress. Errors that are not one of the ErrIngress* sentinels mean
// the decision could not be loaded; callers must treat them as a failure too.
func CheckIngress(ctx context.Context, s Store, req contracts.ExecutionRequest, workflowID string) error {
	if req.IngressDecisionID == "" {
		return ErrIngressDecisionNotFound
	}
	rec, err := s.GetDecision(ctx, req.IngressDecisionID)
	if errors.Is(err, contracts.ErrDecisionNotFound) {
		return ErrIngressDecisionNotFound
	}
	if err != nil {
		return fmt.Errorf("load ingress decision: %w", err)
	}
	return VerifyIngress(rec, req, workflowID)
}

// IngressReason maps an ingress verification failure to its reason code. It
// reports false for errors that are not verification failures.
func IngressReason(err error) (contracts.ReasonCode, bool) {
	switch {
	case errors.Is(err, ErrIngressDecisionNotFound):
		return contracts.ReasonIngressDecisionNotFound, true
	case errors.Is(err, ErrIngressWrongStage):
		return contracts.ReasonIngressDecisionWrongStage, true
	case errors.Is(err, ErrIngressNotAllowed):
		return contracts.ReasonIngressDecisionNotAllow, true
	case errors.Is(err, ErrIngressAgentMismatch):
		return contracts.ReasonIngressAgentMismatch, true
	case errors.Is(err, ErrIngressHashMismatch):
		return contracts.ReasonIngressHashMismatch, true
	case errors.Is(err, ErrIngressIdentityMismatch):
		return contracts.ReasonIngressIdentityMismatch, true
	}
	return "", false
}
