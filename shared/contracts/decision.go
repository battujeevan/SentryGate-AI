package contracts

import (
	"errors"
	"slices"
	"time"
)

// Verdict is the outcome of evaluating a proposal against the active policy.
type Verdict string

const (
	VerdictAllow           Verdict = "ALLOW"
	VerdictDeny            Verdict = "DENY"
	VerdictRequireApproval Verdict = "REQUIRE_APPROVAL"
)

// ReasonCode is a stable, machine-readable explanation for a verdict.
// Codes are part of the public API; do not rename them.
type ReasonCode string

const (
	ReasonAgentUnknown        ReasonCode = "AGENT_UNKNOWN"
	ReasonCommandUnknown      ReasonCode = "COMMAND_UNKNOWN"
	ReasonCommandNotPermitted ReasonCode = "COMMAND_NOT_PERMITTED_FOR_AGENT"
	ReasonTargetUnregistered  ReasonCode = "TARGET_UNREGISTERED"
	ReasonTargetProtected     ReasonCode = "TARGET_PROTECTED"
	ReasonEnvironmentDenied   ReasonCode = "ENVIRONMENT_DENIED"
	ReasonApprovalRequired    ReasonCode = "APPROVAL_REQUIRED"
	ReasonEnvironmentAllowed  ReasonCode = "ENVIRONMENT_ALLOWED"
	ReasonPolicyUnavailable   ReasonCode = "POLICY_UNAVAILABLE"
	ReasonRequestHashMismatch ReasonCode = "REQUEST_HASH_MISMATCH"

	// Workflow-stage reasons: the execution is not backed by a valid recorded
	// ingress ALLOW decision.
	ReasonIngressDecisionNotFound   ReasonCode = "INGRESS_DECISION_NOT_FOUND"
	ReasonIngressDecisionWrongStage ReasonCode = "INGRESS_DECISION_WRONG_STAGE"
	ReasonIngressDecisionNotAllow   ReasonCode = "INGRESS_DECISION_NOT_ALLOW"
	ReasonIngressAgentMismatch      ReasonCode = "INGRESS_DECISION_AGENT_MISMATCH"
	ReasonIngressHashMismatch       ReasonCode = "INGRESS_DECISION_HASH_MISMATCH"
	ReasonIngressIdentityMismatch   ReasonCode = "INGRESS_DECISION_IDENTITY_MISMATCH"

	// The ingress decision is valid but another execution has claimed it.
	ReasonIngressDecisionAlreadyClaimed ReasonCode = "INGRESS_DECISION_ALREADY_CLAIMED"
)

// DecisionStage identifies where a decision was made.
type DecisionStage string

const (
	StageIngress              DecisionStage = "INGRESS"
	StageWorkflowRevalidation DecisionStage = "WORKFLOW_REVALIDATION"
)

// Decision is the result of evaluating one proposal against one policy snapshot.
// Reasons lists only the codes that determined the verdict.
type Decision struct {
	Verdict       Verdict      `json:"verdict"`
	Reasons       []ReasonCode `json:"reasons"`
	Environment   string       `json:"environment,omitempty"`
	PolicyVersion string       `json:"policy_version"`
	PolicyDigest  string       `json:"policy_digest"`
}

// DecisionRecord is the persisted evidence of a single decision.
type DecisionRecord struct {
	DecisionID    string        `json:"decision_id"`
	Stage         DecisionStage `json:"stage"`
	ProposalID    string        `json:"proposal_id"`
	AgentID       string        `json:"agent_id"`
	RequestHash   string        `json:"request_hash"`
	Command       CommandType   `json:"command"`
	TargetID      string        `json:"target_id"`
	Environment   string        `json:"environment"`
	Verdict       Verdict       `json:"verdict"`
	Reasons       []ReasonCode  `json:"reasons"`
	PolicyVersion string        `json:"policy_version"`
	PolicyDigest  string        `json:"policy_digest"`
	WorkflowID    string        `json:"workflow_id"`
	TraceID       string        `json:"trace_id"`
	RecordedAt    time.Time     `json:"recorded_at"`
}

// ErrInvalidDecisionRecord is returned when a record is missing required fields.
var ErrInvalidDecisionRecord = errors.New("decision record is missing required fields")

// ErrDecisionConflict is returned when a decision ID is reused with different content.
var ErrDecisionConflict = errors.New("decision record conflicts with an existing record")

// ErrDecisionNotFound is returned when no record exists for a decision ID.
var ErrDecisionNotFound = errors.New("decision record not found")

// Validate checks that the fields every record must carry are present.
func (r DecisionRecord) Validate() error {
	if r.DecisionID == "" || r.Stage == "" || r.ProposalID == "" ||
		r.Verdict == "" || r.RequestHash == "" || r.RecordedAt.IsZero() {
		return ErrInvalidDecisionRecord
	}
	return nil
}

// Equal reports whether two records carry identical content.
func (r DecisionRecord) Equal(o DecisionRecord) bool {
	return r.DecisionID == o.DecisionID &&
		r.Stage == o.Stage &&
		r.ProposalID == o.ProposalID &&
		r.AgentID == o.AgentID &&
		r.RequestHash == o.RequestHash &&
		r.Command == o.Command &&
		r.TargetID == o.TargetID &&
		r.Environment == o.Environment &&
		r.Verdict == o.Verdict &&
		slices.Equal(r.Reasons, o.Reasons) &&
		r.PolicyVersion == o.PolicyVersion &&
		r.PolicyDigest == o.PolicyDigest &&
		r.WorkflowID == o.WorkflowID &&
		r.TraceID == o.TraceID &&
		r.RecordedAt.Equal(o.RecordedAt)
}

// ExecutionRequest is the workflow input. It carries the identity resolved at
// ingress so the workflow can re-evaluate the proposal before any side effect.
type ExecutionRequest struct {
	AgentID           string        `json:"agent_id"`
	Proposal          AgentProposal `json:"proposal"`
	IngressDecisionID string        `json:"ingress_decision_id"`
	RequestHash       string        `json:"request_hash"`
}

// RevalidationDeniedErrorType is the Temporal application error type returned
// when workflow re-validation does not produce ALLOW.
const RevalidationDeniedErrorType = "REVALIDATION_DENIED"

// IngressDecisionInvalidErrorType is the Temporal application error type
// returned when an execution is not backed by a valid recorded ingress ALLOW,
// or when that cannot be verified. When a specific reason is known, the error's
// details carry it as a ReasonCode.
const IngressDecisionInvalidErrorType = "INGRESS_DECISION_INVALID"

// IngressVerification is the result of checking an execution request against
// its recorded ingress decision. The zero value is not valid.
type IngressVerification struct {
	Valid  bool       `json:"valid"`
	Reason ReasonCode `json:"reason,omitempty"`
	Detail string     `json:"detail,omitempty"`
}
