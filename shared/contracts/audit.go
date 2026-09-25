package contracts

import "time"

// AuditPhase identifies a discrete validation or saga stage recorded
// into the immutable audit trail.
type AuditPhase string

const (
	AuditPhaseIngressValidation AuditPhase = "INGRESS_VALIDATION"
	AuditPhaseDispatch          AuditPhase = "DISPATCH_CONFIG"
	AuditPhaseCompensation      AuditPhase = "COMPENSATION_ROLLBACK"
	AuditPhaseWorkflowComplete  AuditPhase = "WORKFLOW_COMPLETE"
	AuditPhaseWorkflowFailed    AuditPhase = "WORKFLOW_FAILED"
)

// AuditVerdict is the deterministic pass/fail outcome of a recorded phase.
type AuditVerdict string

const (
	AuditVerdictPass AuditVerdict = "PASS"
	AuditVerdictFail AuditVerdict = "FAIL"
)

// AuditRecord is an immutable row suitable for enterprise security audits.
// Fields are intentionally flat and JSON-serializable for table storage.
type AuditRecord struct {
	RecordID   string       `json:"record_id"`
	ProposalID string       `json:"proposal_id"`
	WorkflowID string       `json:"workflow_id"`
	RunID      string       `json:"run_id"`
	Phase      AuditPhase   `json:"phase"`
	Verdict    AuditVerdict `json:"verdict"`
	TargetID   string       `json:"target_id"`
	Command    CommandType  `json:"command"`
	Detail     string       `json:"detail"`
	RecordedAt time.Time    `json:"recorded_at"`
}
