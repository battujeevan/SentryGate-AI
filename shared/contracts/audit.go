package contracts

import "time"

// AuditPhase identifies a workflow stage recorded in the audit trail.
type AuditPhase string

const (
	AuditPhaseRevalidation     AuditPhase = "WORKFLOW_REVALIDATION"
	AuditPhaseDispatch         AuditPhase = "DISPATCH_CONFIG"
	AuditPhaseCompensation     AuditPhase = "COMPENSATION_ROLLBACK"
	AuditPhaseWorkflowComplete AuditPhase = "WORKFLOW_COMPLETE"
	AuditPhaseWorkflowFailed   AuditPhase = "WORKFLOW_FAILED"
)

// AuditVerdict is the pass/fail outcome of a recorded phase.
type AuditVerdict string

const (
	AuditVerdictPass AuditVerdict = "PASS"
	AuditVerdictFail AuditVerdict = "FAIL"
)

// AuditRecord is an append-only row describing one workflow phase.
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
