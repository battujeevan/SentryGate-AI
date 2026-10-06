package contracts

import "time"

// AuditPhase identifies a workflow stage recorded in the audit trail.
type AuditPhase string

const (
	AuditPhaseRevalidation AuditPhase = "WORKFLOW_REVALIDATION"
	// AuditPhaseExecutionFence is recorded only when the dispatch activity did
	// not acquire the execution fence, and therefore did not call the adapter.
	AuditPhaseExecutionFence   AuditPhase = "EXECUTION_FENCE"
	AuditPhaseDispatch         AuditPhase = "DISPATCH_CONFIG"
	AuditPhaseReconciliation   AuditPhase = "DISPATCH_RECONCILIATION"
	AuditPhaseCompensation     AuditPhase = "COMPENSATION_ROLLBACK"
	AuditPhaseWorkflowComplete AuditPhase = "WORKFLOW_COMPLETE"
	AuditPhaseWorkflowFailed   AuditPhase = "WORKFLOW_FAILED"
)

// AuditVerdict is the outcome of a recorded phase. UNKNOWN is used only for
// fence, dispatch and reconciliation phases whose outcome is not known; it is
// not a kind of FAIL.
type AuditVerdict string

const (
	AuditVerdictPass    AuditVerdict = "PASS"
	AuditVerdictFail    AuditVerdict = "FAIL"
	AuditVerdictUnknown AuditVerdict = "UNKNOWN"
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
