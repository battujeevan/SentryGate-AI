package agent

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

// Boundary acts on Temporal directly, without going through ingress.
type Boundary struct {
	Temporal  client.Client
	TaskQueue string
}

// Start starts the SentryGate workflow with workflowID and req, as anyone
// with access to the Temporal frontend could.
func (b *Boundary) Start(ctx context.Context, label, workflowID string, req contracts.ExecutionRequest) Request {
	r := Request{
		Label: label, Role: RoleAttack, Path: PathBoundary, Action: "Temporal StartWorkflowExecution",
		AgentID: req.AgentID, Proposal: &req.Proposal, Execution: &req, WorkflowID: workflowID, SentAt: time.Now().UTC(),
	}
	run, err := b.Temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                b.TaskQueue,
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, workflows.SentryGateSagaWorkflow, req)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.RunID = run.GetRunID()
	return r
}

// Terminate terminates the given run.
func (b *Boundary) Terminate(ctx context.Context, label, workflowID, runID string) Request {
	r := Request{Label: label, Role: RoleAttack, Path: PathBoundary, Action: "Temporal TerminateWorkflowExecution",
		WorkflowID: workflowID, RunID: runID, SentAt: time.Now().UTC()}
	if err := b.Temporal.TerminateWorkflow(ctx, workflowID, runID, "validation: "+label); err != nil {
		r.Error = err.Error()
	}
	return r
}

// Reset resets the given run to the workflow task that finished at eventID.
// Temporal terminates the run if it is still open and starts a new run that
// replays history up to that point.
func (b *Boundary) Reset(ctx context.Context, label, workflowID, runID string, eventID int64) Request {
	r := Request{Label: label, Role: RoleAttack, Path: PathBoundary,
		Action:     fmt.Sprintf("Temporal ResetWorkflowExecution (workflow task finished at event %d)", eventID),
		WorkflowID: workflowID, SentAt: time.Now().UTC()}
	resp, err := b.Temporal.ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 "default",
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Reason:                    "validation: " + label,
		WorkflowTaskFinishEventId: eventID,
		RequestId:                 "reset-" + rand.Text(),
		ResetReapplyType:          enumspb.RESET_REAPPLY_TYPE_NONE,
	})
	if err != nil {
		r.Error = err.Error()
		return r
	}
	r.RunID = resp.GetRunId()
	return r
}
