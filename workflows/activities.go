package workflows

import (
	"context"
	"fmt"

	"github.com/sentrygate-ai/sentrygate/shared/contracts"
)

// InfrastructureActivities encapsulates side-effecting infrastructure mutations
// and their compensating rollbacks for the SentryGate saga.
type InfrastructureActivities struct{}

// DispatchConfig applies a validated AgentProposal to the target edge node.
// Returns NonRetryableInfraError for structural faults that must fail loud.
func (a *InfrastructureActivities) DispatchConfig(ctx context.Context, prop contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("[Activity] Applying policy update on edge node: %s (cmd=%s)\n", prop.TargetID, prop.Type)

	if prop.TargetID == contracts.FailingNodeID {
		return contracts.NonRetryableInfraError
	}
	return nil
}

// RevertStateCompensation rolls infrastructure back to its last known stable state.
func (a *InfrastructureActivities) RevertStateCompensation(ctx context.Context, prop contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("[CRITICAL ROLLBACK] Compensating transaction %s. Reverting %s to stable state.\n", prop.ID, prop.TargetID)
	return nil
}
