package workflows

import (
	"context"
	"fmt"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// InfrastructureActivities is a simulated infrastructure adapter. It performs
// no real changes: dispatch prints the proposal and succeeds, except for
// FAILING_NODE, which fails so the compensation path can be exercised.
type InfrastructureActivities struct{}

// DispatchConfig simulates applying a proposal. It returns
// NonRetryableInfraError for FAILING_NODE.
func (a *InfrastructureActivities) DispatchConfig(ctx context.Context, prop contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("[simulated dispatch] target=%s cmd=%s proposal=%s\n", prop.TargetID, prop.Type, prop.ID)

	if prop.TargetID == contracts.FailingNodeID {
		return contracts.NonRetryableInfraError
	}
	return nil
}

// RevertStateCompensation simulates a compensating action. It restores no
// real state.
func (a *InfrastructureActivities) RevertStateCompensation(ctx context.Context, prop contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("[simulated compensation] proposal=%s target=%s\n", prop.ID, prop.TargetID)
	return nil
}
