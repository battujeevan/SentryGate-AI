package decision

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// ClaimStore enforces that an ingress decision authorizes at most one
// execution. Implementations must make each operation atomic with respect to
// concurrent callers, including callers in other processes.
//
// ClaimExecution claims an unclaimed or RELEASED decision for owner. A repeat
// call by the same owner while the claim is CLAIMED succeeds. If another owner
// holds the claim, or the claim has reached EXECUTING or later, it returns
// contracts.ErrClaimHeld.
//
// AdvanceClaim moves a claim held by owner to the given state if the current
// state is listed by ClaimSources; otherwise it returns
// contracts.ErrClaimNotHeld.
type ClaimStore interface {
	ClaimExecution(ctx context.Context, decisionID string, owner contracts.ClaimOwner, at time.Time) error
	AdvanceClaim(ctx context.Context, decisionID string, owner contracts.ClaimOwner, to contracts.ClaimState, at time.Time) error
	GetClaim(ctx context.Context, decisionID string) (contracts.ExecutionClaim, error)
}

// ClaimSources lists the states from which an owner may move a claim to the
// given state. CLAIMED is entered only through ClaimExecution.
//
// EXECUTING is the execution fence and can be entered only from CLAIMED, so
// exactly one attempt can ever acquire it; a repeat by the same owner fails.
// Every other list includes the target state itself so that a retried
// transition is idempotent. Nothing leads from EXECUTING or later to RELEASED:
// once the adapter may have been called, the claim is never given up.
func ClaimSources(to contracts.ClaimState) ([]contracts.ClaimState, bool) {
	switch to {
	case contracts.ClaimStateExecuting:
		return []contracts.ClaimState{contracts.ClaimStateClaimed}, true
	case contracts.ClaimStateReconciliationRequired:
		return []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired}, true
	case contracts.ClaimStateCompleted:
		return []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired, contracts.ClaimStateCompleted}, true
	case contracts.ClaimStateFailed:
		return []contracts.ClaimState{contracts.ClaimStateExecuting, contracts.ClaimStateReconciliationRequired, contracts.ClaimStateFailed}, true
	case contracts.ClaimStateReleased:
		return []contracts.ClaimState{contracts.ClaimStateClaimed, contracts.ClaimStateReleased}, true
	}
	return nil, false
}

// ExecutionKey is the idempotency key for the single infrastructure execution
// that ingress decision decisionID authorizes. It depends only on the decision
// ID, never on the workflow run or claim token, so every attempt, activity
// retry, reconciliation, worker restart and Temporal reset of that execution
// presents the same key, and a target that honours idempotency keys applies
// the mutation at most once even if it receives it twice.
func ExecutionKey(decisionID string) string {
	return "sentrygate.exec.v1:" + decisionID
}

// ClaimHeldError describes the claim that blocked a ClaimExecution call. It
// wraps contracts.ErrClaimHeld.
func ClaimHeldError(c contracts.ExecutionClaim) error {
	return fmt.Errorf("%w: state=%s workflow_id=%s run_id=%s",
		contracts.ErrClaimHeld, c.State, c.Owner.WorkflowID, c.Owner.RunID)
}

func (m *MemoryStore) ClaimExecution(ctx context.Context, decisionID string, owner contracts.ClaimOwner, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if decisionID == "" || !owner.Valid() {
		return fmt.Errorf("invalid claim for decision %q", decisionID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.byID[decisionID]; !ok {
		return contracts.ErrDecisionNotFound
	}
	c, ok := m.claims[decisionID]
	switch {
	case !ok || c.State == contracts.ClaimStateReleased:
		m.claims[decisionID] = contracts.ExecutionClaim{
			DecisionID: decisionID, Owner: owner, State: contracts.ClaimStateClaimed,
			ClaimedAt: at.UTC(), UpdatedAt: at.UTC(),
		}
		return nil
	case c.Owner == owner && c.State == contracts.ClaimStateClaimed:
		return nil
	default:
		return ClaimHeldError(c)
	}
}

func (m *MemoryStore) AdvanceClaim(ctx context.Context, decisionID string, owner contracts.ClaimOwner, to contracts.ClaimState, at time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	from, ok := ClaimSources(to)
	if !ok {
		return fmt.Errorf("%w: invalid target state %q", contracts.ErrClaimNotHeld, to)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.claims[decisionID]
	if !ok || c.Owner != owner || !slices.Contains(from, c.State) {
		return contracts.ErrClaimNotHeld
	}
	c.State = to
	c.UpdatedAt = at.UTC()
	m.claims[decisionID] = c
	return nil
}

func (m *MemoryStore) GetClaim(ctx context.Context, decisionID string) (contracts.ExecutionClaim, error) {
	if err := ctx.Err(); err != nil {
		return contracts.ExecutionClaim{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.claims[decisionID]
	if !ok {
		return contracts.ExecutionClaim{}, contracts.ErrClaimNotFound
	}
	return c, nil
}
