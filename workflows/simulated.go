package workflows

import (
	"context"
	"fmt"
	"sync"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// SimulatedAdapter is a deterministic, in-memory stand-in for an
// infrastructure target. It changes nothing and contacts nothing. Its
// behaviour depends only on the target ID (see contracts.FailingNodeID and the
// other simulated targets) and on the idempotency keys it has already seen:
//
//   - A key takes effect at most once. Dispatching a key again returns the
//     recorded effect without applying anything.
//   - Reconcile answers from that record. For a key it has never seen, it
//     fences the key, so a late dispatch with it is refused, and reports
//     FAILURE. For contracts.PartitionedNodeID it reports UNKNOWN.
//
// The record lives in process memory: it is lost when the worker restarts and
// is not shared between worker processes. A real adapter must get these
// answers from the target itself.
type SimulatedAdapter struct {
	mu            sync.Mutex
	effects       map[string]contracts.DispatchOutcome
	mutations     map[string]int
	compensations map[string]int
}

// NewSimulatedAdapter returns an adapter that has seen no keys.
func NewSimulatedAdapter() *SimulatedAdapter {
	return &SimulatedAdapter{
		effects:       map[string]contracts.DispatchOutcome{},
		mutations:     map[string]int{},
		compensations: map[string]int{},
	}
}

func (s *SimulatedAdapter) Dispatch(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	key, p := req.IdempotencyKey, req.Proposal
	s.mu.Lock()
	defer s.mu.Unlock()
	if effect, ok := s.effects[key]; ok {
		return effect
	}
	if ctx.Err() != nil {
		return s.fence(key, "simulated: not sent, context ended before dispatch; key fenced")
	}

	fmt.Printf("[simulated dispatch] key=%s target=%s cmd=%s proposal=%s\n", key, p.TargetID, p.Type, p.ID)
	switch p.TargetID {
	case contracts.FailingNodeID:
		return s.apply(key, contracts.DispatchOutcome{
			Status: contracts.OutcomeFailure, PartiallyApplied: true,
			Detail: "simulated: change partly applied, then rejected by target",
		})
	case contracts.LostResponseNodeID:
		s.apply(key, contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, Detail: "simulated: change applied"})
		return contracts.UnknownOutcome("simulated: change applied but the response was lost")
	case contracts.UnreachableNodeID, contracts.PartitionedNodeID:
		return contracts.UnknownOutcome("simulated: no response from target")
	default:
		return s.apply(key, contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, Detail: "simulated: change applied"})
	}
}

func (s *SimulatedAdapter) Reconcile(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	if ctx.Err() != nil {
		return contracts.UnknownOutcome("simulated: reconciliation cancelled")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if effect, ok := s.effects[req.IdempotencyKey]; ok {
		return effect
	}
	if req.Proposal.TargetID == contracts.PartitionedNodeID {
		return contracts.UnknownOutcome("simulated: target not reachable for status queries")
	}
	return s.fence(req.IdempotencyKey, "simulated: target has no record of this key; key fenced so it cannot take effect later")
}

func (s *SimulatedAdapter) Compensate(ctx context.Context, req contracts.DispatchRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("[simulated compensation] key=%s target=%s proposal=%s\n", req.IdempotencyKey, req.Proposal.TargetID, req.Proposal.ID)
	s.compensations[req.IdempotencyKey]++
	return nil
}

// Mutations reports how many times key took effect on the simulated target.
func (s *SimulatedAdapter) Mutations(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mutations[key]
}

// Compensations reports how many times key was compensated.
func (s *SimulatedAdapter) Compensations(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compensations[key]
}

func (s *SimulatedAdapter) apply(key string, effect contracts.DispatchOutcome) contracts.DispatchOutcome {
	s.effects[key] = effect
	s.mutations[key]++
	return effect
}

func (s *SimulatedAdapter) fence(key, detail string) contracts.DispatchOutcome {
	effect := contracts.DispatchOutcome{Status: contracts.OutcomeFailure, Detail: detail}
	s.effects[key] = effect
	return effect
}
