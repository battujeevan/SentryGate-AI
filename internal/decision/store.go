package decision

import (
	"context"
	"slices"
	"sync"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Store persists decision records. AppendDecision must be idempotent: writing
// an identical record twice succeeds, while reusing a decision ID with
// different content returns contracts.ErrDecisionConflict. GetDecision returns
// contracts.ErrDecisionNotFound when no record has the given ID.
type Store interface {
	AppendDecision(ctx context.Context, rec contracts.DecisionRecord) error
	GetDecision(ctx context.Context, decisionID string) (contracts.DecisionRecord, error)
	ListDecisionsByProposal(ctx context.Context, proposalID string) ([]contracts.DecisionRecord, error)
}

// MemoryStore is an in-process Store and ClaimStore for tests.
type MemoryStore struct {
	mu      sync.Mutex
	records []contracts.DecisionRecord
	byID    map[string]int
	claims  map[string]contracts.ExecutionClaim
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{byID: make(map[string]int), claims: make(map[string]contracts.ExecutionClaim)}
}

func (m *MemoryStore) AppendDecision(ctx context.Context, rec contracts.DecisionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := rec.Validate(); err != nil {
		return err
	}
	rec.Reasons = slices.Clone(rec.Reasons)
	m.mu.Lock()
	defer m.mu.Unlock()
	if i, ok := m.byID[rec.DecisionID]; ok {
		if m.records[i].Equal(rec) {
			return nil
		}
		return contracts.ErrDecisionConflict
	}
	m.byID[rec.DecisionID] = len(m.records)
	m.records = append(m.records, rec)
	return nil
}

func (m *MemoryStore) GetDecision(ctx context.Context, decisionID string) (contracts.DecisionRecord, error) {
	if err := ctx.Err(); err != nil {
		return contracts.DecisionRecord{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	i, ok := m.byID[decisionID]
	if !ok {
		return contracts.DecisionRecord{}, contracts.ErrDecisionNotFound
	}
	r := m.records[i]
	r.Reasons = slices.Clone(r.Reasons)
	return r, nil
}

func (m *MemoryStore) ListDecisionsByProposal(ctx context.Context, proposalID string) ([]contracts.DecisionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []contracts.DecisionRecord{}
	for _, r := range m.records {
		if r.ProposalID == proposalID {
			r.Reasons = slices.Clone(r.Reasons)
			out = append(out, r)
		}
	}
	return out, nil
}

// All returns a copy of every record in write order.
func (m *MemoryStore) All() []contracts.DecisionRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]contracts.DecisionRecord, len(m.records))
	for i, r := range m.records {
		r.Reasons = slices.Clone(r.Reasons)
		out[i] = r
	}
	return out
}
