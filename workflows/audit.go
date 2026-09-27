package workflows

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// AuditStore persists workflow AuditRecord rows. Implementations only insert;
// they expose no update or delete operation.
type AuditStore interface {
	Append(ctx context.Context, record contracts.AuditRecord) error
	ListByProposal(ctx context.Context, proposalID string) ([]contracts.AuditRecord, error)
}

// MemoryAuditStore is an in-process append-only audit table for tests.
type MemoryAuditStore struct {
	mu      sync.RWMutex
	records []contracts.AuditRecord
	byProp  map[string][]int
}

// NewMemoryAuditStore constructs an empty in-memory audit table.
func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{
		byProp: make(map[string][]int),
	}
}

// Append writes a new audit row. Existing rows are never mutated or deleted.
func (s *MemoryAuditStore) Append(ctx context.Context, record contracts.AuditRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if record.RecordID == "" || record.ProposalID == "" || record.Phase == "" {
		return contracts.ErrAuditPersistFailed
	}
	if record.RecordedAt.IsZero() {
		record.RecordedAt = time.Now().UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := len(s.records)
	s.records = append(s.records, record)
	s.byProp[record.ProposalID] = append(s.byProp[record.ProposalID], idx)
	return nil
}

// ListByProposal returns a copy of all audit rows for a proposal, in write order.
func (s *MemoryAuditStore) ListByProposal(ctx context.Context, proposalID string) ([]contracts.AuditRecord, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	idxs := s.byProp[proposalID]
	out := make([]contracts.AuditRecord, 0, len(idxs))
	for _, i := range idxs {
		out = append(out, s.records[i])
	}
	return out, nil
}

// All returns a defensive copy of every recorded audit row.
func (s *MemoryAuditStore) All() []contracts.AuditRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]contracts.AuditRecord, len(s.records))
	copy(out, s.records)
	return out
}

// AuditActivities exposes the Temporal activity that writes audit rows.
type AuditActivities struct {
	Store AuditStore
}

// RecordAuditTrail persists a single AuditRecord for one workflow phase.
func (a *AuditActivities) RecordAuditTrail(ctx context.Context, record contracts.AuditRecord) error {
	if a.Store == nil {
		return contracts.ErrAuditPersistFailed
	}
	return a.Store.Append(ctx, record)
}

// newAuditRecord builds an AuditRecord from workflow identity. The record ID
// includes the run ID so a later run for the same proposal cannot collide.
// recordedAt must come from workflow.Now(ctx) so Temporal replay stays deterministic.
func newAuditRecord(
	proposalID, workflowID, runID string,
	phase contracts.AuditPhase,
	verdict contracts.AuditVerdict,
	prop contracts.AgentProposal,
	detail string,
	recordedAt time.Time,
	seq int64,
) contracts.AuditRecord {
	return contracts.AuditRecord{
		RecordID:   fmt.Sprintf("%s-%s-%s-%d", proposalID, runID, phase, seq),
		ProposalID: proposalID,
		WorkflowID: workflowID,
		RunID:      runID,
		Phase:      phase,
		Verdict:    verdict,
		TargetID:   prop.TargetID,
		Command:    prop.Type,
		Detail:     detail,
		RecordedAt: recordedAt.UTC(),
	}
}
