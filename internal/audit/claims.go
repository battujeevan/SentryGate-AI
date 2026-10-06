package audit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// ClaimExecution claims decisionID for owner in a single statement. The
// insert succeeds only if no claim exists; the conflict update succeeds only
// if the existing claim is RELEASED. SQLite serializes writers across
// connections and processes, so two concurrent callers cannot both change the
// row. The follow-up read only decides how to report a claim that was not
// acquired.
func (s *Store) ClaimExecution(ctx context.Context, decisionID string, owner contracts.ClaimOwner, at time.Time) error {
	if decisionID == "" || !owner.Valid() {
		return fmt.Errorf("invalid claim for decision %q", decisionID)
	}
	ts := formatTime(at)
	res, err := s.db.ExecContext(ctx, `
INSERT INTO execution_claims(decision_id, workflow_id, run_id, claim_token, state, claimed_at, updated_at)
VALUES (?, ?, ?, ?, 'CLAIMED', ?, ?)
ON CONFLICT(decision_id) DO UPDATE SET
  workflow_id = excluded.workflow_id,
  run_id      = excluded.run_id,
  claim_token = excluded.claim_token,
  state       = 'CLAIMED',
  claimed_at  = excluded.claimed_at,
  updated_at  = excluded.updated_at
WHERE execution_claims.state = 'RELEASED'`,
		decisionID, owner.WorkflowID, owner.RunID, owner.Token, ts, ts)
	if err != nil && strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		return fmt.Errorf("%w: %v", contracts.ErrDecisionNotFound, err)
	}
	if err != nil {
		return fmt.Errorf("claim execution: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("claim execution: %w", err)
	}
	if n == 1 {
		return nil
	}
	c, err := s.GetClaim(ctx, decisionID)
	if err != nil {
		return fmt.Errorf("read existing claim: %w", err)
	}
	if c.Owner == owner && c.State == contracts.ClaimStateClaimed {
		return nil
	}
	return decision.ClaimHeldError(c)
}

// AdvanceClaim moves a claim held by owner to the given state with a single
// compare-and-set UPDATE on owner and current state.
func (s *Store) AdvanceClaim(ctx context.Context, decisionID string, owner contracts.ClaimOwner, to contracts.ClaimState, at time.Time) error {
	from, ok := decision.ClaimSources(to)
	if !ok {
		return fmt.Errorf("%w: invalid target state %q", contracts.ErrClaimNotHeld, to)
	}
	args := []any{string(to), formatTime(at), decisionID, owner.WorkflowID, owner.RunID, owner.Token}
	for _, f := range from {
		args = append(args, string(f))
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE execution_claims SET state = ?, updated_at = ?
WHERE decision_id = ? AND workflow_id = ? AND run_id = ? AND claim_token = ?
  AND state IN (`+strings.TrimSuffix(strings.Repeat("?,", len(from)), ",")+`)`, args...)
	if err != nil {
		return fmt.Errorf("advance claim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("advance claim: %w", err)
	}
	if n != 1 {
		return contracts.ErrClaimNotHeld
	}
	return nil
}

// GetClaim returns the claim on decisionID, or contracts.ErrClaimNotFound.
func (s *Store) GetClaim(ctx context.Context, decisionID string) (contracts.ExecutionClaim, error) {
	var c contracts.ExecutionClaim
	var state, claimed, updated string
	err := s.db.QueryRowContext(ctx, `
SELECT decision_id, workflow_id, run_id, claim_token, state, claimed_at, updated_at
FROM execution_claims WHERE decision_id = ?`, decisionID).Scan(
		&c.DecisionID, &c.Owner.WorkflowID, &c.Owner.RunID, &c.Owner.Token, &state, &claimed, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return contracts.ExecutionClaim{}, contracts.ErrClaimNotFound
	}
	if err != nil {
		return contracts.ExecutionClaim{}, fmt.Errorf("read claim: %w", err)
	}
	c.State = contracts.ClaimState(state)
	if c.ClaimedAt, err = parseTime(claimed); err != nil {
		return contracts.ExecutionClaim{}, err
	}
	if c.UpdatedAt, err = parseTime(updated); err != nil {
		return contracts.ExecutionClaim{}, err
	}
	return c, nil
}
