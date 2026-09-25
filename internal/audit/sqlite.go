package audit

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Store is an append-only SQLite-backed audit table shared by proxy and worker.
type Store struct {
	db *sql.DB
}

// Open creates (if needed) and opens the audit database at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("audit db dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS audit_records (
  record_id   TEXT PRIMARY KEY,
  proposal_id TEXT NOT NULL,
  workflow_id TEXT NOT NULL,
  run_id      TEXT NOT NULL,
  phase       TEXT NOT NULL,
  verdict     TEXT NOT NULL,
  target_id   TEXT NOT NULL,
  command     TEXT NOT NULL,
  detail      TEXT NOT NULL,
  recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_proposal ON audit_records(proposal_id);
`
	_, err := s.db.Exec(ddl)
	return err
}

// Close releases the database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Append inserts an immutable audit row.
func (s *Store) Append(ctx context.Context, record contracts.AuditRecord) error {
	if record.RecordID == "" || record.ProposalID == "" || record.Phase == "" {
		return contracts.ErrAuditPersistFailed
	}
	if record.RecordedAt.IsZero() {
		record.RecordedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO audit_records(
  record_id, proposal_id, workflow_id, run_id, phase, verdict,
  target_id, command, detail, recorded_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.RecordID, record.ProposalID, record.WorkflowID, record.RunID,
		string(record.Phase), string(record.Verdict),
		record.TargetID, string(record.Command), record.Detail,
		record.RecordedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}
	return nil
}

// ListByProposal returns audit rows for a proposal in insertion order.
func (s *Store) ListByProposal(ctx context.Context, proposalID string) ([]contracts.AuditRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT record_id, proposal_id, workflow_id, run_id, phase, verdict,
       target_id, command, detail, recorded_at
FROM audit_records
WHERE proposal_id = ?
ORDER BY recorded_at ASC, record_id ASC`, proposalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []contracts.AuditRecord
	for rows.Next() {
		var r contracts.AuditRecord
		var phase, verdict, command, recorded string
		if err := rows.Scan(
			&r.RecordID, &r.ProposalID, &r.WorkflowID, &r.RunID,
			&phase, &verdict, &r.TargetID, &command, &r.Detail, &recorded,
		); err != nil {
			return nil, err
		}
		r.Phase = contracts.AuditPhase(phase)
		r.Verdict = contracts.AuditVerdict(verdict)
		r.Command = contracts.CommandType(command)
		if t, err := time.Parse(time.RFC3339Nano, recorded); err == nil {
			r.RecordedAt = t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Ping verifies database connectivity for readiness probes.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}
