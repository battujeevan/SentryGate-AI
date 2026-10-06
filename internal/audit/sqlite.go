package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// busyTimeoutMillis bounds how long a writer waits for a lock held by the
// other process (proxy and worker share the database file).
const busyTimeoutMillis = 5000

// Store is a SQLite-backed store for decision records, workflow audit records
// and execution claims, shared by the proxy and the worker. Decision and audit
// rows are only ever inserted. Claim rows change state only through the
// conditional statements in claims.go. Nothing prevents someone with file
// access from modifying the database.
type Store struct {
	db *sql.DB
}

// Open creates (if needed) and opens the database at path in WAL mode.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("audit db dir: %w", err)
	}
	dsn := fmt.Sprintf("%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", path, busyTimeoutMillis)
	db, err := sql.Open("sqlite", dsn)
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

CREATE TABLE IF NOT EXISTS decision_records (
  decision_id    TEXT PRIMARY KEY,
  stage          TEXT NOT NULL,
  proposal_id    TEXT NOT NULL,
  agent_id       TEXT NOT NULL,
  request_hash   TEXT NOT NULL,
  command        TEXT NOT NULL,
  target_id      TEXT NOT NULL,
  environment    TEXT NOT NULL,
  verdict        TEXT NOT NULL,
  reasons        TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  policy_digest  TEXT NOT NULL,
  workflow_id    TEXT NOT NULL,
  trace_id       TEXT NOT NULL,
  recorded_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decision_proposal ON decision_records(proposal_id);

CREATE TABLE IF NOT EXISTS execution_claims (
  decision_id TEXT PRIMARY KEY REFERENCES decision_records(decision_id),
  workflow_id TEXT NOT NULL,
  run_id      TEXT NOT NULL,
  claim_token TEXT NOT NULL,
  state       TEXT NOT NULL CHECK (state IN ('CLAIMED', 'EXECUTING', 'RECONCILIATION_REQUIRED', 'COMPLETED', 'FAILED', 'RELEASED')),
  claimed_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
);
`
	_, err := s.db.Exec(ddl)
	return err
}

// Close releases the database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Append inserts a workflow audit row. Re-inserting an identical row (for
// example on an activity retry) succeeds; reusing a record ID with different
// content fails.
func (s *Store) Append(ctx context.Context, record contracts.AuditRecord) error {
	if record.RecordID == "" || record.ProposalID == "" || record.Phase == "" {
		return contracts.ErrAuditPersistFailed
	}
	if record.RecordedAt.IsZero() {
		record.RecordedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO audit_records(
  record_id, proposal_id, workflow_id, run_id, phase, verdict,
  target_id, command, detail, recorded_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(record_id) DO NOTHING`,
		record.RecordID, record.ProposalID, record.WorkflowID, record.RunID,
		string(record.Phase), string(record.Verdict),
		record.TargetID, string(record.Command), record.Detail,
		formatTime(record.RecordedAt),
	)
	if err != nil {
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}
	existing, err := s.auditByID(ctx, record.RecordID)
	if err != nil {
		return fmt.Errorf("%w: %v", contracts.ErrAuditPersistFailed, err)
	}
	sameTime := existing.RecordedAt.Equal(record.RecordedAt)
	existing.RecordedAt, record.RecordedAt = time.Time{}, time.Time{}
	if !sameTime || existing != record {
		return fmt.Errorf("%w: record %s already exists with different content", contracts.ErrAuditPersistFailed, record.RecordID)
	}
	return nil
}

const auditColumns = `record_id, proposal_id, workflow_id, run_id, phase, verdict,
       target_id, command, detail, recorded_at`

// ListByProposal returns audit rows for a proposal in insertion order.
func (s *Store) ListByProposal(ctx context.Context, proposalID string) ([]contracts.AuditRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+auditColumns+`
FROM audit_records
WHERE proposal_id = ?
ORDER BY recorded_at ASC, record_id ASC`, proposalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []contracts.AuditRecord{}
	for rows.Next() {
		r, err := scanAudit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) auditByID(ctx context.Context, id string) (contracts.AuditRecord, error) {
	return scanAudit(s.db.QueryRowContext(ctx, `SELECT `+auditColumns+` FROM audit_records WHERE record_id = ?`, id))
}

type scanner interface {
	Scan(dest ...any) error
}

func scanAudit(sc scanner) (contracts.AuditRecord, error) {
	var r contracts.AuditRecord
	var phase, verdict, command, recorded string
	if err := sc.Scan(
		&r.RecordID, &r.ProposalID, &r.WorkflowID, &r.RunID,
		&phase, &verdict, &r.TargetID, &command, &r.Detail, &recorded,
	); err != nil {
		return contracts.AuditRecord{}, err
	}
	r.Phase = contracts.AuditPhase(phase)
	r.Verdict = contracts.AuditVerdict(verdict)
	r.Command = contracts.CommandType(command)
	t, err := parseTime(recorded)
	if err != nil {
		return contracts.AuditRecord{}, err
	}
	r.RecordedAt = t
	return r, nil
}

// AppendDecision inserts a decision record. It is idempotent: an identical
// record already present is accepted, and a different record with the same
// decision ID returns contracts.ErrDecisionConflict.
func (s *Store) AppendDecision(ctx context.Context, rec contracts.DecisionRecord) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	reasons, err := json.Marshal(rec.Reasons)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO decision_records(
  decision_id, stage, proposal_id, agent_id, request_hash, command, target_id,
  environment, verdict, reasons, policy_version, policy_digest, workflow_id,
  trace_id, recorded_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(decision_id) DO NOTHING`,
		rec.DecisionID, string(rec.Stage), rec.ProposalID, rec.AgentID, rec.RequestHash,
		string(rec.Command), rec.TargetID, rec.Environment, string(rec.Verdict), string(reasons),
		rec.PolicyVersion, rec.PolicyDigest, rec.WorkflowID, rec.TraceID, formatTime(rec.RecordedAt),
	)
	if err != nil {
		return fmt.Errorf("insert decision: %w", err)
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}
	existing, err := scanDecision(s.db.QueryRowContext(ctx,
		`SELECT `+decisionColumns+` FROM decision_records WHERE decision_id = ?`, rec.DecisionID))
	if err != nil {
		return fmt.Errorf("read existing decision: %w", err)
	}
	if !existing.Equal(rec) {
		return contracts.ErrDecisionConflict
	}
	return nil
}

const decisionColumns = `decision_id, stage, proposal_id, agent_id, request_hash, command,
       target_id, environment, verdict, reasons, policy_version, policy_digest,
       workflow_id, trace_id, recorded_at`

// GetDecision returns the decision record with the given ID, or
// contracts.ErrDecisionNotFound.
func (s *Store) GetDecision(ctx context.Context, decisionID string) (contracts.DecisionRecord, error) {
	r, err := scanDecision(s.db.QueryRowContext(ctx,
		`SELECT `+decisionColumns+` FROM decision_records WHERE decision_id = ?`, decisionID))
	if errors.Is(err, sql.ErrNoRows) {
		return contracts.DecisionRecord{}, contracts.ErrDecisionNotFound
	}
	if err != nil {
		return contracts.DecisionRecord{}, fmt.Errorf("read decision: %w", err)
	}
	return r, nil
}

// ListDecisionsByProposal returns every decision recorded for a proposal in
// insertion order.
func (s *Store) ListDecisionsByProposal(ctx context.Context, proposalID string) ([]contracts.DecisionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+decisionColumns+`
FROM decision_records
WHERE proposal_id = ?
ORDER BY rowid ASC`, proposalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []contracts.DecisionRecord{}
	for rows.Next() {
		r, err := scanDecision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanDecision(sc scanner) (contracts.DecisionRecord, error) {
	var r contracts.DecisionRecord
	var stage, command, verdict, reasons, recorded string
	if err := sc.Scan(
		&r.DecisionID, &stage, &r.ProposalID, &r.AgentID, &r.RequestHash, &command,
		&r.TargetID, &r.Environment, &verdict, &reasons, &r.PolicyVersion, &r.PolicyDigest,
		&r.WorkflowID, &r.TraceID, &recorded,
	); err != nil {
		return contracts.DecisionRecord{}, err
	}
	r.Stage = contracts.DecisionStage(stage)
	r.Command = contracts.CommandType(command)
	r.Verdict = contracts.Verdict(verdict)
	if err := json.Unmarshal([]byte(reasons), &r.Reasons); err != nil {
		return contracts.DecisionRecord{}, fmt.Errorf("decode reasons: %w", err)
	}
	t, err := parseTime(recorded)
	if err != nil {
		return contracts.DecisionRecord{}, err
	}
	r.RecordedAt = t
	return r, nil
}

// Ping verifies database connectivity for readiness probes.
func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errors.New("stored timestamp is not RFC 3339")
	}
	return t, nil
}
