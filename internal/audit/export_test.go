package audit

import "context"

// Pragmas reports the connection's journal mode and busy timeout (test only).
func (s *Store) Pragmas(ctx context.Context) (journalMode string, busyTimeout int, err error) {
	if err = s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
		return "", 0, err
	}
	err = s.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout)
	return journalMode, busyTimeout, err
}
