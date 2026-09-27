package policy

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Load reads and strictly validates the policy file at path.
func Load(path string) (*Snapshot, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("policy: read: %w", err)
	}
	return Parse(raw)
}

// Status describes the active policy and the outcome of the last reload attempt.
type Status struct {
	Version           string     `json:"version"`
	Digest            string     `json:"digest"`
	LoadedAt          time.Time  `json:"loaded_at"`
	MaxParallelTasks  int        `json:"max_parallel_tasks"`
	ReloadStatus      string     `json:"reload_status"`
	LastReloadError   string     `json:"last_reload_error,omitempty"`
	LastReloadErrorAt *time.Time `json:"last_reload_error_at,omitempty"`
}

// Loader holds the active policy and re-reads the file on an interval. A
// change is detected by comparing SHA-256 digests of the file contents, so it
// does not depend on modification times or on which fields changed. An invalid
// file never replaces the active policy.
type Loader struct {
	path string
	log  *slog.Logger

	current  atomic.Pointer[Snapshot]
	reloadMu sync.Mutex // serializes Reload

	mu         sync.Mutex // guards the fields below
	loadedAt   time.Time
	lastErr    string
	lastErrAt  time.Time
	lastBadKey string

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

// NewLoader loads the policy at path and fails if it is invalid. When interval
// is positive, a background goroutine calls Reload on that interval.
func NewLoader(path string, interval time.Duration, log *slog.Logger) (*Loader, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	s, err := Load(path)
	if err != nil {
		return nil, err
	}
	l := &Loader{
		path:     path,
		log:      log,
		loadedAt: time.Now().UTC(),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	l.current.Store(s)
	if interval > 0 {
		go l.loop(interval)
	} else {
		close(l.done)
	}
	return l, nil
}

// Current returns the active policy snapshot.
func (l *Loader) Current() *Snapshot { return l.current.Load() }

// Reload re-reads the policy file. If its digest differs from the active
// policy and it validates, it becomes active. On any error the active policy
// is kept and the error is recorded in Status.
func (l *Loader) Reload() error {
	l.reloadMu.Lock()
	defer l.reloadMu.Unlock()

	raw, err := os.ReadFile(l.path)
	if err != nil {
		err = fmt.Errorf("policy: read: %w", err)
		l.recordError("read:"+err.Error(), err)
		return err
	}
	cur := l.current.Load()
	digest := rawDigest(raw)
	if digest == cur.Digest() {
		l.clearError()
		return nil
	}
	next, err := Parse(raw)
	if err != nil {
		l.recordError(digest, err)
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastErr, l.lastErrAt, l.lastBadKey = "", time.Time{}, ""
	l.current.Store(next)
	l.loadedAt = time.Now().UTC()
	l.log.Info("policy reloaded",
		"version", next.Version(),
		"digest", next.Digest(),
		"previous_version", cur.Version(),
		"previous_digest", cur.Digest(),
	)
	if next.MaxParallelTasks() != cur.MaxParallelTasks() {
		l.log.Warn("max_parallel_tasks changed but is applied at startup only; restart to take effect",
			"configured", next.MaxParallelTasks())
	}
	return nil
}

func (l *Loader) clearError() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastErr, l.lastErrAt, l.lastBadKey = "", time.Time{}, ""
}

// recordError keeps the active policy, records err for Status and logs it once
// per distinct failure (badKey identifies the rejected content or read error).
func (l *Loader) recordError(badKey string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	first := badKey != l.lastBadKey
	l.lastErr = err.Error()
	l.lastErrAt = time.Now().UTC()
	l.lastBadKey = badKey
	if first {
		cur := l.current.Load()
		l.log.Error("policy reload rejected; keeping active policy",
			"error", err,
			"active_version", cur.Version(),
			"active_digest", cur.Digest(),
		)
	}
}

// Status reports the active policy identity and the last reload error, if any.
func (l *Loader) Status() Status {
	cur := l.current.Load()
	l.mu.Lock()
	defer l.mu.Unlock()
	st := Status{
		Version:          cur.Version(),
		Digest:           cur.Digest(),
		LoadedAt:         l.loadedAt,
		MaxParallelTasks: cur.MaxParallelTasks(),
		ReloadStatus:     "ok",
	}
	if l.lastErr != "" {
		at := l.lastErrAt
		st.ReloadStatus = "error"
		st.LastReloadError = l.lastErr
		st.LastReloadErrorAt = &at
	}
	return st
}

// Close stops the reload loop and waits for it to exit.
func (l *Loader) Close() {
	l.closeOnce.Do(func() { close(l.stop) })
	<-l.done
}

func (l *Loader) loop(interval time.Duration) {
	defer close(l.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			_ = l.Reload() // errors are recorded in Status and logged by Reload
		}
	}
}
