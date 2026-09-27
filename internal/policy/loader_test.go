package policy_test

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/policy"
)

func writePolicy(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newTestLoader(t *testing.T, content string) (*policy.Loader, string, *bytes.Buffer) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.yaml")
	writePolicy(t, path, content)
	var logs bytes.Buffer
	l, err := policy.NewLoader(path, 0, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}
	t.Cleanup(l.Close)
	return l, path, &logs
}

func TestLoaderRejectsInvalidStartupPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	writePolicy(t, path, validPolicy+"unknown_field: true\n")
	if _, err := policy.NewLoader(path, 0, nil); err == nil {
		t.Fatal("expected invalid startup policy to fail")
	}
	if _, err := policy.NewLoader(filepath.Join(t.TempDir(), "missing.yaml"), 0, nil); err == nil {
		t.Fatal("expected missing startup policy to fail")
	}
}

// The previous loader only applied a reload when the number of protected
// targets changed. Swapping which target is protected must take effect.
func TestLoaderReloadsSameLengthReplacement(t *testing.T) {
	l, path, _ := newTestLoader(t, validPolicy)
	if tgt, _ := l.Current().Target("edge-1"); tgt.Protected {
		t.Fatal("precondition: edge-1 unprotected")
	}
	swapped := strings.Replace(validPolicy, "    protected: true\n", "    protected: TMP\n", 1)
	swapped = strings.Replace(swapped, "    protected: false\n", "    protected: true\n", 1)
	swapped = strings.Replace(swapped, "    protected: TMP\n", "    protected: false\n", 1)
	writePolicy(t, path, swapped)

	if err := l.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if tgt, _ := l.Current().Target("edge-1"); !tgt.Protected {
		t.Fatal("same-length replacement was not applied")
	}
	if tgt, _ := l.Current().Target("ROOT_CORE_EDGE"); tgt.Protected {
		t.Fatal("same-length replacement was not applied")
	}
}

func TestLoaderReloadsWhenMtimeUnchanged(t *testing.T) {
	l, path, _ := newTestLoader(t, validPolicy)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	oldDigest := l.Current().Digest()

	writePolicy(t, path, strings.Replace(validPolicy, `"2026-09-27.1"`, `"2026-09-27.2"`, 1))
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := l.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if l.Current().Version() != "2026-09-27.2" || l.Current().Digest() == oldDigest {
		t.Fatalf("content change with unchanged mtime not detected: %s", l.Current().Version())
	}
}

func TestLoaderMalformedReloadKeepsActivePolicy(t *testing.T) {
	l, path, logs := newTestLoader(t, validPolicy)
	good := l.Current()

	writePolicy(t, path, validPolicy+"max_risk_ceiling: 0.9\n")
	if err := l.Reload(); err == nil {
		t.Fatal("expected malformed reload to fail")
	}
	if l.Current() != good {
		t.Fatal("malformed policy replaced the active policy")
	}
	st := l.Status()
	if st.ReloadStatus != "error" || st.LastReloadError == "" || st.LastReloadErrorAt == nil {
		t.Fatalf("reload error not visible in status: %+v", st)
	}
	if st.Version != good.Version() || st.Digest != good.Digest() {
		t.Fatalf("status must describe the active policy: %+v", st)
	}
	if !strings.Contains(logs.String(), "policy reload rejected") {
		t.Fatalf("reload error not logged: %s", logs.String())
	}

	// A repeated failure for the same content is recorded but logged once.
	_ = l.Reload()
	if n := strings.Count(logs.String(), "policy reload rejected"); n != 1 {
		t.Fatalf("expected one log line for repeated identical failure, got %d", n)
	}

	writePolicy(t, path, validPolicy)
	if err := l.Reload(); err != nil {
		t.Fatalf("restoring the active policy must succeed: %v", err)
	}
	if st := l.Status(); st.ReloadStatus != "ok" || st.LastReloadError != "" {
		t.Fatalf("error not cleared after recovery: %+v", st)
	}
}

func TestLoaderMissingFileKeepsActivePolicy(t *testing.T) {
	l, path, _ := newTestLoader(t, validPolicy)
	good := l.Current()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := l.Reload(); err == nil {
		t.Fatal("expected reload of missing file to fail")
	}
	if l.Current() != good || l.Status().ReloadStatus != "error" {
		t.Fatal("missing file must keep the active policy and report an error")
	}
}

func TestLoaderWarnsThatMaxParallelTasksIsStartupOnly(t *testing.T) {
	l, path, logs := newTestLoader(t, validPolicy)
	writePolicy(t, path, strings.Replace(validPolicy, "max_parallel_tasks: 4", "max_parallel_tasks: 8", 1))
	if err := l.Reload(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "applied at startup only") {
		t.Fatalf("expected startup-only warning, logs: %s", logs.String())
	}
}

func TestLoaderBackgroundReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.yaml")
	writePolicy(t, path, validPolicy)
	l, err := policy.NewLoader(path, 20*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	writePolicy(t, path, strings.Replace(validPolicy, `"2026-09-27.1"`, `"bg-2"`, 1))
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if l.Current().Version() == "bg-2" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("background reload not applied; version %s", l.Current().Version())
}
