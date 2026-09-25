package policy_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func TestLoadOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	content := "max_risk_ceiling: 0.5\nmax_parallel_tasks: 4\nprotected_targets:\n  - ROOT_CORE_EDGE\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := policy.LoadOnce(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxRiskCeiling != 0.5 || cfg.MaxParallelTasks != 4 {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if len(cfg.ProtectedTargets) != 1 || cfg.ProtectedTargets[0] != contracts.RootCoreEdgeID {
		t.Fatalf("protected: %+v", cfg.ProtectedTargets)
	}
}

func TestLoaderHotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(path, []byte("max_risk_ceiling: 0.7\nmax_parallel_tasks: 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	loader, err := policy.NewLoader(path, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer loader.Close()

	if got := loader.Get().MaxRiskCeiling; got != 0.7 {
		t.Fatalf("got %v", got)
	}

	time.Sleep(20 * time.Millisecond) // ensure mtime advances on some FS
	if err := os.WriteFile(path, []byte("max_risk_ceiling: 0.9\nmax_parallel_tasks: 2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if loader.Get().MaxRiskCeiling == 0.9 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("policy did not hot-reload, still %v", loader.Get().MaxRiskCeiling)
}
