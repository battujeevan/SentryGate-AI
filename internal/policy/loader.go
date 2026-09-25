package policy

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/sentrygate-ai/sentrygate/shared/contracts"
)

// File holds an on-disk policy document that mirrors contracts.PolicyConfig.
type File struct {
	MaxRiskCeiling   float64  `yaml:"max_risk_ceiling"`
	MaxParallelTasks int      `yaml:"max_parallel_tasks"`
	ProtectedTargets []string `yaml:"protected_targets"`
}

func (f File) ToConfig() contracts.PolicyConfig {
	cfg := contracts.PolicyConfig{
		MaxRiskCeiling:   f.MaxRiskCeiling,
		MaxParallelTasks: f.MaxParallelTasks,
		ProtectedTargets: append([]string(nil), f.ProtectedTargets...),
	}
	if cfg.MaxRiskCeiling == 0 {
		cfg.MaxRiskCeiling = contracts.DefaultPolicyConfig().MaxRiskCeiling
	}
	if cfg.MaxParallelTasks == 0 {
		cfg.MaxParallelTasks = contracts.DefaultPolicyConfig().MaxParallelTasks
	}
	if len(cfg.ProtectedTargets) == 0 {
		cfg.ProtectedTargets = contracts.DefaultPolicyConfig().ProtectedTargets
	}
	return cfg
}

// Loader watches a YAML policy file and hot-reloads it on an interval.
type Loader struct {
	path     string
	interval time.Duration
	current  atomic.Value // contracts.PolicyConfig
	mu       sync.Mutex
	lastMod  time.Time
	stop     chan struct{}
}

// NewLoader reads the initial policy and starts a background reload loop.
func NewLoader(path string, interval time.Duration) (*Loader, error) {
	l := &Loader{
		path:     path,
		interval: interval,
		stop:     make(chan struct{}),
	}
	cfg, mod, err := readPolicyFile(path)
	if err != nil {
		return nil, err
	}
	l.current.Store(cfg)
	l.lastMod = mod
	go l.loop()
	return l, nil
}

// Get returns the latest policy snapshot.
func (l *Loader) Get() contracts.PolicyConfig {
	return l.current.Load().(contracts.PolicyConfig)
}

// Close stops the reload loop.
func (l *Loader) Close() {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
}

func (l *Loader) loop() {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			_ = l.reloadIfChanged()
		}
	}
}

func (l *Loader) reloadIfChanged() error {
	info, err := os.Stat(l.path)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !info.ModTime().After(l.lastMod) {
		return nil
	}
	cfg, mod, err := readPolicyFile(l.path)
	if err != nil {
		return err
	}
	l.current.Store(cfg)
	l.lastMod = mod
	return nil
}

func readPolicyFile(path string) (contracts.PolicyConfig, time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return contracts.PolicyConfig{}, time.Time{}, fmt.Errorf("stat policy: %w", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return contracts.PolicyConfig{}, time.Time{}, fmt.Errorf("read policy: %w", err)
	}
	var f File
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return contracts.PolicyConfig{}, time.Time{}, fmt.Errorf("parse policy: %w", err)
	}
	return f.ToConfig(), info.ModTime(), nil
}

// LoadOnce reads a policy file without starting a watcher.
func LoadOnce(path string) (contracts.PolicyConfig, error) {
	cfg, _, err := readPolicyFile(path)
	return cfg, err
}
