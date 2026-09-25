package proxy

import (
	"context"
	"fmt"
	"sync"

	"github.com/sentrygate-ai/sentrygate/shared/contracts"
)

// proposalPool recycles AgentProposal envelopes to reduce GC pressure under
// high-frequency LLM tool-call ingress.
var proposalPool = sync.Pool{
	New: func() any {
		return &contracts.AgentProposal{}
	},
}

// AcquireProposal borrows a zeroed AgentProposal from the object pool.
func AcquireProposal() *contracts.AgentProposal {
	p := proposalPool.Get().(*contracts.AgentProposal)
	*p = contracts.AgentProposal{}
	return p
}

// ReleaseProposal returns a proposal envelope to the pool after use.
func ReleaseProposal(p *contracts.AgentProposal) {
	if p == nil {
		return
	}
	*p = contracts.AgentProposal{}
	proposalPool.Put(p)
}

// SentryProxy is the deterministic ingress firewall sitting between
// autonomous AI agents and enterprise infrastructure endpoints.
type SentryProxy struct {
	mu               sync.RWMutex
	maxRiskCeiling   float64
	workerPool       chan struct{}
	protectedTargets map[string]struct{}
	f5               F5BIGIPClient
	zscaler          ZscalerClient
}

// NewSentryProxy constructs a proxy from a compliance PolicyConfig and
// optional downstream clients. Nil clients are replaced with in-memory mocks.
func NewSentryProxy(cfg contracts.PolicyConfig, f5 F5BIGIPClient, zscaler ZscalerClient) *SentryProxy {
	if cfg.MaxParallelTasks <= 0 {
		cfg.MaxParallelTasks = 1
	}
	protected := make(map[string]struct{}, len(cfg.ProtectedTargets))
	for _, id := range cfg.ProtectedTargets {
		protected[id] = struct{}{}
	}
	if f5 == nil {
		f5 = NewMockF5Client()
	}
	if zscaler == nil {
		zscaler = NewMockZscalerClient()
	}
	return &SentryProxy{
		maxRiskCeiling:   cfg.MaxRiskCeiling,
		workerPool:       make(chan struct{}, cfg.MaxParallelTasks),
		protectedTargets: protected,
		f5:               f5,
		zscaler:          zscaler,
	}
}

// InterceptAndValidate applies non-bypassable safety boundaries to an
// AgentProposal. Validation is pure and side-effect free.
func (p *SentryProxy) InterceptAndValidate(ctx context.Context, prop *contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if prop == nil {
		return fmt.Errorf("security isolation: nil agent proposal")
	}

	p.mu.RLock()
	ceiling := p.maxRiskCeiling
	_, blockedTarget := p.protectedTargets[prop.TargetID]
	p.mu.RUnlock()

	// Rule 1: High-risk threshold drop
	if prop.RiskScore > ceiling {
		return fmt.Errorf("%w: risk score %.2f exceeds ceiling %.2f",
			contracts.ErrRiskCeilingBreach, prop.RiskScore, ceiling)
	}

	// Rule 2: Root / protected-target mandate (deterministic contract edge)
	if prop.Type == contracts.CmdDeletePolicy && blockedTarget {
		return contracts.ErrRootCoreMutation
	}

	return nil
}

// ApplyPolicy hot-swaps risk ceiling and protected targets without resizing
// the worker pool (pool depth is fixed at construction time).
func (p *SentryProxy) ApplyPolicy(cfg contracts.PolicyConfig) {
	protected := make(map[string]struct{}, len(cfg.ProtectedTargets))
	for _, id := range cfg.ProtectedTargets {
		protected[id] = struct{}{}
	}
	p.mu.Lock()
	p.maxRiskCeiling = cfg.MaxRiskCeiling
	p.protectedTargets = protected
	p.mu.Unlock()
}

// PolicySnapshot returns the currently enforced ceiling and protected targets.
func (p *SentryProxy) PolicySnapshot() contracts.PolicyConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	targets := make([]string, 0, len(p.protectedTargets))
	for id := range p.protectedTargets {
		targets = append(targets, id)
	}
	return contracts.PolicyConfig{
		MaxRiskCeiling:   p.maxRiskCeiling,
		MaxParallelTasks: cap(p.workerPool),
		ProtectedTargets: targets,
	}
}

// WithThrottle acquires a worker-pool slot, runs fn, then releases the slot.
// Context cancellation unblocks waiters without leaking capacity.
// The release path avoids a deferred closure so hot-path callers can stay at 0 allocs/op.
func (p *SentryProxy) WithThrottle(ctx context.Context, fn func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.workerPool <- struct{}{}:
	}
	err := fn()
	<-p.workerPool
	return err
}

// InterceptAndValidateThrottled acquires a worker-pool slot, runs the
// deterministic firewall check, then releases the slot — without a callback
// closure, so concurrent benchmarks can remain at 0 B/op and 0 allocs/op.
func (p *SentryProxy) InterceptAndValidateThrottled(ctx context.Context, prop *contracts.AgentProposal) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.workerPool <- struct{}{}:
	}
	err := p.InterceptAndValidate(ctx, prop)
	<-p.workerPool
	return err
}

// F5 returns the configured BIG-IP client for certificate and routing ops.
func (p *SentryProxy) F5() F5BIGIPClient { return p.f5 }

// Zscaler returns the configured Zero-Trust policy client.
func (p *SentryProxy) Zscaler() ZscalerClient { return p.zscaler }
