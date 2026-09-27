package proxy

import (
	"context"
	"sync"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// proposalPool recycles AgentProposal envelopes decoded at ingress.
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

// SentryProxy evaluates agent proposals at ingress. It reads the active policy
// snapshot from its Source on every call, so a policy reload takes effect on
// the next request. Concurrency is bounded by a fixed-size slot pool sized at
// construction; changing max_parallel_tasks requires a restart.
type SentryProxy struct {
	policy     policy.Source
	workerPool chan struct{}
}

// NewSentryProxy constructs a proxy over a policy source with maxParallel
// evaluation slots (minimum 1).
func NewSentryProxy(src policy.Source, maxParallel int) *SentryProxy {
	if maxParallel < 1 {
		maxParallel = 1
	}
	return &SentryProxy{
		policy:     src,
		workerPool: make(chan struct{}, maxParallel),
	}
}

// Evaluate returns the verdict for a proposal made by agentID against the
// currently active policy. It has no side effects.
func (p *SentryProxy) Evaluate(agentID string, prop *contracts.AgentProposal) contracts.Decision {
	return decision.Evaluate(p.policy.Current(), agentID, *prop)
}

// EvaluateThrottled acquires an evaluation slot, evaluates, then releases the
// slot. It returns ctx.Err() if the context ends while waiting for a slot.
func (p *SentryProxy) EvaluateThrottled(ctx context.Context, agentID string, prop *contracts.AgentProposal) (contracts.Decision, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Decision{}, err
	}
	select {
	case <-ctx.Done():
		return contracts.Decision{}, ctx.Err()
	case p.workerPool <- struct{}{}:
	}
	d := p.Evaluate(agentID, prop)
	<-p.workerPool
	return d, nil
}

// MaxParallel reports the fixed evaluation concurrency.
func (p *SentryProxy) MaxParallel() int { return cap(p.workerPool) }
