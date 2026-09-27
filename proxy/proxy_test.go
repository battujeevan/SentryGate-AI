package proxy_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/proxy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

const benchPolicy = `version: bench
max_parallel_tasks: 64
environments:
  staging: allow
  production: require_approval
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - id: agent-a
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-node-west-1, environment: staging, protected: false}
`

func mustParse(t testing.TB, doc string) *policy.Snapshot {
	t.Helper()
	s, err := policy.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// swappableSource lets a test replace the active snapshot between calls.
type swappableSource struct {
	p atomic.Pointer[policy.Snapshot]
}

func (s *swappableSource) Current() *policy.Snapshot { return s.p.Load() }

func TestEvaluateUsesCurrentSnapshot(t *testing.T) {
	src := &swappableSource{}
	src.p.Store(mustParse(t, benchPolicy))
	p := proxy.NewSentryProxy(src, 4)
	prop := &contracts.AgentProposal{ID: "p", Type: contracts.CmdModifyRouting, TargetID: "edge-node-west-1"}

	if d := p.Evaluate("agent-a", prop); d.Verdict != contracts.VerdictAllow {
		t.Fatalf("expected ALLOW, got %+v", d)
	}

	src.p.Store(mustParse(t, `version: tightened
max_parallel_tasks: 64
environments: {staging: allow, production: require_approval}
commands: [MODIFY_ROUTING]
agents: [{id: agent-a, commands: [MODIFY_ROUTING]}]
targets:
  - {id: edge-node-west-1, environment: staging, protected: true}
`))
	d := p.Evaluate("agent-a", prop)
	if d.Verdict != contracts.VerdictDeny || d.PolicyVersion != "tightened" {
		t.Fatalf("policy swap not observed: %+v", d)
	}
}

func TestEvaluateThrottledHonoursCancellation(t *testing.T) {
	p := proxy.NewSentryProxy(policy.StaticSource(mustParse(t, benchPolicy)), 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.EvaluateThrottled(ctx, "agent-a", &contracts.AgentProposal{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestMockF5CertificateFlow(t *testing.T) {
	f5 := proxy.NewMockF5Client()
	ctx := context.Background()
	if err := f5.Authenticate(ctx); err != nil {
		t.Fatalf("auth: %v", err)
	}
	res, err := f5.UpdateCertificate(ctx, proxy.CertUpdateRequest{
		VirtualServer: "vs-web-01",
		CommonName:    "api.example.com",
		CertPEM:       "-----BEGIN CERT-----\nMOCK\n-----END CERT-----",
		KeyPEM:        "-----BEGIN KEY-----\nMOCK\n-----END KEY-----",
		ACMEOrderID:   "le-order-1",
	})
	if err != nil {
		t.Fatalf("cert update: %v", err)
	}
	if res.TLSVersion != "TLS1.3" {
		t.Fatalf("unexpected TLS version %q", res.TLSVersion)
	}
}

func TestMockZscalerPolicyFlow(t *testing.T) {
	zs := proxy.NewMockZscalerClient()
	ctx := context.Background()
	if err := zs.Authenticate(ctx); err != nil {
		t.Fatalf("auth: %v", err)
	}
	res, err := zs.UpsertPolicy(ctx, proxy.ZscalerPolicyRequest{
		PolicyID:   "pol-42",
		Action:     proxy.ZscalerActionAllow,
		AppSegment: "corp-intranet",
		UserGroup:  "eng",
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if res.Revision != 1 {
		t.Fatalf("expected revision 1, got %d", res.Revision)
	}
	if err := zs.DeletePolicy(ctx, contracts.RootCoreEdgeID); !errors.Is(err, contracts.ErrRootCoreMutation) {
		t.Fatalf("expected root block on delete, got %v", err)
	}
}

func passingProposal() *contracts.AgentProposal {
	return &contracts.AgentProposal{
		ID:       "bench-proposal-001",
		Type:     contracts.CmdModifyRouting,
		TargetID: "edge-node-west-1",
		Payload:  `{"route":"stable"}`,
	}
}

// BenchmarkProxyEvaluate measures the policy evaluation hot path with proxy
// and proposal construction excluded from the timer.
func BenchmarkProxyEvaluate(b *testing.B) {
	p := proxy.NewSentryProxy(policy.StaticSource(mustParse(b, benchPolicy)), 64)
	prop := passingProposal()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if d := p.Evaluate("agent-a", prop); d.Verdict != contracts.VerdictAllow {
			b.Fatal(d)
		}
	}
}

// BenchmarkProxyEvaluateThrottledParallel measures evaluation through the
// bounded slot pool under GOMAXPROCS parallel callers.
func BenchmarkProxyEvaluateThrottledParallel(b *testing.B) {
	p := proxy.NewSentryProxy(policy.StaticSource(mustParse(b, benchPolicy)), 64)
	prop := passingProposal()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d, err := p.EvaluateThrottled(ctx, "agent-a", prop)
			if err != nil || d.Verdict != contracts.VerdictAllow {
				b.Error(d, err)
				return
			}
		}
	})
}
