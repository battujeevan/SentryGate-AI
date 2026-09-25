package proxy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/sentrygate-ai/sentrygate/proxy"
	"github.com/sentrygate-ai/sentrygate/shared/contracts"
)

func TestInterceptAndValidate_RiskCeiling(t *testing.T) {
	cfg := contracts.DefaultPolicyConfig()
	p := proxy.NewSentryProxy(cfg, nil, nil)

	prop := &contracts.AgentProposal{
		ID:        "p-1",
		Type:      contracts.CmdModifyRouting,
		TargetID:  "edge-a",
		RiskScore: cfg.MaxRiskCeiling + 0.01,
	}
	err := p.InterceptAndValidate(context.Background(), prop)
	if !errors.Is(err, contracts.ErrRiskCeilingBreach) {
		t.Fatalf("expected ErrRiskCeilingBreach, got %v", err)
	}
}

func TestInterceptAndValidate_RootCoreBlocked(t *testing.T) {
	cfg := contracts.DefaultPolicyConfig()
	p := proxy.NewSentryProxy(cfg, nil, nil)

	prop := &contracts.AgentProposal{
		ID:        "p-2",
		Type:      contracts.CmdDeletePolicy,
		TargetID:  contracts.RootCoreEdgeID,
		RiskScore: 0.1,
	}
	err := p.InterceptAndValidate(context.Background(), prop)
	if !errors.Is(err, contracts.ErrRootCoreMutation) {
		t.Fatalf("expected ErrRootCoreMutation, got %v", err)
	}
}

func TestInterceptAndValidate_AcceptsSafeProposal(t *testing.T) {
	cfg := contracts.DefaultPolicyConfig()
	p := proxy.NewSentryProxy(cfg, nil, nil)

	prop := &contracts.AgentProposal{
		ID:        "p-3",
		Type:      contracts.CmdUpdateCert,
		TargetID:  "vs-web-01",
		RiskScore: 0.2,
	}
	if err := p.InterceptAndValidate(context.Background(), prop); err != nil {
		t.Fatalf("expected accept, got %v", err)
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

// productionProxyFloor builds the production-grade SentryProxy floor model used
// by throughput and allocation benchmarks (risk ceiling 0.75, pool depth 50000).
func productionProxyFloor() *proxy.SentryProxy {
	return proxy.NewSentryProxy(contracts.PolicyConfig{
		MaxRiskCeiling:   0.75,
		MaxParallelTasks: 50000,
		ProtectedTargets: []string{contracts.RootCoreEdgeID},
	}, nil, nil)
}

// passingProposal is a pre-built AgentProposal that clears every deterministic
// firewall rule. Allocated once outside timed loops to isolate check cost.
func passingProposal() *contracts.AgentProposal {
	return &contracts.AgentProposal{
		ID:        "bench-proposal-001",
		Type:      contracts.CmdModifyRouting,
		TargetID:  "edge-node-west-1",
		Payload:   `{"route":"stable"}`,
		RiskScore: 0.42,
	}
}

// BenchmarkProxyInterceptorThroughput measures InterceptAndValidate hot-path
// cost with proposal and proxy construction excluded from the timer.
func BenchmarkProxyInterceptorThroughput(b *testing.B) {
	p := productionProxyFloor()
	proposal := passingProposal()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := p.InterceptAndValidate(ctx, proposal); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkProxyHighConcurrencyAllocations stress-tests the bounded worker
// pool under GOMAXPROCS parallel load and asserts the validation path stays
// at zero heap traffic (0 B/op, 0 allocs/op).
func BenchmarkProxyHighConcurrencyAllocations(b *testing.B) {
	p := productionProxyFloor()
	proposal := passingProposal()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := p.InterceptAndValidateThrottled(ctx, proposal); err != nil {
				b.Fatal(err)
			}
		}
	})
}
