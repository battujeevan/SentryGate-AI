package main

import (
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/config"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func TestNewAdapter(t *testing.T) {
	t.Setenv("SENTRYGATE_ADAPTER", "")
	t.Setenv("SENTRYGATE_MCP_URL", "")
	t.Setenv("SENTRYGATE_MCP_TARGET_ARGUMENT", "")
	cfg := config.LoadWorkerFromEnv()
	if a, err := newAdapter(cfg); err != nil {
		t.Fatalf("default adapter: %v", err)
	} else if _, ok := a.(*workflows.SimulatedAdapter); !ok {
		t.Fatalf("default adapter = %T, want *workflows.SimulatedAdapter", a)
	}

	cfg.Adapter = "mcp"
	if _, err := newAdapter(cfg); err == nil {
		t.Fatal("mcp adapter without SENTRYGATE_MCP_URL was accepted")
	}
	cfg.MCPURL = "http://127.0.0.1:9/mcp"
	a, err := newAdapter(cfg)
	if err != nil {
		t.Fatalf("mcp adapter: %v", err)
	}
	m, ok := a.(*workflows.MCPAdapter)
	if !ok || m.Endpoint != cfg.MCPURL || m.TargetArgument != "target_id" {
		t.Fatalf("mcp adapter = %#v, want endpoint %s and target argument target_id", a, cfg.MCPURL)
	}

	cfg.Adapter = "real-f5"
	if _, err := newAdapter(cfg); err == nil {
		t.Fatal("unknown adapter was accepted")
	}
}
