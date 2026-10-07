package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds runtime settings for the proxy and Temporal worker.
// Values are loaded from environment variables (see .env.example).
type Config struct {
	Addr              string
	AgentKeys         string // raw SENTRYGATE_AGENT_KEYS; parse with auth.ParseAgentKeys, never log
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
	PolicyPath        string
	AuditDBPath       string
	OTelServiceName   string
	OTelExporter      string // "stdout" | "none"
	ShutdownTimeout   time.Duration
	PolicyReloadEvery time.Duration

	// Worker infrastructure adapter: "simulated" (default) or "mcp".
	Adapter           string
	MCPURL            string
	MCPTargetArgument string
}

// LoadFromEnv reads proxy configuration. SENTRYGATE_AGENT_KEYS is required.
func LoadFromEnv() (Config, error) {
	cfg := baseFromEnv()
	cfg.AgentKeys = os.Getenv("SENTRYGATE_AGENT_KEYS")
	if cfg.AgentKeys == "" {
		return Config{}, fmt.Errorf("SENTRYGATE_AGENT_KEYS is required (format: agent-id:key,agent-id:key)")
	}
	return cfg, nil
}

// LoadWorkerFromEnv reads worker configuration. The worker does not accept
// agent traffic and never needs agent keys.
func LoadWorkerFromEnv() Config {
	cfg := baseFromEnv()
	cfg.Adapter = envOr("SENTRYGATE_ADAPTER", "simulated")
	cfg.MCPURL = os.Getenv("SENTRYGATE_MCP_URL")
	cfg.MCPTargetArgument = envOr("SENTRYGATE_MCP_TARGET_ARGUMENT", "target_id")
	return cfg
}

func baseFromEnv() Config {
	return Config{
		Addr:              envOr("SENTRYGATE_ADDR", ":8080"),
		TemporalHostPort:  envOr("TEMPORAL_HOST_PORT", "localhost:7233"),
		TemporalNamespace: envOr("TEMPORAL_NAMESPACE", "default"),
		TaskQueue:         envOr("TEMPORAL_TASK_QUEUE", "sentrygate-saga"),
		PolicyPath:        envOr("SENTRYGATE_POLICY_PATH", "policies/default.yaml"),
		AuditDBPath:       envOr("SENTRYGATE_AUDIT_DB", "data/audit.db"),
		OTelServiceName:   envOr("OTEL_SERVICE_NAME", "sentrygate"),
		OTelExporter:      envOr("OTEL_EXPORTER", "stdout"),
		ShutdownTimeout:   durationOr("SENTRYGATE_SHUTDOWN_TIMEOUT", 10*time.Second),
		PolicyReloadEvery: durationOr("SENTRYGATE_POLICY_RELOAD", 5*time.Second),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func durationOr(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		if n, err2 := strconv.Atoi(v); err2 == nil {
			return time.Duration(n) * time.Second
		}
		return fallback
	}
	return d
}
