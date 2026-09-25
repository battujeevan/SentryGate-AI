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
	APIKey            string
	TemporalHostPort  string
	TemporalNamespace string
	TaskQueue         string
	PolicyPath        string
	AuditDBPath       string
	OTelServiceName   string
	OTelExporter      string // "stdout" | "none"
	ShutdownTimeout   time.Duration
	PolicyReloadEvery time.Duration
}

// LoadFromEnv reads configuration from the process environment.
// Proxy processes require SENTRYGATE_API_KEY.
func LoadFromEnv() (Config, error) {
	cfg := baseFromEnv()
	if cfg.APIKey == "" {
		return Config{}, fmt.Errorf("SENTRYGATE_API_KEY is required")
	}
	return cfg, nil
}

// LoadFromEnvOptionalAPIKey is used by the worker (no ingress auth).
func LoadFromEnvOptionalAPIKey() Config {
	return baseFromEnv()
}

func baseFromEnv() Config {
	return Config{
		Addr:              envOr("SENTRYGATE_ADDR", ":8080"),
		APIKey:            os.Getenv("SENTRYGATE_API_KEY"),
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
