package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/sentrygate-ai/sentrygate/internal/audit"
	"github.com/sentrygate-ai/sentrygate/internal/config"
	"github.com/sentrygate-ai/sentrygate/internal/logging"
	"github.com/sentrygate-ai/sentrygate/internal/telemetry"
	"github.com/sentrygate-ai/sentrygate/workflows"
)

func main() {
	cfg := config.LoadFromEnvOptionalAPIKey()
	log := logging.New("sentrygate-worker")

	ctx := context.Background()
	shutdownTel, err := telemetry.Setup(ctx, cfg.OTelServiceName+"-worker", cfg.OTelExporter)
	if err != nil {
		log.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTel(context.Background()) }()

	auditStore, err := audit.Open(cfg.AuditDBPath)
	if err != nil {
		log.Error("audit db open failed", "path", cfg.AuditDBPath, "error", err)
		os.Exit(1)
	}
	defer auditStore.Close()

	c, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Error("unable to create Temporal client", "error", err)
		os.Exit(1)
	}
	defer c.Close()

	w := worker.New(c, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.SentryGateSagaWorkflow)
	w.RegisterActivity(&workflows.InfrastructureActivities{})
	w.RegisterActivity(&workflows.AuditActivities{Store: auditStore})

	// Lightweight readiness HTTP for container probes.
	go serveWorkerHealth(cfg.Addr, auditStore, log)

	log.Info("temporal worker started",
		"queue", cfg.TaskQueue,
		"host", cfg.TemporalHostPort,
		"namespace", cfg.TemporalNamespace,
		"audit_db", cfg.AuditDBPath,
	)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Error("worker terminated", "error", err)
		os.Exit(1)
	}
}

func serveWorkerHealth(proxyAddr string, store *audit.Store, log *slog.Logger) {
	// Worker health binds to :8081 by default to avoid colliding with the proxy.
	addr := os.Getenv("SENTRYGATE_WORKER_HEALTH_ADDR")
	if addr == "" {
		addr = ":8081"
	}
	_ = proxyAddr

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			http.Error(w, "audit db not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	log.Info("worker health listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("worker health server failed", "error", err)
	}
}
