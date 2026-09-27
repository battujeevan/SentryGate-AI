package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/internal/config"
	"github.com/battujeevan/SentryGate-AI/internal/logging"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/internal/telemetry"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func main() {
	cfg := config.LoadWorkerFromEnv()
	log := logging.New("sentrygate-worker")

	ctx := context.Background()
	shutdownTel, err := telemetry.Setup(ctx, cfg.OTelServiceName+"-worker", cfg.OTelExporter)
	if err != nil {
		log.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTel(context.Background()) }()

	// The worker re-validates every workflow against its own copy of the
	// policy, so it must start with a valid policy file.
	policyLoader, err := policy.NewLoader(cfg.PolicyPath, cfg.PolicyReloadEvery, log)
	if err != nil {
		log.Error("policy load failed", "path", cfg.PolicyPath, "error", err)
		os.Exit(1)
	}
	defer policyLoader.Close()

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
	w.RegisterActivity(&workflows.DecisionActivities{Policy: policyLoader, Store: auditStore})
	w.RegisterActivity(&workflows.InfrastructureActivities{})
	w.RegisterActivity(&workflows.AuditActivities{Store: auditStore})

	go serveWorkerHealth(auditStore, log)

	pol := policyLoader.Current()
	log.Info("temporal worker started",
		"queue", cfg.TaskQueue,
		"host", cfg.TemporalHostPort,
		"namespace", cfg.TemporalNamespace,
		"audit_db", cfg.AuditDBPath,
		"policy_version", pol.Version(),
		"policy_digest", pol.Digest(),
	)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Error("worker terminated", "error", err)
		os.Exit(1)
	}
}

func serveWorkerHealth(store *audit.Store, log *slog.Logger) {
	addr := os.Getenv("SENTRYGATE_WORKER_HEALTH_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := store.Ping(ctx); err != nil {
			http.Error(w, "audit db not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Info("worker health listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Error("worker health server failed", "error", err)
	}
}
