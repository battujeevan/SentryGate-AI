package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.temporal.io/sdk/client"

	"github.com/battujeevan/SentryGate-AI/internal/audit"
	"github.com/battujeevan/SentryGate-AI/internal/auth"
	"github.com/battujeevan/SentryGate-AI/internal/config"
	"github.com/battujeevan/SentryGate-AI/internal/logging"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/internal/telemetry"
	"github.com/battujeevan/SentryGate-AI/proxy"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}
	log := logging.New("sentrygate-proxy")

	keyring, err := auth.ParseAgentKeys(cfg.AgentKeys)
	if err != nil {
		log.Error("invalid SENTRYGATE_AGENT_KEYS", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	shutdownTel, err := telemetry.Setup(ctx, cfg.OTelServiceName+"-proxy", cfg.OTelExporter)
	if err != nil {
		log.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTel(context.Background()) }()

	policyLoader, err := policy.NewLoader(cfg.PolicyPath, cfg.PolicyReloadEvery, log)
	if err != nil {
		log.Error("policy load failed", "path", cfg.PolicyPath, "error", err)
		os.Exit(1)
	}
	defer policyLoader.Close()

	pol := policyLoader.Current()
	for _, id := range keyring.AgentIDs() {
		if !pol.HasAgent(id) {
			log.Warn("agent key configured for an agent not declared in the policy; its requests will be denied", "agent_id", id)
		}
	}
	sentry := proxy.NewSentryProxy(policyLoader, pol.MaxParallelTasks())

	auditStore, err := audit.Open(cfg.AuditDBPath)
	if err != nil {
		log.Error("audit db open failed", "path", cfg.AuditDBPath, "error", err)
		os.Exit(1)
	}
	defer auditStore.Close()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Error("temporal dial failed", "host", cfg.TemporalHostPort, "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	api := &apiServer{
		log:       log,
		keyring:   keyring,
		proxy:     sentry,
		workflows: temporalClient,
		taskQueue: cfg.TaskQueue,
		decisions: auditStore,
		audit:     auditStore,
		policy:    policyLoader,
		ready:     auditStore,
	}

	// Once ReadTimeout expires, net/http also cancels the request context of a
	// running handler, so it must exceed the worst-case intercept: SQLite busy
	// timeout (5s) plus the Temporal start RPC (10s SDK default). No
	// WriteTimeout: it would cut off a response after the workflow had started.
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           otelhttp.NewHandler(newHandler(api), "sentrygate-proxy"),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("proxy listening",
			"addr", cfg.Addr,
			"policy_version", pol.Version(),
			"policy_digest", pol.Digest(),
			"max_parallel_tasks", sentry.MaxParallel(),
			"agents_with_keys", len(keyring.AgentIDs()),
			"temporal", cfg.TemporalHostPort,
		)
		log.Info("max_parallel_tasks is applied at startup only; policy reloads do not resize the evaluation pool")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("proxy server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("proxy shut down cleanly")
}
