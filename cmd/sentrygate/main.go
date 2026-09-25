package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}
	log := logging.New("sentrygate-proxy")

	ctx := context.Background()
	shutdownTel, err := telemetry.Setup(ctx, cfg.OTelServiceName+"-proxy", cfg.OTelExporter)
	if err != nil {
		log.Error("telemetry setup failed", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTel(context.Background()) }()

	policyLoader, err := policy.NewLoader(cfg.PolicyPath, cfg.PolicyReloadEvery)
	if err != nil {
		log.Error("policy load failed", "path", cfg.PolicyPath, "error", err)
		os.Exit(1)
	}
	defer policyLoader.Close()

	pol := policyLoader.Get()
	sentry := proxy.NewSentryProxy(pol, nil, nil)

	// Hot-reload policy into the proxy on an interval.
	go func() {
		t := time.NewTicker(cfg.PolicyReloadEvery)
		defer t.Stop()
		var last contracts.PolicyConfig = pol
		for range t.C {
			cur := policyLoader.Get()
			if cur.MaxRiskCeiling != last.MaxRiskCeiling || len(cur.ProtectedTargets) != len(last.ProtectedTargets) {
				sentry.ApplyPolicy(cur)
				log.Info("policy hot-reloaded", "risk_ceiling", cur.MaxRiskCeiling, "protected", cur.ProtectedTargets)
				last = cur
			}
		}
	}()

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
		log:      log,
		proxy:    sentry,
		temporal: temporalClient,
		queue:    cfg.TaskQueue,
		audit:    auditStore,
		cfg:      cfg,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", api.healthz)
	mux.HandleFunc("/readyz", api.readyz)

	protected := http.NewServeMux()
	protected.HandleFunc("/v1/intercept", api.intercept)
	protected.HandleFunc("/v1/audit/", api.auditByProposal)
	protected.HandleFunc("/v1/policy", api.policy)
	mux.Handle("/v1/", auth.APIKeyMiddleware(cfg.APIKey, protected))

	handler := otelhttp.NewHandler(mux, "sentrygate-proxy")
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("proxy listening",
			"addr", cfg.Addr,
			"risk_ceiling", pol.MaxRiskCeiling,
			"workers", pol.MaxParallelTasks,
			"temporal", cfg.TemporalHostPort,
		)
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

type apiServer struct {
	log      *slog.Logger
	proxy    *proxy.SentryProxy
	temporal client.Client
	queue    string
	audit    *audit.Store
	cfg      config.Config
}

func (a *apiServer) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *apiServer) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.audit.Ping(ctx); err != nil {
		http.Error(w, "audit db not ready", http.StatusServiceUnavailable)
		return
	}
	// Temporal connectivity: check namespace describe via workflow service is heavy;
	// a lightweight check is sufficient — Dial already succeeded at boot.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (a *apiServer) policy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(a.proxy.PolicySnapshot())
}

func (a *apiServer) auditByProposal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := r.URL.Path[len("/v1/audit/"):]
	if id == "" {
		http.Error(w, "proposal id required", http.StatusBadRequest)
		return
	}
	rows, err := a.audit.ListByProposal(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rows)
}

func (a *apiServer) intercept(w http.ResponseWriter, r *http.Request) {
	ctx, span := telemetry.Tracer("sentrygate/proxy").Start(r.Context(), "InterceptAndValidate")
	defer span.End()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	prop := proxy.AcquireProposal()
	defer proxy.ReleaseProposal(prop)

	if err := json.NewDecoder(r.Body).Decode(prop); err != nil {
		http.Error(w, fmt.Sprintf("invalid agent proposal: %v", err), http.StatusBadRequest)
		return
	}
	if prop.ID == "" {
		http.Error(w, "proposal id is required", http.StatusBadRequest)
		return
	}

	// Copy proposal before releasing pool buffer for Temporal payload.
	submitted := *prop

	err := a.proxy.InterceptAndValidateThrottled(ctx, prop)
	if err != nil {
		a.log.Warn("proposal rejected", "proposal_id", submitted.ID, "error", err)
		span.RecordError(err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "rejected",
			"error":  err.Error(),
		})
		return
	}

	workflowID := fmt.Sprintf("saga-%s", submitted.ID)
	run, err := a.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: a.queue,
	}, workflows.SentryGateSagaWorkflow, submitted)
	if err != nil {
		a.log.Error("workflow start failed", "proposal_id", submitted.ID, "error", err)
		http.Error(w, fmt.Sprintf("failed to start saga: %v", err), http.StatusBadGateway)
		return
	}

	a.log.Info("proposal accepted; saga started",
		"proposal_id", submitted.ID,
		"workflow_id", run.GetID(),
		"run_id", run.GetRunID(),
		"target", submitted.TargetID,
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":      "accepted",
		"proposal_id": submitted.ID,
		"workflow_id": run.GetID(),
		"run_id":      run.GetRunID(),
		"next":        "query /v1/audit/" + submitted.ID,
	})
}
