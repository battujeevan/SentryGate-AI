package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/battujeevan/SentryGate-AI/internal/auth"
	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/internal/telemetry"
	"github.com/battujeevan/SentryGate-AI/proxy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

// maxBodyBytes bounds the size of a proposal request body.
const maxBodyBytes = 64 << 10

var proposalIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// workflowStarter is the subset of client.Client the handler needs.
type workflowStarter interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
}

type auditReader interface {
	ListByProposal(ctx context.Context, proposalID string) ([]contracts.AuditRecord, error)
}

type policyStatusSource interface {
	Status() policy.Status
}

type pinger interface {
	Ping(ctx context.Context) error
}

type apiServer struct {
	log       *slog.Logger
	keyring   *auth.Keyring
	proxy     *proxy.SentryProxy
	workflows workflowStarter
	taskQueue string
	decisions decision.Store
	audit     auditReader
	policy    policyStatusSource
	ready     pinger
}

// newHandler builds the HTTP routes. Everything under /v1/ requires an agent key.
func newHandler(a *apiServer) http.Handler {
	protected := http.NewServeMux()
	protected.HandleFunc("POST /v1/intercept", a.intercept)
	protected.HandleFunc("GET /v1/decisions/{proposal_id}", a.decisionsByProposal)
	protected.HandleFunc("GET /v1/audit/{proposal_id}", a.auditByProposal)
	protected.HandleFunc("GET /v1/policy", a.policyStatus)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	mux.Handle("/v1/", auth.Middleware(a.keyring, protected))
	return mux
}

type interceptResponse struct {
	Status     string                 `json:"status"`
	Verdict    contracts.Verdict      `json:"verdict,omitempty"`
	Reasons    []contracts.ReasonCode `json:"reasons,omitempty"`
	DecisionID string                 `json:"decision_id,omitempty"`
	ProposalID string                 `json:"proposal_id,omitempty"`
	WorkflowID string                 `json:"workflow_id,omitempty"`
	RunID      string                 `json:"run_id,omitempty"`
	Next       string                 `json:"next,omitempty"`
	Error      string                 `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, statusText, msg string) {
	writeJSON(w, status, interceptResponse{Status: statusText, Error: msg})
}

func (a *apiServer) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (a *apiServer) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.ready.Ping(ctx); err != nil {
		a.log.Warn("readiness check failed", "error", err)
		http.Error(w, "audit db not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

// policyStatus returns policy metadata only: version, digest and reload state.
func (a *apiServer) policyStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.policy.Status())
}

func (a *apiServer) decisionsByProposal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("proposal_id")
	if !proposalIDPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid", "invalid proposal id")
		return
	}
	rows, err := a.decisions.ListDecisionsByProposal(r.Context(), id)
	if err != nil {
		a.log.Error("list decisions failed", "proposal_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "error", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

func (a *apiServer) auditByProposal(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("proposal_id")
	if !proposalIDPattern.MatchString(id) {
		writeError(w, http.StatusBadRequest, "invalid", "invalid proposal id")
		return
	}
	rows, err := a.audit.ListByProposal(r.Context(), id)
	if err != nil {
		a.log.Error("list audit failed", "proposal_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "error", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

// decodeProposal strictly decodes exactly one JSON object into prop. It
// returns the HTTP status and a client-safe message on failure.
func decodeProposal(r *http.Request, w http.ResponseWriter, prop *contracts.AgentProposal) (int, string) {
	body := http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	tooLarge := func(err error) bool {
		var mbe *http.MaxBytesError
		return errors.As(err, &mbe)
	}
	if err := dec.Decode(prop); err != nil {
		if tooLarge(err) {
			return http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes)
		}
		return http.StatusBadRequest, "invalid agent proposal: " + err.Error()
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if tooLarge(err) {
			return http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxBodyBytes)
		}
		return http.StatusBadRequest, "invalid agent proposal: unexpected data after JSON object"
	}
	if !proposalIDPattern.MatchString(prop.ID) {
		return http.StatusBadRequest, "invalid agent proposal: id must match " + proposalIDPattern.String()
	}
	return 0, ""
}

func (a *apiServer) intercept(w http.ResponseWriter, r *http.Request) {
	ctx, span := telemetry.Tracer("sentrygate/proxy").Start(r.Context(), "Intercept")
	defer span.End()

	agentID, ok := auth.AgentIDFromContext(ctx)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid API key")
		return
	}

	prop := proxy.AcquireProposal()
	defer proxy.ReleaseProposal(prop)

	if status, msg := decodeProposal(r, w, prop); status != 0 {
		// Malformed requests are not decisions: log them, do not record them.
		a.log.Warn("malformed proposal rejected", "agent_id", agentID, "status", status, "reason", msg)
		writeError(w, status, "invalid", msg)
		return
	}

	d, err := a.proxy.EvaluateThrottled(ctx, agentID, prop)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "request cancelled before evaluation")
		return
	}

	workflowID := "saga-" + prop.ID
	rec := contracts.DecisionRecord{
		DecisionID:    decision.NewDecisionID(),
		Stage:         contracts.StageIngress,
		ProposalID:    prop.ID,
		AgentID:       agentID,
		RequestHash:   decision.RequestHash(*prop),
		Command:       prop.Type,
		TargetID:      prop.TargetID,
		Environment:   d.Environment,
		Verdict:       d.Verdict,
		Reasons:       d.Reasons,
		PolicyVersion: d.PolicyVersion,
		PolicyDigest:  d.PolicyDigest,
		TraceID:       traceID(ctx),
		RecordedAt:    time.Now().UTC(),
	}
	if d.Verdict == contracts.VerdictAllow {
		rec.WorkflowID = workflowID
	}

	logAttrs := []any{
		"decision_id", rec.DecisionID, "proposal_id", prop.ID, "agent_id", agentID,
		"verdict", d.Verdict, "reasons", d.Reasons, "command", prop.Type, "target", prop.TargetID,
		"policy_version", d.PolicyVersion,
	}

	recordErr := a.decisions.AppendDecision(ctx, rec)
	if recordErr != nil {
		a.log.Error("decision record write failed", append(logAttrs, "error", recordErr)...)
	}

	resp := interceptResponse{
		Verdict:    d.Verdict,
		Reasons:    d.Reasons,
		DecisionID: rec.DecisionID,
		ProposalID: prop.ID,
	}
	if recordErr != nil {
		resp.DecisionID = ""
	}

	switch d.Verdict {
	case contracts.VerdictDeny:
		a.log.Warn("proposal denied", logAttrs...)
		resp.Status = "rejected"
		writeJSON(w, http.StatusForbidden, resp)
		return
	case contracts.VerdictRequireApproval:
		a.log.Warn("proposal requires approval; not executed", logAttrs...)
		resp.Status = "not_executed"
		writeJSON(w, http.StatusForbidden, resp)
		return
	case contracts.VerdictAllow:
	default:
		a.log.Error("unexpected verdict; failing closed", logAttrs...)
		writeError(w, http.StatusInternalServerError, "error", "internal error")
		return
	}

	// ALLOW: without a durable decision record no workflow may start.
	if recordErr != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "decision could not be recorded; proposal not executed")
		return
	}

	run, err := a.workflows.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: a.taskQueue,
		// Without this, the SDK returns the already-running execution as if it
		// had been started for this request.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, workflows.SentryGateSagaWorkflow, contracts.ExecutionRequest{
		AgentID:           agentID,
		Proposal:          *prop,
		IngressDecisionID: rec.DecisionID,
		RequestHash:       rec.RequestHash,
	})
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		a.log.Warn("workflow already running for proposal id; not executed", append(logAttrs, "workflow_id", workflowID)...)
		resp.Status = "not_executed"
		resp.Error = "a workflow for this proposal id is already running; this request was not executed"
		writeJSON(w, http.StatusConflict, resp)
		return
	}
	if err != nil {
		a.log.Error("workflow start failed", append(logAttrs, "workflow_id", workflowID, "error", err)...)
		writeError(w, http.StatusBadGateway, "error", "failed to start workflow")
		return
	}

	a.log.Info("proposal allowed; workflow started", append(logAttrs, "workflow_id", run.GetID(), "run_id", run.GetRunID())...)
	resp.Status = "accepted"
	resp.WorkflowID = run.GetID()
	resp.RunID = run.GetRunID()
	resp.Next = "GET /v1/decisions/" + prop.ID + " and /v1/audit/" + prop.ID
	writeJSON(w, http.StatusOK, resp)
}

func traceID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}
