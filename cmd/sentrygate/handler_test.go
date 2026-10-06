package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/battujeevan/SentryGate-AI/internal/auth"
	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/proxy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

const (
	keySim     = "sim-key-0123456789abcdef"
	keyLimited = "limited-key-0123456789ab"
	keyRogue   = "rogue-key-0123456789abcd"
)

var allKeys = []string{keySim, keyLimited, keyRogue}

const handlerPolicy = `version: http-v1
max_parallel_tasks: 8
environments:
  staging: allow
  production: require_approval
commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
agents:
  - id: agent-sim
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE, DELETE_POLICY]
  - id: limited
    commands: [MODIFY_ROUTING]
targets:
  - {id: ROOT_CORE_EDGE, environment: production, protected: true}
  - {id: edge-node-west-1, environment: staging, protected: false}
  - {id: prod-payments-edge, environment: production, protected: false}
`

type fakeRun struct{ id, runID string }

func (r fakeRun) GetID() string                          { return r.id }
func (r fakeRun) GetRunID() string                       { return r.runID }
func (r fakeRun) Get(context.Context, interface{}) error { return nil }
func (r fakeRun) GetWithOptions(context.Context, interface{}, client.WorkflowRunGetOptions) error {
	return nil
}

type fakeStarter struct {
	mu      sync.Mutex
	opts    []client.StartWorkflowOptions
	reqs    []contracts.ExecutionRequest
	err     error
	onStart func(contracts.ExecutionRequest)
	// running maps workflow IDs to the run ID of an execution that is already
	// open. It reproduces the SDK: unless WorkflowExecutionErrorWhenAlreadyStarted
	// is set, the existing run is returned without an error.
	running map[string]string
}

func (f *fakeStarter) ExecuteWorkflow(_ context.Context, opts client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	req := args[0].(contracts.ExecutionRequest)
	if f.onStart != nil {
		f.onStart(req)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if runID, ok := f.running[opts.ID]; ok {
		if opts.WorkflowExecutionErrorWhenAlreadyStarted {
			return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow execution already started", "", runID)
		}
		return fakeRun{id: opts.ID, runID: runID}, nil
	}
	f.opts = append(f.opts, opts)
	f.reqs = append(f.reqs, req)
	return fakeRun{id: opts.ID, runID: "run-1"}, nil
}

func (f *fakeStarter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

type failingStore struct{ decision.MemoryStore }

func (*failingStore) AppendDecision(context.Context, contracts.DecisionRecord) error {
	return errors.New("sqlite: database is locked")
}

type fakeStatus struct{ st policy.Status }

func (f fakeStatus) Status() policy.Status { return f.st }

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

type testServer struct {
	handler http.Handler
	starter *fakeStarter
	store   decision.Store
	logs    *bytes.Buffer
	snap    *policy.Snapshot
}

func newTestServer(t *testing.T, store decision.Store) *testServer {
	t.Helper()
	snap, err := policy.Parse([]byte(handlerPolicy))
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := auth.ParseAgentKeys("agent-sim:" + keySim + ",limited:" + keyLimited + ",rogue:" + keyRogue)
	if err != nil {
		t.Fatal(err)
	}
	if store == nil {
		store = decision.NewMemoryStore()
	}
	logs := &bytes.Buffer{}
	starter := &fakeStarter{}
	api := &apiServer{
		log:       slog.New(slog.NewJSONHandler(logs, nil)),
		keyring:   keyring,
		proxy:     proxy.NewSentryProxy(policy.StaticSource(snap), snap.MaxParallelTasks()),
		workflows: starter,
		taskQueue: "test-queue",
		decisions: store,
		audit:     workflows.NewMemoryAuditStore(),
		policy: fakeStatus{policy.Status{
			Version: snap.Version(), Digest: snap.Digest(), LoadedAt: time.Now().UTC(),
			MaxParallelTasks: snap.MaxParallelTasks(), ReloadStatus: "ok",
		}},
		ready: okPinger{},
	}
	return &testServer{handler: newHandler(api), starter: starter, store: store, logs: logs, snap: snap}
}

func (s *testServer) do(method, path, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set(auth.HeaderAPIKey, key)
	}
	rr := httptest.NewRecorder()
	s.handler.ServeHTTP(rr, req)
	return rr
}

func (s *testServer) records(t *testing.T) []contracts.DecisionRecord {
	t.Helper()
	ms, ok := s.store.(*decision.MemoryStore)
	if !ok {
		return nil
	}
	return ms.All()
}

func proposalJSON(id string, cmd contracts.CommandType, target, payload string) string {
	b, _ := json.Marshal(contracts.AgentProposal{ID: id, Type: cmd, TargetID: target, Payload: payload})
	return string(b)
}

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder) interceptResponse {
	t.Helper()
	var r interceptResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &r); err != nil {
		t.Fatalf("decode response %q: %v", rr.Body.String(), err)
	}
	return r
}

// --- Authentication ---

func TestInterceptAuthentication(t *testing.T) {
	s := newTestServer(t, nil)
	body := proposalJSON("auth-1", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)

	for name, key := range map[string]string{"missing key": "", "wrong key": "not-a-real-key-000000000"} {
		t.Run(name, func(t *testing.T) {
			rr := s.do(http.MethodPost, "/v1/intercept", key, body)
			if rr.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", rr.Code)
			}
		})
	}
	if s.starter.count() != 0 || len(s.records(t)) != 0 {
		t.Fatal("unauthenticated requests must not start workflows or create records")
	}

	rr := s.do(http.MethodPost, "/v1/intercept", keySim, body)
	if rr.Code != http.StatusOK {
		t.Fatalf("valid key: status = %d body=%s", rr.Code, rr.Body.String())
	}
	recs := s.records(t)
	if len(recs) != 1 || recs[0].AgentID != "agent-sim" {
		t.Fatalf("identity not resolved from key: %+v", recs)
	}
	if s.starter.reqs[0].AgentID != "agent-sim" {
		t.Fatalf("workflow input carries wrong identity: %+v", s.starter.reqs[0])
	}
}

func TestAgentKeysNeverAppearInResponsesLogsOrRecords(t *testing.T) {
	s := newTestServer(t, nil)
	var responses strings.Builder
	requests := []struct{ key, body string }{
		{keySim, proposalJSON("leak-allow", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)},
		{keySim, proposalJSON("leak-deny", contracts.CmdDeletePolicy, contracts.RootCoreEdgeID, `{}`)},
		{keyLimited, proposalJSON("leak-perm", contracts.CmdUpdateCert, "edge-node-west-1", `{}`)},
		{keyRogue, proposalJSON("leak-rogue", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)},
		{keySim, `{"id":"leak-bad","risk_score":0.1}`},
		{keySim + "x", proposalJSON("leak-401", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)},
	}
	for _, r := range requests {
		responses.WriteString(s.do(http.MethodPost, "/v1/intercept", r.key, r.body).Body.String())
	}
	responses.WriteString(s.do(http.MethodGet, "/v1/decisions/leak-allow", keySim, "").Body.String())
	responses.WriteString(s.do(http.MethodGet, "/v1/policy", keySim, "").Body.String())

	recJSON, _ := json.Marshal(s.records(t))
	inputs, _ := json.Marshal(s.starter.reqs)
	for _, key := range allKeys {
		for where, text := range map[string]string{
			"responses": responses.String(), "logs": s.logs.String(),
			"records": string(recJSON), "workflow input": string(inputs),
		} {
			if strings.Contains(text, key) {
				t.Fatalf("agent key leaked into %s", where)
			}
		}
	}
}

// --- Decision boundary ---

func TestInterceptDecisionBoundary(t *testing.T) {
	cases := []struct {
		name     string
		key      string
		cmd      contracts.CommandType
		target   string
		status   int
		respStat string
		verdict  contracts.Verdict
		reason   contracts.ReasonCode
	}{
		{"unknown command", keySim, "REBOOT_ALL", "edge-node-west-1", 403, "rejected", contracts.VerdictDeny, contracts.ReasonCommandUnknown},
		{"unknown target", keySim, contracts.CmdModifyRouting, "shadow-edge", 403, "rejected", contracts.VerdictDeny, contracts.ReasonTargetUnregistered},
		{"protected delete", keySim, contracts.CmdDeletePolicy, contracts.RootCoreEdgeID, 403, "rejected", contracts.VerdictDeny, contracts.ReasonTargetProtected},
		{"protected modify routing", keySim, contracts.CmdModifyRouting, contracts.RootCoreEdgeID, 403, "rejected", contracts.VerdictDeny, contracts.ReasonTargetProtected},
		{"protected update certificate", keySim, contracts.CmdUpdateCert, contracts.RootCoreEdgeID, 403, "rejected", contracts.VerdictDeny, contracts.ReasonTargetProtected},
		{"production requires approval", keySim, contracts.CmdUpdateCert, "prod-payments-edge", 403, "not_executed", contracts.VerdictRequireApproval, contracts.ReasonApprovalRequired},
		{"undeclared agent", keyRogue, contracts.CmdModifyRouting, "edge-node-west-1", 403, "rejected", contracts.VerdictDeny, contracts.ReasonAgentUnknown},
		{"command not permitted for agent", keyLimited, contracts.CmdUpdateCert, "edge-node-west-1", 403, "rejected", contracts.VerdictDeny, contracts.ReasonCommandNotPermitted},
		{"allowed staging", keySim, contracts.CmdModifyRouting, "edge-node-west-1", 200, "accepted", contracts.VerdictAllow, contracts.ReasonEnvironmentAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, nil)
			rr := s.do(http.MethodPost, "/v1/intercept", tc.key, proposalJSON("p-1", tc.cmd, tc.target, `{"k":"v"}`))
			if rr.Code != tc.status {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tc.status, rr.Body.String())
			}
			resp := decodeResp(t, rr)
			if resp.Status != tc.respStat || resp.Verdict != tc.verdict || !slices.Contains(resp.Reasons, tc.reason) {
				t.Fatalf("unexpected response: %+v", resp)
			}
			if resp.DecisionID == "" {
				t.Fatal("response must carry the decision id")
			}
			wantStarts := 0
			if tc.verdict == contracts.VerdictAllow {
				wantStarts = 1
			}
			if got := s.starter.count(); got != wantStarts {
				t.Fatalf("workflow starts = %d, want %d", got, wantStarts)
			}
			recs := s.records(t)
			if len(recs) != 1 || recs[0].Verdict != tc.verdict || recs[0].DecisionID != resp.DecisionID {
				t.Fatalf("expected exactly one matching decision record, got %+v", recs)
			}
		})
	}
}

func TestAllowStartsExactlyOneWorkflowWithRecordedDecision(t *testing.T) {
	s := newTestServer(t, nil)
	ms := s.store.(*decision.MemoryStore)
	var recordedBeforeStart bool
	s.starter.onStart = func(req contracts.ExecutionRequest) {
		for _, r := range ms.All() {
			if r.DecisionID == req.IngressDecisionID && r.Verdict == contracts.VerdictAllow {
				recordedBeforeStart = true
			}
		}
	}

	prop := contracts.AgentProposal{ID: "allow-1", Type: contracts.CmdModifyRouting, TargetID: "edge-node-west-1", Payload: `{"route":"stable"}`}
	raw, _ := json.Marshal(prop)
	rr := s.do(http.MethodPost, "/v1/intercept", keySim, string(raw))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	if !recordedBeforeStart {
		t.Fatal("ALLOW decision must be recorded before the workflow starts")
	}
	if s.starter.count() != 1 {
		t.Fatalf("expected exactly one workflow start, got %d", s.starter.count())
	}
	resp := decodeResp(t, rr)
	req, opts := s.starter.reqs[0], s.starter.opts[0]
	if opts.ID != "saga-allow-1" || opts.TaskQueue != "test-queue" || resp.WorkflowID != "saga-allow-1" || resp.RunID != "run-1" {
		t.Fatalf("unexpected start options %+v / response %+v", opts, resp)
	}
	if req.Proposal != prop || req.IngressDecisionID != resp.DecisionID || req.RequestHash != decision.RequestHash(prop) {
		t.Fatalf("unexpected workflow input: %+v", req)
	}
}

func TestRiskScoreAndUnknownFieldsRejected(t *testing.T) {
	for name, body := range map[string]string{
		"risk_score":    `{"id":"r-1","type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"{}","risk_score":0.01}`,
		"unknown field": `{"id":"r-2","type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"{}","trusted":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestServer(t, nil)
			rr := s.do(http.MethodPost, "/v1/intercept", keySim, body)
			if rr.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", rr.Code)
			}
			if s.starter.count() != 0 || len(s.records(t)) != 0 {
				t.Fatal("rejected request must not start a workflow or create a decision record")
			}
		})
	}
}

// --- Decision records ---

func TestDecisionRecordsCaptureIdentityPolicyAndHash(t *testing.T) {
	s := newTestServer(t, nil)
	cases := []struct {
		id      string
		key     string
		cmd     contracts.CommandType
		target  string
		verdict contracts.Verdict
		agent   string
		env     string
	}{
		{"rec-allow", keySim, contracts.CmdModifyRouting, "edge-node-west-1", contracts.VerdictAllow, "agent-sim", "staging"},
		{"rec-deny", keyLimited, contracts.CmdDeletePolicy, contracts.RootCoreEdgeID, contracts.VerdictDeny, "limited", "production"},
		{"rec-approval", keySim, contracts.CmdUpdateCert, "prod-payments-edge", contracts.VerdictRequireApproval, "agent-sim", "production"},
	}
	for _, tc := range cases {
		prop := contracts.AgentProposal{ID: tc.id, Type: tc.cmd, TargetID: tc.target, Payload: `{"n":1}`}
		raw, _ := json.Marshal(prop)
		s.do(http.MethodPost, "/v1/intercept", tc.key, string(raw))

		rr := s.do(http.MethodGet, "/v1/decisions/"+tc.id, keySim, "")
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: GET decisions status %d", tc.id, rr.Code)
		}
		var recs []contracts.DecisionRecord
		if err := json.Unmarshal(rr.Body.Bytes(), &recs); err != nil || len(recs) != 1 {
			t.Fatalf("%s: expected one record, got %s (err %v)", tc.id, rr.Body.String(), err)
		}
		r := recs[0]
		if r.Stage != contracts.StageIngress || r.Verdict != tc.verdict || r.AgentID != tc.agent ||
			r.Command != tc.cmd || r.TargetID != tc.target || r.Environment != tc.env ||
			r.PolicyVersion != "http-v1" || r.PolicyDigest != s.snap.Digest() ||
			r.RequestHash != decision.RequestHash(prop) || r.RecordedAt.IsZero() || len(r.Reasons) == 0 {
			t.Fatalf("%s: incomplete record %+v", tc.id, r)
		}
		wantWorkflow := ""
		if tc.verdict == contracts.VerdictAllow {
			wantWorkflow = "saga-" + tc.id
		}
		if r.WorkflowID != wantWorkflow {
			t.Fatalf("%s: workflow id = %q, want %q", tc.id, r.WorkflowID, wantWorkflow)
		}
	}
}

func TestRequestHashIndependentOfRequestFormatting(t *testing.T) {
	s := newTestServer(t, nil)
	compact := `{"id":"fmt-1","type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"{\"a\":1}"}`
	spaced := "{\n \"payload\": \"{\\\"a\\\":1}\",\n \"target_id\" : \"edge-node-west-1\",\n \"id\":\"fmt-1\",\n \"type\": \"MODIFY_ROUTING\"\n}\n"
	s.do(http.MethodPost, "/v1/intercept", keySim, compact)
	s.do(http.MethodPost, "/v1/intercept", keySim, spaced)
	recs := s.records(t)
	if len(recs) != 2 || recs[0].RequestHash != recs[1].RequestHash {
		t.Fatalf("hash differs across JSON formatting: %+v", recs)
	}
}

func TestAllowWithFailedDecisionWriteReturns503AndStartsNothing(t *testing.T) {
	s := newTestServer(t, &failingStore{})
	rr := s.do(http.MethodPost, "/v1/intercept", keySim, proposalJSON("fail-allow", contracts.CmdModifyRouting, "edge-node-west-1", `{}`))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if s.starter.count() != 0 {
		t.Fatal("workflow must not start when the ALLOW decision cannot be recorded")
	}
	if strings.Contains(rr.Body.String(), "database is locked") {
		t.Fatal("internal error leaked to client")
	}
}

func TestDenyWithFailedDecisionWriteStaysDenied(t *testing.T) {
	s := newTestServer(t, &failingStore{})
	rr := s.do(http.MethodPost, "/v1/intercept", keySim, proposalJSON("fail-deny", contracts.CmdDeletePolicy, contracts.RootCoreEdgeID, `{}`))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	resp := decodeResp(t, rr)
	if resp.Verdict != contracts.VerdictDeny || resp.DecisionID != "" {
		t.Fatalf("expected DENY without decision id, got %+v", resp)
	}
	if !strings.Contains(s.logs.String(), "decision record write failed") {
		t.Fatal("failed DENY write must be logged")
	}
}

func TestWorkflowStartFailureReturnsGeneric502(t *testing.T) {
	s := newTestServer(t, nil)
	s.starter.err = errors.New("rpc error: code = Unavailable desc = connection refused 10.0.0.7:7233")
	rr := s.do(http.MethodPost, "/v1/intercept", keySim, proposalJSON("wf-fail", contracts.CmdModifyRouting, "edge-node-west-1", `{}`))
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if strings.Contains(rr.Body.String(), "10.0.0.7") || strings.Contains(rr.Body.String(), "rpc error") {
		t.Fatalf("internal error leaked: %s", rr.Body.String())
	}
}

func TestDuplicateInFlightProposalIDReturns409NotExecuted(t *testing.T) {
	s := newTestServer(t, nil)
	s.starter.running = map[string]string{"saga-dup-1": "run-of-other-request"}

	rr := s.do(http.MethodPost, "/v1/intercept", keyLimited, proposalJSON("dup-1", contracts.CmdModifyRouting, "edge-node-west-1", `{"route":"b"}`))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
	resp := decodeResp(t, rr)
	if resp.Status != "not_executed" {
		t.Fatalf("status field = %q, want not_executed", resp.Status)
	}
	if resp.WorkflowID != "" || resp.RunID != "" || strings.Contains(rr.Body.String(), "run-of-other-request") {
		t.Fatalf("existing execution must not be reported as this request's run: %s", rr.Body.String())
	}
	if !strings.Contains(resp.Error, "already running") {
		t.Fatalf("error = %q, want an already-running explanation", resp.Error)
	}
	if s.starter.count() != 0 {
		t.Fatal("no new execution may be recorded as started")
	}
	recs := s.records(t)
	if len(recs) != 1 || recs[0].Verdict != contracts.VerdictAllow || recs[0].DecisionID != resp.DecisionID {
		t.Fatalf("expected the ingress ALLOW decision to remain recorded, got %+v", recs)
	}
	if !strings.Contains(s.logs.String(), "workflow already running for proposal id") {
		t.Fatalf("conflict not logged: %s", s.logs.String())
	}
}

// --- HTTP hardening ---

func TestInterceptRejectsMalformedRequests(t *testing.T) {
	valid := proposalJSON("ok-1", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)
	cases := []struct {
		name   string
		body   string
		status int
	}{
		{"oversized body", `{"id":"big-1","type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"` + strings.Repeat("a", maxBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
		{"trailing garbage", valid + ` trailing`, http.StatusBadRequest},
		{"second JSON object", valid + valid, http.StatusBadRequest},
		{"not JSON", `id=1`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
		{"empty id", proposalJSON("", contracts.CmdModifyRouting, "edge-node-west-1", `{}`), http.StatusBadRequest},
		{"id with spaces", proposalJSON("bad id", contracts.CmdModifyRouting, "edge-node-west-1", `{}`), http.StatusBadRequest},
		{"id with slash", proposalJSON("../etc", contracts.CmdModifyRouting, "edge-node-west-1", `{}`), http.StatusBadRequest},
		{"overlong id", proposalJSON(strings.Repeat("x", 129), contracts.CmdModifyRouting, "edge-node-west-1", `{}`), http.StatusBadRequest},
		{"wrong field type", `{"id":7,"type":"MODIFY_ROUTING","target_id":"edge-node-west-1","payload":"{}"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, nil)
			rr := s.do(http.MethodPost, "/v1/intercept", keySim, tc.body)
			if rr.Code != tc.status {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tc.status, rr.Body.String())
			}
			if s.starter.count() != 0 || len(s.records(t)) != 0 {
				t.Fatal("malformed requests must not start workflows or create decision records")
			}
			if !strings.Contains(s.logs.String(), `"agent_id":"agent-sim"`) {
				t.Fatalf("malformed request not logged with agent id: %s", s.logs.String())
			}
		})
	}
}

func TestTrailingWhitespaceIsAccepted(t *testing.T) {
	s := newTestServer(t, nil)
	rr := s.do(http.MethodPost, "/v1/intercept", keySim, proposalJSON("ws-1", contracts.CmdModifyRouting, "edge-node-west-1", `{}`)+"\n  \n")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestMethodAndPathValidation(t *testing.T) {
	s := newTestServer(t, nil)
	if rr := s.do(http.MethodGet, "/v1/intercept", keySim, ""); rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/intercept = %d, want 405", rr.Code)
	}
	if rr := s.do(http.MethodGet, "/v1/decisions/bad%20id", keySim, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid decisions id = %d, want 400", rr.Code)
	}
	if rr := s.do(http.MethodGet, "/v1/audit/bad%20id", keySim, ""); rr.Code != http.StatusBadRequest {
		t.Fatalf("invalid audit id = %d, want 400", rr.Code)
	}
	if rr := s.do(http.MethodGet, "/v1/decisions/unknown-1", keySim, ""); rr.Code != http.StatusOK || strings.TrimSpace(rr.Body.String()) != "[]" {
		t.Fatalf("unknown proposal should return empty list, got %d %s", rr.Code, rr.Body.String())
	}
	if rr := s.do(http.MethodGet, "/healthz", "", ""); rr.Code != http.StatusOK {
		t.Fatalf("healthz = %d", rr.Code)
	}
}

// The ingress record the handler writes must authorize exactly the workflow it
// starts, as the worker verifies it, and nothing else.
func TestIngressRecordAuthorizesOnlyItsOwnWorkflow(t *testing.T) {
	s := newTestServer(t, nil)
	rr := s.do(http.MethodPost, "/v1/intercept", keySim,
		proposalJSON("bind-1", contracts.CmdModifyRouting, "edge-node-west-1", `{"weight":10}`))
	if rr.Code != http.StatusOK || s.starter.count() != 1 {
		t.Fatalf("status = %d, started = %d", rr.Code, s.starter.count())
	}
	ctx := context.Background()
	req, workflowID := s.starter.reqs[0], s.starter.opts[0].ID

	if err := decision.CheckIngress(ctx, s.store, req, workflowID); err != nil {
		t.Fatalf("handler's own execution does not verify: %v", err)
	}

	otherAgent := req
	otherAgent.AgentID = "limited"
	if err := decision.CheckIngress(ctx, s.store, otherAgent, workflowID); !errors.Is(err, decision.ErrIngressAgentMismatch) {
		t.Fatalf("other agent: err = %v", err)
	}
	if err := decision.CheckIngress(ctx, s.store, req, "saga-other"); !errors.Is(err, decision.ErrIngressIdentityMismatch) {
		t.Fatalf("other workflow: err = %v", err)
	}

	// A denied proposal starts no workflow; its record cannot authorize one.
	s.do(http.MethodPost, "/v1/intercept", keySim,
		proposalJSON("bind-deny", contracts.CmdDeletePolicy, contracts.RootCoreEdgeID, `{}`))
	var denied contracts.DecisionRecord
	for _, r := range s.records(t) {
		if r.ProposalID == "bind-deny" {
			denied = r
		}
	}
	forged := contracts.ExecutionRequest{
		AgentID: "agent-sim",
		Proposal: contracts.AgentProposal{
			ID: "bind-deny", Type: contracts.CmdDeletePolicy, TargetID: contracts.RootCoreEdgeID, Payload: `{}`,
		},
		IngressDecisionID: denied.DecisionID,
		RequestHash:       denied.RequestHash,
	}
	if err := decision.CheckIngress(ctx, s.store, forged, "saga-bind-deny"); !errors.Is(err, decision.ErrIngressNotAllowed) {
		t.Fatalf("denied record: err = %v", err)
	}
}

func TestPolicyEndpointReturnsMetadataOnly(t *testing.T) {
	s := newTestServer(t, nil)
	rr := s.do(http.MethodGet, "/v1/policy", keySim, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"version", "digest", "loaded_at", "reload_status"} {
		if _, ok := got[k]; !ok {
			t.Fatalf("policy status missing %q: %v", k, got)
		}
	}
	for _, forbidden := range []string{"agents", "targets", "commands", "ROOT_CORE_EDGE", "agent-sim"} {
		if strings.Contains(rr.Body.String(), forbidden) {
			t.Fatalf("policy endpoint exposes %q: %s", forbidden, rr.Body.String())
		}
	}
}
