package mcpserver_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/validation/mcpserver"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

func start(t *testing.T) (*mcpserver.Server, string) {
	t.Helper()
	srv, err := mcpserver.New(mcpserver.Config{
		DBPath:              filepath.Join(t.TempDir(), "mcp.db"),
		TargetArgument:      "customer_id",
		LostResponseTargets: []string{"customer-lost-response"},
		GatedTargets:        []string{"customer-gated"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { ts.Close(); _ = srv.Close() })
	return srv, ts.URL + "/mcp"
}

func session(t *testing.T, url string) *mcp.ClientSession {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	s, err := c.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: url, MaxRetries: -1, DisableStandaloneSSE: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func snapshot(t *testing.T, srv *mcpserver.Server) mcpserver.State {
	t.Helper()
	st, err := srv.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func customer(st mcpserver.State, id string) mcpserver.Customer {
	for _, c := range st.Customers {
		if c.ID == id {
			return c
		}
	}
	return mcpserver.Customer{}
}

func TestToolsRecordEveryCall(t *testing.T) {
	srv, url := start(t)
	s := session(t, url)
	ctx := context.Background()
	meta := mcp.Meta{"io.sentrygate/agent_id": "agent-a", "io.sentrygate/idempotency_key": "k1"}

	calls := []struct {
		tool      string
		args      map[string]any
		isError   bool
		mutations int
	}{
		{mcpserver.ToolRead, map[string]any{"customer_id": "customer-123"}, false, 0},
		{mcpserver.ToolUpdate, map[string]any{"customer_id": "customer-123", "email": "new@example.test"}, false, 1},
		{mcpserver.ToolUpdate, map[string]any{"customer_id": "customer-123", "password": "x"}, true, 0},
		{mcpserver.ToolExport, map[string]any{"customer_id": "customer-456"}, false, 1},
		{mcpserver.ToolDelete, map[string]any{"customer_id": "customer-456"}, false, 1},
		{mcpserver.ToolRead, map[string]any{"customer_id": "customer-456"}, true, 0},
		{mcpserver.ToolRead, map[string]any{}, true, 0},
	}
	for i, c := range calls {
		res, err := s.CallTool(ctx, &mcp.CallToolParams{Name: c.tool, Arguments: c.args, Meta: meta})
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if res.IsError != c.isError {
			t.Fatalf("call %d %s: isError = %v, want %v", i, c.tool, res.IsError, c.isError)
		}
	}

	st := snapshot(t, srv)
	if len(st.Executions) != len(calls) {
		t.Fatalf("executions = %d, want %d", len(st.Executions), len(calls))
	}
	for i, c := range calls {
		e := st.Executions[i]
		if e.Tool != c.tool || e.MutationCount != c.mutations || e.AgentID != "agent-a" || e.IdempotencyKey != "k1" || e.RequestHash == "" {
			t.Errorf("execution %d = %+v, want tool %s with %d mutations recorded for agent-a/k1", i, e, c.tool, c.mutations)
		}
	}
	if got := customer(st, "customer-123").Email; got != "new@example.test" {
		t.Fatalf("customer-123 email = %q", got)
	}
	if !customer(st, "customer-456").Deleted || len(st.Exports) != 1 {
		t.Fatalf("customer-456 = %+v, exports = %d; want deleted and one export", customer(st, "customer-456"), len(st.Exports))
	}
	if c := st.Count(); c.Mutations != 3 || c.Invocations != 7 {
		t.Fatalf("counts = %+v, want 7 invocations and 3 mutations", c)
	}
}

// The server must not hide a duplicate delivery behind the idempotency key.
func TestDuplicateDeliveryIsAppliedAndCountedTwice(t *testing.T) {
	srv, url := start(t)
	s := session(t, url)
	for range 2 {
		if _, err := s.CallTool(context.Background(), &mcp.CallToolParams{
			Name:      mcpserver.ToolUpdate,
			Arguments: map[string]any{"customer_id": "customer-123", "tier": "gold"},
			Meta:      mcp.Meta{"io.sentrygate/idempotency_key": "same-key"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if c := snapshot(t, srv).Count(); c.ByKey["same-key"] != 2 || c.Mutations != 2 {
		t.Fatalf("counts = %+v, want the key applied twice", c)
	}
}

func adapter(url string) *workflows.MCPAdapter {
	return &workflows.MCPAdapter{Endpoint: url, TargetArgument: "customer_id"}
}

func dispatchRequest(decisionID, target, payload string) contracts.DispatchRequest {
	return contracts.DispatchRequest{
		IdempotencyKey: decision.ExecutionKey(decisionID),
		AgentID:        "agent-a",
		Proposal:       contracts.AgentProposal{ID: "p-" + decisionID, Type: "UPDATE_CUSTOMER", TargetID: target, Payload: payload},
	}
}

func TestSentryGateAdapterAgainstTestServer(t *testing.T) {
	srv, url := start(t)
	ctx := context.Background()
	a := adapter(url)

	ok := dispatchRequest("dec_ok", "customer-123", `{"email":"a@example.test"}`)
	if out := a.Dispatch(ctx, ok); out.Status != contracts.OutcomeSuccess {
		t.Fatalf("dispatch = %+v, want SUCCESS", out)
	}
	st := snapshot(t, srv)
	e := st.Executions[0]
	if e.SentryGateRequestHash != decision.RequestHash(ok.Proposal) || e.ProposalID != "p-dec_ok" || e.Target != "customer-123" {
		t.Fatalf("execution = %+v, want SentryGate hash, proposal and target recorded", e)
	}

	// Lost response: applied once, adapter reports UNKNOWN, reconciliation
	// confirms SUCCESS without applying again.
	lost := dispatchRequest("dec_lost", "customer-lost-response", `{"email":"b@example.test"}`)
	if out := a.Dispatch(ctx, lost); out.Status != contracts.OutcomeUnknown {
		t.Fatalf("lost-response dispatch = %+v, want UNKNOWN", out)
	}
	if out := a.Reconcile(ctx, lost); out.Status != contracts.OutcomeSuccess {
		t.Fatalf("reconcile = %+v, want SUCCESS", out)
	}
	st = snapshot(t, srv)
	if c := st.Count(); c.ByKey[lost.IdempotencyKey] != 1 || c.MutByTarget["customer-lost-response"] != 1 {
		t.Fatalf("counts = %+v, want the lost-response key applied exactly once", c)
	}
	if len(st.TransportEvents) == 0 || st.TransportEvents[0].Event != "RESPONSE_DROPPED" {
		t.Fatalf("transport events = %+v, want RESPONSE_DROPPED", st.TransportEvents)
	}

	// Never delivered: reconciliation fences the key, so a late delivery is refused.
	late := dispatchRequest("dec_late", "customer-456", `{"tier":"gold"}`)
	if out := a.Reconcile(ctx, late); out.Status != contracts.OutcomeFailure {
		t.Fatalf("reconcile of an unseen key = %+v, want FAILURE (fenced)", out)
	}
	if out := a.Dispatch(ctx, late); out.Status != contracts.OutcomeFailure {
		t.Fatalf("late dispatch = %+v, want FAILURE", out)
	}
	st = snapshot(t, srv)
	if c := st.Count(); c.MutByTarget["customer-456"] != 0 {
		t.Fatalf("counts = %+v, want no mutation on customer-456 after fencing", c)
	}
}

func TestGateHoldsCallsUntilReleased(t *testing.T) {
	srv, url := start(t)
	a := adapter(url)
	done := make(chan contracts.DispatchOutcome, 1)
	go func() {
		done <- a.Dispatch(context.Background(), dispatchRequest("dec_gate", "customer-gated", `{"tier":"gold"}`))
	}()

	deadline := time.Now().Add(5 * time.Second)
	for snapshot(t, srv).Count().Invocations != 0 || !waiting(srv) {
		if time.Now().After(deadline) {
			t.Fatal("call never reached the gate")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case out := <-done:
		t.Fatalf("call completed before release: %+v", out)
	case <-time.After(200 * time.Millisecond):
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest("POST", "/gate/release", nil))
	if out := <-done; out.Status != contracts.OutcomeSuccess {
		t.Fatalf("gated dispatch = %+v, want SUCCESS", out)
	}
	if c := snapshot(t, srv).Count(); c.Mutations != 1 {
		t.Fatalf("counts = %+v, want one mutation", c)
	}
}

func waiting(srv *mcpserver.Server) bool {
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/gate", nil))
	return rec.Body.String() == "{\"waiting\":1}\n"
}
