package workflows_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/workflows"
)

type mcpCall struct {
	name string
	args map[string]any
	meta map[string]any
}

// fakeMCP is an MCP server whose tool results come from respond. With drop
// set, it runs a tools/call to completion and then closes the connection
// without answering.
type fakeMCP struct {
	t       *testing.T
	respond func(name string, args map[string]any) (*mcp.CallToolResult, error)
	drop    bool

	mu    sync.Mutex
	calls []mcpCall
}

func newFakeMCP(t *testing.T, tools []string, respond func(string, map[string]any) (*mcp.CallToolResult, error)) (*fakeMCP, string) {
	t.Helper()
	f := &fakeMCP{t: t, respond: respond}
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "1"}, nil)
	for _, name := range tools {
		srv.AddTool(&mcp.Tool{Name: name, InputSchema: json.RawMessage(`{"type":"object"}`)}, f.handle)
	}
	h := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	ts := httptest.NewServer(f.wrap(h))
	t.Cleanup(ts.Close)
	return f, ts.URL
}

func (f *fakeMCP) handle(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := map[string]any{}
	if len(req.Params.Arguments) > 0 {
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			f.t.Errorf("arguments are not an object: %s", req.Params.Arguments)
		}
	}
	f.mu.Lock()
	f.calls = append(f.calls, mcpCall{name: req.Params.Name, args: args, meta: req.Params.GetMeta()})
	f.mu.Unlock()
	return f.respond(req.Params.Name, args)
}

func (f *fakeMCP) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if !f.drop || !bytes.Contains(body, []byte(`"tools/call"`)) {
			h.ServeHTTP(w, r)
			return
		}
		h.ServeHTTP(httptest.NewRecorder(), r)
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
	})
}

func (f *fakeMCP) recorded() []mcpCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mcpCall(nil), f.calls...)
}

func structured(v map[string]any, isError bool) (*mcp.CallToolResult, error) {
	return &mcp.CallToolResult{
		IsError:           isError,
		StructuredContent: v,
		Content:           []mcp.Content{&mcp.TextContent{Text: "result"}},
	}, nil
}

func mcpRequest(payload string) contracts.DispatchRequest {
	return contracts.DispatchRequest{
		IdempotencyKey: decision.ExecutionKey("dec_mcp"),
		AgentID:        "agent-a",
		Proposal: contracts.AgentProposal{
			ID: "prop-mcp", Type: "UPDATE_CUSTOMER", TargetID: "customer-123", Payload: payload,
		},
	}
}

func mcpAdapter(url string) *workflows.MCPAdapter {
	return &workflows.MCPAdapter{Endpoint: url, TargetArgument: "customer_id"}
}

func TestMCPAdapterDispatchSendsTheAuthorizedCall(t *testing.T) {
	f, url := newFakeMCP(t, []string{"update_customer"}, func(string, map[string]any) (*mcp.CallToolResult, error) {
		return structured(map[string]any{"status": "SUCCESS", "execution_id": "x000001"}, false)
	})
	req := mcpRequest(`{"email":"a@example.test"}`)

	out := mcpAdapter(url).Dispatch(context.Background(), req)

	if out.Status != contracts.OutcomeSuccess || !strings.Contains(out.Detail, "x000001") {
		t.Fatalf("outcome = %+v, want SUCCESS naming the execution", out)
	}
	calls := f.recorded()
	if len(calls) != 1 {
		t.Fatalf("tool calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.name != "update_customer" || c.args["customer_id"] != "customer-123" || c.args["email"] != "a@example.test" || len(c.args) != 2 {
		t.Fatalf("call = %s %v, want update_customer with the payload and the authorized target", c.name, c.args)
	}
	for key, want := range map[string]string{
		workflows.MCPMetaIdempotencyKey: req.IdempotencyKey,
		workflows.MCPMetaRequestHash:    decision.RequestHash(req.Proposal),
		workflows.MCPMetaAgentID:        "agent-a",
		workflows.MCPMetaProposalID:     "prop-mcp",
	} {
		if c.meta[key] != want {
			t.Errorf("_meta[%s] = %v, want %s", key, c.meta[key], want)
		}
	}
}

func TestMCPAdapterRefusesRequestsItCannotSendFaithfully(t *testing.T) {
	f, url := newFakeMCP(t, []string{"update_customer"}, func(string, map[string]any) (*mcp.CallToolResult, error) {
		return structured(map[string]any{"status": "SUCCESS"}, false)
	})
	cases := map[string]func(*contracts.DispatchRequest){
		"payload sets the target argument": func(r *contracts.DispatchRequest) {
			r.Proposal.Payload = `{"customer_id":"customer-456","email":"x@example.test"}`
		},
		"payload repeats the authorized target": func(r *contracts.DispatchRequest) {
			r.Proposal.Payload = `{"customer_id":"customer-123"}`
		},
		"payload is an array": func(r *contracts.DispatchRequest) { r.Proposal.Payload = `[1]` },
		"payload is null":     func(r *contracts.DispatchRequest) { r.Proposal.Payload = `null` },
		"payload is not JSON": func(r *contracts.DispatchRequest) { r.Proposal.Payload = `email=x` },
		"no idempotency key":  func(r *contracts.DispatchRequest) { r.IdempotencyKey = "" },
		"no target":           func(r *contracts.DispatchRequest) { r.Proposal.TargetID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			req := mcpRequest(`{"email":"a@example.test"}`)
			mutate(&req)
			out := mcpAdapter(url).Dispatch(context.Background(), req)
			if out.Status != contracts.OutcomeFailure || !strings.Contains(out.Detail, "not sent") {
				t.Fatalf("outcome = %+v, want FAILURE (not sent)", out)
			}
		})
	}
	if n := len(f.recorded()); n != 0 {
		t.Fatalf("tool calls = %d, want 0", n)
	}
}

func TestMCPAdapterResultMapping(t *testing.T) {
	cases := []struct {
		name    string
		respond func() (*mcp.CallToolResult, error)
		want    contracts.OutcomeStatus
	}{
		{"result without isError", func() (*mcp.CallToolResult, error) { return structured(map[string]any{}, false) }, contracts.OutcomeSuccess},
		{"tool error with confirmed FAILURE", func() (*mcp.CallToolResult, error) {
			return structured(map[string]any{"status": "FAILURE"}, true)
		}, contracts.OutcomeFailure},
		{"tool error without a status", func() (*mcp.CallToolResult, error) { return structured(map[string]any{}, true) }, contracts.OutcomeUnknown},
		{"tool error claiming SUCCESS", func() (*mcp.CallToolResult, error) {
			return structured(map[string]any{"status": "SUCCESS"}, true)
		}, contracts.OutcomeUnknown},
		{"protocol error", func() (*mcp.CallToolResult, error) { return nil, errors.New("server exploded") }, contracts.OutcomeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, url := newFakeMCP(t, []string{"update_customer"}, func(string, map[string]any) (*mcp.CallToolResult, error) { return tc.respond() })
			out := mcpAdapter(url).Dispatch(context.Background(), mcpRequest(`{}`))
			if out.Status != tc.want {
				t.Fatalf("outcome = %+v, want %s", out, tc.want)
			}
			if n := len(f.recorded()); n != 1 {
				t.Fatalf("tool calls = %d, want 1", n)
			}
		})
	}
}

// The server applies the call and the connection drops before the response.
// The adapter must report UNKNOWN and must not send the call again.
func TestMCPAdapterLostResponseIsUnknownAndNotResent(t *testing.T) {
	f, url := newFakeMCP(t, []string{"update_customer"}, func(string, map[string]any) (*mcp.CallToolResult, error) {
		return structured(map[string]any{"status": "SUCCESS"}, false)
	})
	f.drop = true

	out := mcpAdapter(url).Dispatch(context.Background(), mcpRequest(`{"email":"a@example.test"}`))

	if out.Status != contracts.OutcomeUnknown {
		t.Fatalf("outcome = %+v, want UNKNOWN", out)
	}
	if n := len(f.recorded()); n != 1 {
		t.Fatalf("tool calls = %d, want exactly 1 (no re-send)", n)
	}
}

func TestMCPAdapterUnreachableServerIsNotSent(t *testing.T) {
	ts := httptest.NewServer(http.NotFoundHandler())
	url := ts.URL
	ts.Close()

	out := mcpAdapter(url).Dispatch(context.Background(), mcpRequest(`{}`))

	if out.Status != contracts.OutcomeFailure || !strings.Contains(out.Detail, "not sent") {
		t.Fatalf("outcome = %+v, want FAILURE (not sent)", out)
	}
}

func TestMCPAdapterReconcile(t *testing.T) {
	cases := []struct {
		state string
		want  contracts.OutcomeStatus
	}{
		{"APPLIED", contracts.OutcomeSuccess},
		{"REJECTED", contracts.OutcomeFailure},
		{"FENCED", contracts.OutcomeFailure},
		{"SOMETHING_ELSE", contracts.OutcomeUnknown},
		{"", contracts.OutcomeUnknown},
	}
	for _, tc := range cases {
		t.Run("state "+tc.state, func(t *testing.T) {
			f, url := newFakeMCP(t, []string{workflows.DefaultMCPStatusTool}, func(string, map[string]any) (*mcp.CallToolResult, error) {
				return structured(map[string]any{"state": tc.state}, false)
			})
			req := mcpRequest(`{}`)
			out := mcpAdapter(url).Reconcile(context.Background(), req)
			if out.Status != tc.want {
				t.Fatalf("outcome = %+v, want %s", out, tc.want)
			}
			calls := f.recorded()
			if len(calls) != 1 || calls[0].args["idempotency_key"] != req.IdempotencyKey || calls[0].args["fence"] != true {
				t.Fatalf("status calls = %+v, want one call with the key and fence=true", calls)
			}
		})
	}

	t.Run("target without the status tool", func(t *testing.T) {
		_, url := newFakeMCP(t, []string{"update_customer"}, func(string, map[string]any) (*mcp.CallToolResult, error) {
			return structured(map[string]any{"status": "SUCCESS"}, false)
		})
		if out := mcpAdapter(url).Reconcile(context.Background(), mcpRequest(`{}`)); out.Status != contracts.OutcomeUnknown {
			t.Fatalf("outcome = %+v, want UNKNOWN", out)
		}
	})
	t.Run("status tool error", func(t *testing.T) {
		_, url := newFakeMCP(t, []string{workflows.DefaultMCPStatusTool}, func(string, map[string]any) (*mcp.CallToolResult, error) {
			return structured(map[string]any{"state": "APPLIED"}, true)
		})
		if out := mcpAdapter(url).Reconcile(context.Background(), mcpRequest(`{}`)); out.Status != contracts.OutcomeUnknown {
			t.Fatalf("outcome = %+v, want UNKNOWN", out)
		}
	})
}

func TestMCPAdapterDoesNotCompensate(t *testing.T) {
	if err := mcpAdapter("http://127.0.0.1:1").Compensate(context.Background(), mcpRequest(`{}`)); err == nil {
		t.Fatal("Compensate returned nil, want an error")
	}
}
