// Package agent is the deterministic test agent. It makes no decisions of its
// own: every request it sends is spelled out by a test case. It reaches the
// system in two ways:
//
//   - Gateway: HTTP requests to SentryGate's ingress (POST /v1/intercept),
//     the path an agent is expected to use and the one a gateway such as F5
//     would sit on.
//   - Boundary: direct access to Temporal, bypassing ingress. This models an
//     attacker who can start, terminate or reset workflows, and is used to
//     present modified, replayed or forged authorizations to the execution
//     boundary.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/battujeevan/SentryGate-AI/internal/auth"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Identity is an agent and its API key. The key is never serialized.
type Identity struct {
	AgentID string `json:"agent_id"`
	Key     string `json:"-"`
}

// Authorize sets the agent's API key on req.
func Authorize(req *http.Request, id Identity) {
	if id.Key != "" {
		req.Header.Set(auth.HeaderAPIKey, id.Key)
	}
}

// ToolCall is an MCP tool call expressed as a SentryGate proposal: the tool
// name is the lower-case command, the target is the proposal target and the
// arguments are the payload.
type ToolCall struct {
	ProposalID string
	Tool       string
	Target     string
	Arguments  map[string]any
}

// Proposal returns the SentryGate proposal for the call. The payload is the
// arguments as JSON with sorted keys, so the same call always has the same
// request hash.
func (c ToolCall) Proposal() contracts.AgentProposal {
	args := c.Arguments
	if args == nil {
		args = map[string]any{}
	}
	payload, err := json.Marshal(args)
	if err != nil {
		panic(fmt.Sprintf("tool call arguments are not JSON: %v", err))
	}
	return contracts.AgentProposal{
		ID:       c.ProposalID,
		Type:     contracts.CommandType(strings.ToUpper(c.Tool)),
		TargetID: c.Target,
		Payload:  string(payload),
	}
}

// IngressResponse is the proxy's response to POST /v1/intercept.
type IngressResponse struct {
	Status     string   `json:"status"`
	Verdict    string   `json:"verdict,omitempty"`
	Reasons    []string `json:"reasons,omitempty"`
	DecisionID string   `json:"decision_id,omitempty"`
	ProposalID string   `json:"proposal_id,omitempty"`
	WorkflowID string   `json:"workflow_id,omitempty"`
	RunID      string   `json:"run_id,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// Paths a request can take.
const (
	PathGateway  = "gateway"
	PathBoundary = "execution-boundary"
)

// Roles a request can have in a test.
const (
	RoleLegitimate = "legitimate"
	RoleAttack     = "attack"
)

// Request is the record of one request the agent sent. It is written to
// request.json and never contains an API key.
type Request struct {
	Label      string                      `json:"label"`
	Role       string                      `json:"role"`
	Path       string                      `json:"path"`
	Action     string                      `json:"action"`
	AgentID    string                      `json:"agent_id"`
	Proposal   *contracts.AgentProposal    `json:"proposal,omitempty"`
	RawBody    string                      `json:"raw_body,omitempty"`
	Execution  *contracts.ExecutionRequest `json:"execution_request,omitempty"`
	WorkflowID string                      `json:"workflow_id,omitempty"`
	RunID      string                      `json:"run_id,omitempty"`
	SentAt     time.Time                   `json:"sent_at"`
	HTTPStatus int                         `json:"http_status,omitempty"`
	Response   *IngressResponse            `json:"response,omitempty"`
	Error      string                      `json:"error,omitempty"`
}

// Gateway sends ingress requests.
type Gateway struct {
	BaseURL string
	HTTP    *http.Client
}

// Submit sends prop to ingress as id.
func (g *Gateway) Submit(ctx context.Context, label, role string, id Identity, prop contracts.AgentProposal) Request {
	body, _ := json.Marshal(prop)
	r := g.send(ctx, label, role, id, body)
	r.Proposal = &prop
	return r
}

// SubmitRaw sends body to ingress unchanged as id.
func (g *Gateway) SubmitRaw(ctx context.Context, label, role string, id Identity, body string) Request {
	r := g.send(ctx, label, role, id, []byte(body))
	r.RawBody = body
	return r
}

func (g *Gateway) send(ctx context.Context, label, role string, id Identity, body []byte) Request {
	r := Request{Label: label, Role: role, Path: PathGateway, Action: "POST /v1/intercept", AgentID: id.AgentID, SentAt: time.Now().UTC()}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.BaseURL+"/v1/intercept", bytes.NewReader(body))
	if err != nil {
		r.Error = err.Error()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	Authorize(req, id)
	resp, err := g.HTTP.Do(req)
	if err != nil {
		r.Error = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.HTTPStatus = resp.StatusCode
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var ir IngressResponse
	if err := json.Unmarshal(raw, &ir); err != nil {
		r.Error = fmt.Sprintf("response is not JSON: %q", strings.TrimSpace(string(raw)))
		return r
	}
	r.Response = &ir
	r.WorkflowID, r.RunID = ir.WorkflowID, ir.RunID
	return r
}

// Concurrently runs f(0..n-1) in n goroutines released at the same moment
// and waits for all of them.
func Concurrently(n int, f func(i int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			f(i)
		}()
	}
	close(start)
	wg.Wait()
}
