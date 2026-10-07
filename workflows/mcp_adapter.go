package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// _meta keys the MCP adapter attaches to every tools/call it sends.
const (
	MCPMetaIdempotencyKey = "io.sentrygate/idempotency_key"
	MCPMetaRequestHash    = "io.sentrygate/request_hash"
	MCPMetaAgentID        = "io.sentrygate/agent_id"
	MCPMetaProposalID     = "io.sentrygate/proposal_id"
)

// DefaultMCPStatusTool is the tool MCPAdapter.Reconcile calls when StatusTool
// is empty.
const DefaultMCPStatusTool = "sentrygate_execution_status"

// MCPAdapter dispatches a proposal as one MCP tools/call over Streamable HTTP.
//
// The tool name is the proposal's command in lower case (READ_CUSTOMER calls
// read_customer). The arguments are the proposal's payload, which must be a
// JSON object, plus TargetArgument set to the proposal's target ID. The
// command and target are what policy evaluated and what the request hash
// covers, so a payload that sets TargetArgument itself is refused without
// sending anything. The idempotency key, request hash, agent ID and proposal ID
// travel in _meta.
//
// Outcomes:
//   - The call was not sent (invalid payload, no connection): FAILURE.
//   - The server returned a result without isError: SUCCESS.
//   - The server returned isError with structured status FAILURE: FAILURE.
//   - Anything else, including a transport error after the request may have
//     been sent and a protocol error: UNKNOWN.
//
// The client never re-sends a tools/call: transport reconnection is disabled
// and the request carries no idempotency header that would let net/http retry
// it.
//
// Reconcile calls StatusTool with the idempotency key and fence=true. The
// target must answer from its own records: APPLIED gives SUCCESS; REJECTED
// (received but not applied) or FENCED (never received, and now refused if it
// arrives) give FAILURE; anything else, or a target without the tool, gives
// UNKNOWN. Compensate is not supported.
type MCPAdapter struct {
	Endpoint       string
	TargetArgument string
	StatusTool     string
	HTTPClient     *http.Client
}

// ToolForCommand returns the MCP tool name MCPAdapter calls for cmd.
func ToolForCommand(cmd contracts.CommandType) string {
	return strings.ToLower(string(cmd))
}

func (a *MCPAdapter) Dispatch(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	args, err := a.arguments(req)
	if err != nil {
		return notSent(err.Error())
	}
	session, err := a.connect(ctx)
	if err != nil {
		return notSent("could not connect to the MCP server: " + err.Error())
	}
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      ToolForCommand(req.Proposal.Type),
		Arguments: args,
		Meta: mcp.Meta{
			MCPMetaIdempotencyKey: req.IdempotencyKey,
			MCPMetaRequestHash:    decision.RequestHash(req.Proposal),
			MCPMetaAgentID:        req.AgentID,
			MCPMetaProposalID:     req.Proposal.ID,
		},
	})
	if err != nil {
		return contracts.UnknownOutcome("mcp: tools/call may have been sent; no usable response: " + err.Error())
	}
	status := structuredString(res, "status")
	detail := fmt.Sprintf("mcp: tool=%s status=%s execution_id=%s", ToolForCommand(req.Proposal.Type), status, structuredString(res, "execution_id"))
	switch {
	case !res.IsError:
		return contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, Detail: detail}
	case status == string(contracts.OutcomeFailure):
		return contracts.DispatchOutcome{Status: contracts.OutcomeFailure, Detail: detail + " " + resultText(res)}
	}
	return contracts.UnknownOutcome(detail + " tool error without a confirmed status: " + resultText(res))
}

func (a *MCPAdapter) Reconcile(ctx context.Context, req contracts.DispatchRequest) contracts.DispatchOutcome {
	tool := a.StatusTool
	if tool == "" {
		tool = DefaultMCPStatusTool
	}
	if req.IdempotencyKey == "" {
		return contracts.UnknownOutcome("mcp: cannot reconcile without an idempotency key")
	}
	session, err := a.connect(ctx)
	if err != nil {
		return contracts.UnknownOutcome("mcp: reconciliation could not connect: " + err.Error())
	}
	defer session.Close()

	res, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      tool,
		Arguments: map[string]any{"idempotency_key": req.IdempotencyKey, "fence": true},
	})
	if err != nil {
		return contracts.UnknownOutcome("mcp: reconciliation got no usable response: " + err.Error())
	}
	if res.IsError {
		return contracts.UnknownOutcome("mcp: reconciliation tool error: " + resultText(res))
	}
	state := structuredString(res, "state")
	detail := fmt.Sprintf("mcp: reconciled key state=%s", state)
	switch state {
	case "APPLIED":
		return contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, Detail: detail}
	case "REJECTED", "FENCED":
		return contracts.DispatchOutcome{Status: contracts.OutcomeFailure, Detail: detail}
	}
	return contracts.UnknownOutcome(detail)
}

func (a *MCPAdapter) Compensate(context.Context, contracts.DispatchRequest) error {
	return errors.New("mcp adapter does not support compensation")
}

func (a *MCPAdapter) arguments(req contracts.DispatchRequest) (map[string]any, error) {
	switch {
	case a.Endpoint == "":
		return nil, errors.New("no MCP endpoint configured")
	case a.TargetArgument == "":
		return nil, errors.New("no MCP target argument configured")
	case req.IdempotencyKey == "":
		return nil, errors.New("request has no idempotency key")
	case req.Proposal.TargetID == "":
		return nil, errors.New("proposal has no target")
	}
	args := map[string]any{}
	if req.Proposal.Payload != "" {
		if err := json.Unmarshal([]byte(req.Proposal.Payload), &args); err != nil || args == nil {
			return nil, errors.New("payload is not a JSON object")
		}
	}
	if _, ok := args[a.TargetArgument]; ok {
		return nil, fmt.Errorf("payload sets the target argument %q; the target comes only from the authorized proposal", a.TargetArgument)
	}
	args[a.TargetArgument] = req.Proposal.TargetID
	return args, nil
}

func (a *MCPAdapter) connect(ctx context.Context) (*mcp.ClientSession, error) {
	client := mcp.NewClient(&mcp.Implementation{Name: "sentrygate-worker", Version: "1"}, nil)
	return client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             a.Endpoint,
		HTTPClient:           a.HTTPClient,
		MaxRetries:           -1,
		DisableStandaloneSSE: true,
	}, nil)
}

func notSent(detail string) contracts.DispatchOutcome {
	return contracts.DispatchOutcome{Status: contracts.OutcomeFailure, Detail: "mcp: not sent: " + detail}
}

func structuredString(res *mcp.CallToolResult, field string) string {
	m, ok := res.StructuredContent.(map[string]any)
	if !ok {
		return ""
	}
	s, _ := m[field].(string)
	return s
}

func resultText(res *mcp.CallToolResult) string {
	var parts []string
	for _, c := range res.Content {
		if t, ok := c.(*mcp.TextContent); ok {
			parts = append(parts, t.Text)
		}
	}
	return strings.Join(parts, " ")
}
