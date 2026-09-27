package contracts

// AgentProposal is the JSON body an agent submits to POST /v1/intercept.
// It deliberately has no risk or trust fields: every property that affects
// the verdict is derived server-side from the policy and the authenticated
// agent identity. Unknown JSON fields are rejected at ingress.
type AgentProposal struct {
	ID       string      `json:"id"`
	Type     CommandType `json:"type"`
	TargetID string      `json:"target_id"`
	Payload  string      `json:"payload"`
}
