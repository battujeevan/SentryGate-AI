package contracts

// AgentProposal maps the incoming JSON structure sent from an LLM tool-calling node.
type AgentProposal struct {
	ID        string      `json:"id"`
	Type      CommandType `json:"type"`
	TargetID  string      `json:"target_id"`
	Payload   string      `json:"payload"`
	RiskScore float64     `json:"risk_score"`
}

// PolicyConfig holds compliance-tunable safety boundaries that can be
// inspected and versioned independently of workflow binaries.
type PolicyConfig struct {
	// MaxRiskCeiling is the inclusive upper bound for AgentProposal.RiskScore.
	MaxRiskCeiling float64 `json:"max_risk_ceiling"`

	// MaxParallelTasks bounds the outbound worker-pool concurrency.
	MaxParallelTasks int `json:"max_parallel_tasks"`

	// ProtectedTargets lists TargetIDs that must never be mutated autonomously.
	ProtectedTargets []string `json:"protected_targets"`
}

// DefaultPolicyConfig returns the enterprise baseline safety contract.
func DefaultPolicyConfig() PolicyConfig {
	return PolicyConfig{
		MaxRiskCeiling:   0.75,
		MaxParallelTasks: 8,
		ProtectedTargets: []string{RootCoreEdgeID},
	}
}
