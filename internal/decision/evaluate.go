// Package decision evaluates agent proposals against a policy snapshot and
// persists the resulting decision records.
package decision

import (
	"crypto/rand"

	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

// Evaluate applies the policy to a proposal made by agentID. It is pure and
// deterministic for a given snapshot.
//
// Checks run in a fixed order: unknown agent, unknown command, command not
// permitted for the agent, unregistered target, protected target, environment
// rule. Every applicable deny reason is collected. The environment rule only
// decides the verdict when no deny reason applies. Precedence is
// DENY > REQUIRE_APPROVAL > ALLOW, and Reasons lists only the codes at the
// winning level.
func Evaluate(s *policy.Snapshot, agentID string, p contracts.AgentProposal) contracts.Decision {
	if s == nil {
		return contracts.Decision{
			Verdict: contracts.VerdictDeny,
			Reasons: []contracts.ReasonCode{contracts.ReasonPolicyUnavailable},
		}
	}

	d := contracts.Decision{PolicyVersion: s.Version(), PolicyDigest: s.Digest()}
	var deny []contracts.ReasonCode

	agentKnown := s.HasAgent(agentID)
	if !agentKnown {
		deny = append(deny, contracts.ReasonAgentUnknown)
	}
	commandKnown := s.HasCommand(p.Type)
	if !commandKnown {
		deny = append(deny, contracts.ReasonCommandUnknown)
	}
	if agentKnown && commandKnown && !s.AgentMayUse(agentID, p.Type) {
		deny = append(deny, contracts.ReasonCommandNotPermitted)
	}

	target, targetKnown := s.Target(p.TargetID)
	if !targetKnown {
		deny = append(deny, contracts.ReasonTargetUnregistered)
	} else {
		d.Environment = target.Environment
		if target.Protected {
			deny = append(deny, contracts.ReasonTargetProtected)
		}
	}

	var mode policy.EnvironmentMode
	if targetKnown {
		mode, _ = s.EnvironmentMode(target.Environment)
		if mode == policy.ModeDeny {
			deny = append(deny, contracts.ReasonEnvironmentDenied)
		}
	}

	switch {
	case len(deny) > 0:
		d.Verdict = contracts.VerdictDeny
		d.Reasons = deny
	case mode == policy.ModeRequireApproval:
		d.Verdict = contracts.VerdictRequireApproval
		d.Reasons = []contracts.ReasonCode{contracts.ReasonApprovalRequired}
	case mode == policy.ModeAllow:
		d.Verdict = contracts.VerdictAllow
		d.Reasons = []contracts.ReasonCode{contracts.ReasonEnvironmentAllowed}
	default:
		// Unreachable for a validated snapshot; fail closed regardless.
		d.Verdict = contracts.VerdictDeny
		d.Reasons = []contracts.ReasonCode{contracts.ReasonPolicyUnavailable}
	}
	return d
}

// Revalidate re-evaluates an execution request inside the workflow boundary.
// It also verifies that the proposal still matches the hash recorded at
// ingress; a mismatch forces DENY.
func Revalidate(s *policy.Snapshot, req contracts.ExecutionRequest) contracts.Decision {
	d := Evaluate(s, req.AgentID, req.Proposal)
	if RequestHash(req.Proposal) != req.RequestHash {
		if d.Verdict != contracts.VerdictDeny {
			d.Reasons = nil
		}
		d.Verdict = contracts.VerdictDeny
		d.Reasons = append(d.Reasons, contracts.ReasonRequestHashMismatch)
	}
	return d
}

// NewDecisionID returns a random, URL-safe decision identifier.
func NewDecisionID() string {
	return "dec_" + rand.Text()
}
