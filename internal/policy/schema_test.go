package policy_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/policy"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

const validPolicy = `version: "2026-09-27.1"
max_parallel_tasks: 4
environments:
  staging: allow
  production: require_approval
  lab: deny
commands:
  - MODIFY_ROUTING
  - UPDATE_CERTIFICATE
  - DELETE_POLICY
agents:
  - id: agent-a
    commands: [MODIFY_ROUTING, UPDATE_CERTIFICATE]
targets:
  - id: ROOT_CORE_EDGE
    environment: production
    protected: true
  - id: edge-1
    environment: staging
    protected: false
`

func TestParseValidPolicy(t *testing.T) {
	s, err := policy.Parse([]byte(validPolicy))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.Version() != "2026-09-27.1" || s.MaxParallelTasks() != 4 {
		t.Fatalf("unexpected header: version=%q max=%d", s.Version(), s.MaxParallelTasks())
	}
	sum := sha256.Sum256([]byte(validPolicy))
	if s.Digest() != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest mismatch: %s", s.Digest())
	}
	if !s.HasCommand(contracts.CmdDeletePolicy) || s.HasCommand("REBOOT") {
		t.Fatal("command catalogue lookup wrong")
	}
	if !s.AgentMayUse("agent-a", contracts.CmdModifyRouting) || s.AgentMayUse("agent-a", contracts.CmdDeletePolicy) {
		t.Fatal("agent permission lookup wrong")
	}
	tgt, ok := s.Target(contracts.RootCoreEdgeID)
	if !ok || !tgt.Protected || tgt.Environment != "production" {
		t.Fatalf("unexpected target: %+v ok=%v", tgt, ok)
	}
	if m, ok := s.EnvironmentMode("lab"); !ok || m != policy.ModeDeny {
		t.Fatalf("unexpected mode %q", m)
	}
}

func TestParseRejectsInvalidPolicies(t *testing.T) {
	cases := map[string]string{
		"empty file":               "",
		"unknown top-level field":  validPolicy + "max_risk_ceiling: 0.75\n",
		"unknown nested field":     strings.Replace(validPolicy, "    protected: false\n", "    protected: false\n    owner: team-x\n", 1),
		"second document":          validPolicy + "---\n" + validPolicy,
		"duplicate key":            validPolicy + "version: other\n",
		"missing version":          strings.Replace(validPolicy, "version: \"2026-09-27.1\"\n", "", 1),
		"invalid version":          strings.Replace(validPolicy, `"2026-09-27.1"`, `"has spaces"`, 1),
		"missing max parallel":     strings.Replace(validPolicy, "max_parallel_tasks: 4\n", "", 1),
		"zero max parallel":        strings.Replace(validPolicy, "max_parallel_tasks: 4", "max_parallel_tasks: 0", 1),
		"huge max parallel":        strings.Replace(validPolicy, "max_parallel_tasks: 4", "max_parallel_tasks: 10001", 1),
		"invalid env mode":         strings.Replace(validPolicy, "lab: deny", "lab: maybe", 1),
		"invalid env name":         strings.Replace(validPolicy, "lab: deny", "Lab Env: deny", 1),
		"no commands":              strings.Replace(validPolicy, "  - MODIFY_ROUTING\n  - UPDATE_CERTIFICATE\n  - DELETE_POLICY\n", "", 1),
		"invalid command name":     strings.Replace(validPolicy, "  - DELETE_POLICY\n", "  - delete-policy\n", 1),
		"duplicate command":        strings.Replace(validPolicy, "  - DELETE_POLICY\n", "  - DELETE_POLICY\n  - DELETE_POLICY\n", 1),
		"agent unknown command":    strings.Replace(validPolicy, "[MODIFY_ROUTING, UPDATE_CERTIFICATE]", "[MODIFY_ROUTING, REBOOT]", 1),
		"agent duplicate command":  strings.Replace(validPolicy, "[MODIFY_ROUTING, UPDATE_CERTIFICATE]", "[MODIFY_ROUTING, MODIFY_ROUTING]", 1),
		"agent empty commands":     strings.Replace(validPolicy, "[MODIFY_ROUTING, UPDATE_CERTIFICATE]", "[]", 1),
		"duplicate agent":          strings.Replace(validPolicy, "targets:\n", "  - id: agent-a\n    commands: [MODIFY_ROUTING]\ntargets:\n", 1),
		"invalid agent id":         strings.Replace(validPolicy, "id: agent-a", "id: \"agent a\"", 1),
		"target undeclared env":    strings.Replace(validPolicy, "environment: staging", "environment: qa", 1),
		"target missing protected": strings.Replace(validPolicy, "    protected: false\n", "", 1),
		"duplicate target":         validPolicy + "  - id: edge-1\n    environment: staging\n    protected: false\n",
		"invalid target id":        strings.Replace(validPolicy, "id: edge-1", "id: \"edge 1\"", 1),
		"wrong type":               strings.Replace(validPolicy, "max_parallel_tasks: 4", "max_parallel_tasks: four", 1),
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := policy.Parse([]byte(doc)); err == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestShippedDefaultPolicyIsValid(t *testing.T) {
	raw, err := os.ReadFile("../../policies/default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, err := policy.Parse(raw)
	if err != nil {
		t.Fatalf("policies/default.yaml is invalid: %v", err)
	}
	if tgt, ok := s.Target(contracts.RootCoreEdgeID); !ok || !tgt.Protected {
		t.Fatal("default policy must register ROOT_CORE_EDGE as protected")
	}
}
