// Package evidence defines the files a validation test writes and reads them
// back. The report generator works from these files only.
package evidence

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Matrix statuses.
const (
	// StatusVerified: the test ran and every check it defines passed.
	StatusVerified = "VERIFIED"
	// StatusFailed: the test ran and at least one check failed.
	StatusFailed = "FAILED"
	// StatusBlocked: the test could not be carried out (environment or
	// harness problem); its checks say nothing about the system.
	StatusBlocked = "BLOCKED"
	// StatusNotTested: there is no evidence for the test.
	StatusNotTested = "NOT_TESTED"
	// StatusUnknown: the test ran but the evidence is insufficient to decide.
	StatusUnknown = "UNKNOWN"
	// StatusNotApplicable: the test does not apply to the system.
	StatusNotApplicable = "NOT_APPLICABLE"
)

// Check results.
const (
	CheckPass    = "PASS"
	CheckFail    = "FAIL"
	CheckUnknown = "UNKNOWN"
)

// Test kinds.
const (
	KindLegitimate = "legitimate"
	KindAttack     = "attack"
	KindFault      = "fault"
)

// SentryGate layer results.
const (
	SGBlocked       = "BLOCKED_EXECUTION"
	SGDidNotBlock   = "DID_NOT_BLOCK"
	SGExecuted      = "EXECUTED"
	SGNotExecuted   = "NOT_EXECUTED"
	SGUnknown       = "UNKNOWN"
	SGNotApplicable = "NOT_APPLICABLE"
)

// Infrastructure layer results.
const (
	InfraNotExecuted        = "NOT_EXECUTED"
	InfraExecutedNoMutation = "EXECUTED_NO_MUTATION"
	InfraMutated            = "MUTATED"
	InfraUnknown            = "UNKNOWN"
)

// Required files of a test's evidence directory.
var RequiredFiles = []string{
	"test-result.json", "request.json", "authorization.json",
	"sentrygate-decision.json", "execution.json", "infrastructure-state.json",
}

// CatalogueEntry names a test that the suite defines, whether or not it has
// evidence.
type CatalogueEntry struct {
	ID   string
	Name string
}

// Check is one expectation of a test and what was observed.
type Check struct {
	Name     string `json:"name"`
	Expected string `json:"expected"`
	Observed string `json:"observed"`
	Result   string `json:"result"`
}

// AttemptSummary is one request a test sent and how SentryGate handled it.
type AttemptSummary struct {
	Label            string `json:"label"`
	Role             string `json:"role"`
	Path             string `json:"path"`
	Action           string `json:"action"`
	AgentID          string `json:"agent_id"`
	Tool             string `json:"tool,omitempty"`
	Target           string `json:"target,omitempty"`
	SentryGate       string `json:"sentrygate_outcome"`
	SentryGateDetail string `json:"sentrygate_detail,omitempty"`
	F5Decision       string `json:"f5_authorization_decision,omitempty"`
	F5Source         string `json:"f5_observation_source,omitempty"`
}

// Result is test-result.json.
type Result struct {
	TestID               string           `json:"test_id"`
	TestName             string           `json:"test_name"`
	Kind                 string           `json:"kind"`
	Objective            string           `json:"objective"`
	Method               []string         `json:"method"`
	Expected             string           `json:"expected"`
	Status               string           `json:"status"`
	F5Result             string           `json:"f5_result"`
	F5Provider           string           `json:"f5_provider"`
	F5ObservationSource  string           `json:"f5_observation_source"`
	SentryGateResult     string           `json:"sentrygate_result"`
	InfrastructureResult string           `json:"infrastructure_result"`
	MutationCount        int              `json:"mutation_count"`
	ExpectedMutations    int              `json:"expected_mutation_count"`
	Invocations          int              `json:"tool_invocations"`
	Checks               []Check          `json:"checks"`
	Attempts             []AttemptSummary `json:"attempts"`
	Notes                []string         `json:"notes"`
	Limitations          []string         `json:"limitations"`
	BlockedReason        string           `json:"blocked_reason,omitempty"`
	Environment          map[string]any   `json:"environment"`
	StartedAt            string           `json:"started_at"`
	FinishedAt           string           `json:"finished_at"`
	EvidencePath         string           `json:"evidence_path"`
	Files                []string         `json:"files"`
}

// WriteJSON writes v as indented JSON with a trailing newline.
func WriteJSON(path string, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// Load reads every TCxx/test-result.json under root, sorted by test ID.
func Load(root string) ([]Result, error) {
	dirs, err := filepath.Glob(filepath.Join(root, "TC*"))
	if err != nil {
		return nil, err
	}
	var out []Result
	for _, d := range dirs {
		b, err := os.ReadFile(filepath.Join(d, "test-result.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("%s: %w", d, err)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TestID < out[j].TestID })
	return out, nil
}

// MissingFiles returns the required files absent from dir.
func MissingFiles(dir string) []string {
	var missing []string
	for _, f := range RequiredFiles {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			missing = append(missing, f)
		}
	}
	return missing
}

// Slash returns p with forward slashes, for paths written to evidence.
func Slash(p string) string { return strings.ReplaceAll(p, `\`, "/") }
