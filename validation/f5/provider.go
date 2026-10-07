// Package f5 is the boundary between the validation suite and F5 AI Gateway /
// MCP Gateway. It defines what the suite needs to know about F5's handling of
// a request; it does not model or predict that handling.
//
// The only implementation today is MockProvider, which observes nothing and
// says so. A real provider must be built on F5's documented audit or log
// interface once access is available (see validation/F5/README.md); nothing
// in this package assumes what that interface looks like.
package f5

import (
	"context"
	"time"
)

// Observation sources.
const (
	SourceMock = "MOCK"
	SourceF5   = "F5"
)

// Authorization decisions an Observation can carry.
const (
	DecisionAllow = "ALLOW"
	DecisionDeny  = "DENY"
	// DecisionNotObserved: no F5 component handled the request, or its
	// handling could not be found.
	DecisionNotObserved = "NOT_OBSERVED"
)

// Mocked is the value of every field a MockProvider fills in.
const Mocked = "MOCK"

// Correlation identifies the request whose F5-side handling is looked up.
type Correlation struct {
	TestID       string    `json:"test_id"`
	RequestLabel string    `json:"request_label"`
	ProposalID   string    `json:"proposal_id"`
	AgentID      string    `json:"agent_id"`
	Tool         string    `json:"tool"`
	Target       string    `json:"target"`
	SentAt       time.Time `json:"sent_at"`
}

// Observation is what F5 recorded about one request. Fields listed in
// MockedFields were not observed from F5.
type Observation struct {
	Source                     string   `json:"source"`
	Provider                   string   `json:"provider"`
	RequestID                  string   `json:"request_id"`
	AgentIdentity              string   `json:"agent_identity"`
	Tool                       string   `json:"tool"`
	Target                     string   `json:"target"`
	AuthorizationDecision      string   `json:"authorization_decision"`
	PolicyIdentifier           string   `json:"policy_identifier"`
	Timestamp                  string   `json:"timestamp"`
	AuditEvent                 string   `json:"audit_event"`
	DownstreamRequestReference string   `json:"downstream_request_reference"`
	RawEvidenceReference       string   `json:"raw_evidence_reference"`
	MockedFields               []string `json:"mocked_fields"`
	Note                       string   `json:"note,omitempty"`
}

// F5ObservationProvider looks up F5's handling of a request.
type F5ObservationProvider interface {
	Name() string
	Observe(ctx context.Context, c Correlation) (Observation, error)
}

// MockProvider is used while no F5 component is in the request path. It
// returns a placeholder with every field set to MOCK and the decision
// NOT_OBSERVED. It never reports ALLOW or DENY.
type MockProvider struct{}

func (MockProvider) Name() string { return "mock (no F5 in path)" }

func (MockProvider) Observe(context.Context, Correlation) (Observation, error) {
	return Observation{
		Source:                     SourceMock,
		Provider:                   MockProvider{}.Name(),
		RequestID:                  Mocked,
		AgentIdentity:              Mocked,
		Tool:                       Mocked,
		Target:                     Mocked,
		AuthorizationDecision:      DecisionNotObserved,
		PolicyIdentifier:           Mocked,
		Timestamp:                  Mocked,
		AuditEvent:                 Mocked,
		DownstreamRequestReference: Mocked,
		RawEvidenceReference:       Mocked,
		MockedFields: []string{
			"request_id", "agent_identity", "tool", "target", "authorization_decision", "policy_identifier",
			"timestamp", "audit_event", "downstream_request_reference", "raw_evidence_reference",
		},
		Note: "No F5 component was in the request path. This is a placeholder, not an F5 observation.",
	}, nil
}

// Layer results for F5 in a test.
const (
	ResultNotTested     = "NOT_TESTED"
	ResultNotApplicable = "NOT_APPLICABLE"
	ResultUnknown       = "UNKNOWN"
	// ResultPrevented: F5 denied every attack request in the test.
	ResultPrevented = "PREVENTED"
	// ResultAllowed: F5 allowed at least one request in the test. For a
	// legitimate request this is the expected result.
	ResultAllowed = "ALLOWED"
)

// Classify derives the F5 layer result of a test from the observations of the
// requests that passed through the gateway path. Any observation that is not
// from F5 makes the result NOT_TESTED; a real observation without a decision
// makes it UNKNOWN. With no gateway-path requests the result is
// NOT_APPLICABLE.
func Classify(obs []Observation) string {
	if len(obs) == 0 {
		return ResultNotApplicable
	}
	allowed := false
	for _, o := range obs {
		if o.Source != SourceF5 {
			return ResultNotTested
		}
		switch o.AuthorizationDecision {
		case DecisionAllow:
			allowed = true
		case DecisionDeny:
		default:
			return ResultUnknown
		}
	}
	if allowed {
		return ResultAllowed
	}
	return ResultPrevented
}
