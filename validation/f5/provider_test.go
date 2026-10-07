package f5

import (
	"context"
	"testing"
)

func TestMockProviderNeverReportsADecision(t *testing.T) {
	o, err := MockProvider{}.Observe(context.Background(), Correlation{TestID: "TC01"})
	if err != nil {
		t.Fatal(err)
	}
	if o.Source != SourceMock || o.AuthorizationDecision != DecisionNotObserved {
		t.Fatalf("observation = %+v, want MOCK / NOT_OBSERVED", o)
	}
	for name, v := range map[string]string{
		"request_id": o.RequestID, "agent_identity": o.AgentIdentity, "tool": o.Tool, "target": o.Target,
		"policy_identifier": o.PolicyIdentifier, "timestamp": o.Timestamp, "audit_event": o.AuditEvent,
		"downstream_request_reference": o.DownstreamRequestReference, "raw_evidence_reference": o.RawEvidenceReference,
	} {
		if v != Mocked {
			t.Errorf("%s = %q, want %q", name, v, Mocked)
		}
	}
	if len(o.MockedFields) != 10 {
		t.Errorf("mocked fields = %v, want all 10 observation fields", o.MockedFields)
	}
}

func TestClassify(t *testing.T) {
	mock, _ := MockProvider{}.Observe(context.Background(), Correlation{})
	real := func(d string) Observation { return Observation{Source: SourceF5, AuthorizationDecision: d} }
	cases := []struct {
		name string
		obs  []Observation
		want string
	}{
		{"no gateway-path requests", nil, ResultNotApplicable},
		{"mock only", []Observation{mock}, ResultNotTested},
		{"real and mock", []Observation{real(DecisionDeny), mock}, ResultNotTested},
		{"real without a decision", []Observation{real(DecisionNotObserved)}, ResultUnknown},
		{"real deny", []Observation{real(DecisionDeny), real(DecisionDeny)}, ResultPrevented},
		{"real allow", []Observation{real(DecisionDeny), real(DecisionAllow)}, ResultAllowed},
	}
	for _, tc := range cases {
		if got := Classify(tc.obs); got != tc.want {
			t.Errorf("%s: Classify = %s, want %s", tc.name, got, tc.want)
		}
	}
}
