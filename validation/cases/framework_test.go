package cases

import (
	"testing"

	"github.com/battujeevan/SentryGate-AI/internal/decision"
	"github.com/battujeevan/SentryGate-AI/shared/contracts"
	"github.com/battujeevan/SentryGate-AI/validation/agent"
	"github.com/battujeevan/SentryGate-AI/validation/evidence"
	"github.com/battujeevan/SentryGate-AI/validation/harness"
)

func gatewayAttempt(status int, resp *agent.IngressResponse) *Attempt {
	return &Attempt{Request: agent.Request{Path: agent.PathGateway, HTTPStatus: status, Response: resp}, attempt: true}
}

func runAttempt(o harness.RunOutcome, decisionID string) *Attempt {
	return &Attempt{
		Request: agent.Request{Path: agent.PathBoundary, RunID: o.RunID}, attempt: true, Outcome: &o,
		exec: &contracts.ExecutionRequest{IngressDecisionID: decisionID},
	}
}

func TestClassify(t *testing.T) {
	key := decision.ExecutionKey("dec_sent")
	cases := []struct {
		name string
		a    *Attempt
		want string
	}{
		{"ingress deny", gatewayAttempt(403, &agent.IngressResponse{Verdict: "DENY", Reasons: []string{"X"}}), OutcomeDeniedAtIngress},
		{"bad request", gatewayAttempt(400, &agent.IngressResponse{Error: "bad"}), OutcomeRejectedAtIngress},
		{"unauthenticated", gatewayAttempt(401, &agent.IngressResponse{Error: "no key"}), OutcomeRejectedAtIngress},
		{"already running", gatewayAttempt(409, &agent.IngressResponse{}), OutcomeNotStarted},
		{"accepted, run not observed", gatewayAttempt(200, &agent.IngressResponse{}), OutcomeUnknown},
		{"server error", gatewayAttempt(502, &agent.IngressResponse{}), OutcomeUnknown},
		{"start rejected", &Attempt{Request: agent.Request{Path: agent.PathBoundary, Error: "already started"}, attempt: true}, OutcomeNotStarted},
		{"completed", runAttempt(harness.RunOutcome{Status: harness.RunCompleted}, "d"), OutcomeExecuted},
		{"binding refused", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.IngressDecisionInvalidErrorType}, "d"), OutcomeRefusedAtBoundary},
		{"claim refused", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.ExecutionClaimRejectedErrorType}, "d"), OutcomeRefusedAtBoundary},
		{"revalidation refused", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.RevalidationDeniedErrorType}, "d"), OutcomeRefusedAtBoundary},
		{"dispatch failed, nothing sent", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.DispatchFailedErrorType}, "dec_unsent"), OutcomeRefusedByAdapter},
		{"dispatch failed, call sent", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.DispatchFailedErrorType}, "dec_sent"), OutcomeExecuted},
		{"outcome unknown", runAttempt(harness.RunOutcome{Status: harness.RunFailed, ErrorType: contracts.DispatchOutcomeUnknownErrorType}, "d"), OutcomeUnknown},
		{"still open", runAttempt(harness.RunOutcome{Status: harness.RunOpen}, "d"), OutcomeUnknown},
		{"terminated", runAttempt(harness.RunOutcome{Status: harness.RunTerminated}, "d"), OutcomeTerminated},
	}
	for _, tc := range cases {
		classify(tc.a, map[string]int{key: 1})
		if tc.a.SentryGate != tc.want {
			t.Errorf("%s: outcome %s (%s), want %s", tc.name, tc.a.SentryGate, tc.a.SentryGateDetail, tc.want)
		}
	}
}

func TestSentryGateResult(t *testing.T) {
	att := func(role, outcome string) *Attempt {
		return &Attempt{Request: agent.Request{Role: role}, attempt: true, SentryGate: outcome}
	}
	cases := []struct {
		kind     string
		attempts []*Attempt
		want     string
	}{
		{evidence.KindAttack, []*Attempt{att(agent.RoleLegitimate, OutcomeExecuted), att(agent.RoleAttack, OutcomeDeniedAtIngress)}, evidence.SGBlocked},
		{evidence.KindAttack, []*Attempt{att(agent.RoleAttack, OutcomeRefusedAtBoundary), att(agent.RoleAttack, OutcomeExecuted)}, evidence.SGDidNotBlock},
		{evidence.KindAttack, []*Attempt{att(agent.RoleAttack, OutcomeRefusedAtBoundary), att(agent.RoleAttack, OutcomeUnknown)}, evidence.SGUnknown},
		{evidence.KindAttack, []*Attempt{att(agent.RoleLegitimate, OutcomeExecuted)}, evidence.SGNotApplicable},
		{evidence.KindLegitimate, []*Attempt{att(agent.RoleLegitimate, OutcomeExecuted)}, evidence.SGExecuted},
		{evidence.KindFault, []*Attempt{att(agent.RoleLegitimate, OutcomeUnknown)}, evidence.SGUnknown},
		{evidence.KindLegitimate, []*Attempt{att(agent.RoleLegitimate, OutcomeDeniedAtIngress)}, evidence.SGNotExecuted},
	}
	for i, tc := range cases {
		if got := sentryGateResult(tc.kind, tc.attempts); got != tc.want {
			t.Errorf("case %d: %s, want %s", i, got, tc.want)
		}
	}
}

func TestCatalogue(t *testing.T) {
	all := All()
	if len(all) != 12 {
		t.Fatalf("%d tests, want 12", len(all))
	}
	for i, c := range all {
		if want := "TC" + string(rune('0'+(i+1)/10)) + string(rune('0'+(i+1)%10)); c.ID != want {
			t.Errorf("test %d has ID %s, want %s", i, c.ID, want)
		}
		if c.Name == "" || c.Objective == "" || c.Expected == "" || len(c.Method) == 0 || c.Run == nil {
			t.Errorf("%s is incomplete", c.ID)
		}
		if c.Kind != evidence.KindAttack && c.Kind != evidence.KindLegitimate && c.Kind != evidence.KindFault {
			t.Errorf("%s kind %q", c.ID, c.Kind)
		}
	}
}
