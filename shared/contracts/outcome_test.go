package contracts_test

import (
	"testing"

	"github.com/battujeevan/SentryGate-AI/shared/contracts"
)

func TestCheckedOutcome(t *testing.T) {
	cases := []struct {
		in      contracts.DispatchOutcome
		status  contracts.OutcomeStatus
		partial bool
	}{
		{contracts.DispatchOutcome{Status: contracts.OutcomeSuccess}, contracts.OutcomeSuccess, false},
		{contracts.DispatchOutcome{Status: contracts.OutcomeSuccess, PartiallyApplied: true}, contracts.OutcomeSuccess, false},
		{contracts.DispatchOutcome{Status: contracts.OutcomeFailure}, contracts.OutcomeFailure, false},
		{contracts.DispatchOutcome{Status: contracts.OutcomeFailure, PartiallyApplied: true}, contracts.OutcomeFailure, true},
		{contracts.DispatchOutcome{Status: contracts.OutcomeUnknown, PartiallyApplied: true}, contracts.OutcomeUnknown, false},
		{contracts.DispatchOutcome{}, contracts.OutcomeUnknown, false},
		{contracts.DispatchOutcome{Status: "success"}, contracts.OutcomeUnknown, false},
		{contracts.DispatchOutcome{Status: "FAILED", PartiallyApplied: true}, contracts.OutcomeUnknown, false},
	}
	for _, tc := range cases {
		got := tc.in.Checked()
		if got.Status != tc.status || got.PartiallyApplied != tc.partial {
			t.Errorf("%+v.Checked() = %+v, want %s partial=%t", tc.in, got, tc.status, tc.partial)
		}
	}
}
