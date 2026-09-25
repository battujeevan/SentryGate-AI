package contracts

import "errors"

// NonRetryableInfraError represents structural network faults that must
// fail loud immediately and never enter Temporal retry loops.
//
// Compliance note: this sentinel lives in the contracts package so audit
// tooling can inspect non-retryable boundaries without redeploying workflows.
var NonRetryableInfraError = errors.New("NON_RETRYABLE_INFRASTRUCTURE_FAULT")

// NonRetryableErrorType is the Temporal NonRetryableErrorTypes string matching
// NonRetryableInfraError.Error(). Keep these synchronized for compliance audits.
const NonRetryableErrorType = "NON_RETRYABLE_INFRASTRUCTURE_FAULT"

// ErrRiskCeilingBreach is returned when an AgentProposal exceeds the
// proxy risk threshold floor.
var ErrRiskCeilingBreach = errors.New("security isolation: risk score breaches threshold floor")

// ErrRootCoreMutation is returned when an agent attempts to mutate ROOT_CORE_EDGE.
var ErrRootCoreMutation = errors.New("critical compliance breach: autonomous mutation targeting ROOT_CORE_EDGE is blocked")

// ErrAuthHandshakeFailed is returned by mock clients when TLS/API auth fails.
var ErrAuthHandshakeFailed = errors.New("downstream authentication handshake failed")

// ErrAuditPersistFailed is returned when an immutable audit record cannot be written.
var ErrAuditPersistFailed = errors.New("audit trail persistence failed")
