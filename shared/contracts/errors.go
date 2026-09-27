package contracts

import "errors"

// NonRetryableInfraError represents structural faults that should fail
// immediately rather than be retried.
var NonRetryableInfraError = errors.New("NON_RETRYABLE_INFRASTRUCTURE_FAULT")

// NonRetryableErrorType is listed in the workflow's NonRetryableErrorTypes, but
// it does not match NonRetryableInfraError: Temporal compares error type names,
// not messages, and a plain errors.New value has no such type. Single-attempt
// behaviour comes from MaximumAttempts: 1.
const NonRetryableErrorType = "NON_RETRYABLE_INFRASTRUCTURE_FAULT"

// ErrRootCoreMutation is returned by the simulated Zscaler client when asked
// to delete ROOT_CORE_EDGE.
var ErrRootCoreMutation = errors.New("critical compliance breach: autonomous mutation targeting ROOT_CORE_EDGE is blocked")

// ErrAuthHandshakeFailed is returned by the simulated clients when auth fails.
var ErrAuthHandshakeFailed = errors.New("downstream authentication handshake failed")

// ErrAuditPersistFailed is returned when an audit record cannot be written.
var ErrAuditPersistFailed = errors.New("audit trail persistence failed")
