package contracts

// CommandType enumerates immutable infrastructure mutation intents
// accepted from LLM tool-calling nodes.
type CommandType string

const (
	CmdModifyRouting CommandType = "MODIFY_ROUTING"
	CmdUpdateCert    CommandType = "UPDATE_CERTIFICATE"
	CmdDeletePolicy  CommandType = "DELETE_POLICY"
)

// RootCoreEdgeID is the non-bypassable protected target identity.
// Autonomous agents must never mutate this profile.
const RootCoreEdgeID = "ROOT_CORE_EDGE"

// FailingNodeID triggers a deterministic non-retryable infrastructure fault
// used by integration tests and saga compensation drills.
const FailingNodeID = "FAILING_NODE"
