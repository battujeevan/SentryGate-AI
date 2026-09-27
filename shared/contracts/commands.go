package contracts

// CommandType names an infrastructure mutation an agent can propose. The set
// of accepted commands is defined by the policy's command catalogue; these
// constants are the commands used by the shipped policy and the simulator.
type CommandType string

const (
	CmdModifyRouting CommandType = "MODIFY_ROUTING"
	CmdUpdateCert    CommandType = "UPDATE_CERTIFICATE"
	CmdDeletePolicy  CommandType = "DELETE_POLICY"
)

// RootCoreEdgeID is the protected target used by the shipped policy and demos.
// Protection itself comes from the policy's target registry, not this constant.
const RootCoreEdgeID = "ROOT_CORE_EDGE"

// FailingNodeID makes the simulated infrastructure adapter fail, which
// exercises the compensation path in tests and demos.
const FailingNodeID = "FAILING_NODE"
