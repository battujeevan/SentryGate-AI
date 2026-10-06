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

// Targets with fixed behaviour in the simulated infrastructure adapter, used
// by tests and demos. They have no meaning to a real adapter.
const (
	// FailingNodeID: dispatch partly applies and then fails (confirmed
	// FAILURE with PartiallyApplied), which exercises compensation.
	FailingNodeID = "FAILING_NODE"
	// LostResponseNodeID: dispatch applies the change but the response is
	// lost (UNKNOWN); reconciliation finds it applied (SUCCESS).
	LostResponseNodeID = "LOST_RESPONSE_NODE"
	// UnreachableNodeID: the request never reaches the target (UNKNOWN);
	// reconciliation finds no trace of it (FAILURE).
	UnreachableNodeID = "UNREACHABLE_NODE"
	// PartitionedNodeID: neither dispatch nor reconciliation gets an answer
	// (UNKNOWN, and still UNKNOWN after reconciliation).
	PartitionedNodeID = "PARTITIONED_NODE"
)
