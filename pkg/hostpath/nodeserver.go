package hostpath


const (
	TopologyKeyNode = "topology.hostpath.csi/node"

	failedPreconditionAccessModeConflict = "volume uses SINGLE_NODE_SINGLE_WRITER access mode and is already mounted at a different target path"
)