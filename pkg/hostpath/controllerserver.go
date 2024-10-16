package hostpath

import (
	"context"
	"github.com/bearcat-panda/csi-demo/pkg/state"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

func (hp *hostpath) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (resp *csi.CreateVolumeResponse, finalerr error) {
	if err := hp.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		klog.V(3).Infof("invalid create volume req: %v", req)
		return nil, err
	}

	if len(req.GetMutableParameters()) > 0 {
		if err := hp.validateControllerServiceRequest(csi.ControllerServiceCapability_RPC_MODIFY_VOLUME); err != nil {
			klog.V(3).Infof("invalid create volume req: %v", req)
			return nil, err
		}
		// Check if the mutable parameters are in the accepted list
		if err := hp.validateVolumeMutableParameters(req.MutableParameters); err != nil {
			return nil, err
		}
	}

	// Check arguments
	if len(req.GetName()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	caps := req.GetVolumeCapabilities()
	if caps == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	// Keep a record of the requested access types.
	var accessTypeMount, accessTypeBlock bool

	for _, cap := range caps {
		if cap.GetBlock() != nil {
			accessTypeBlock = true
		}
		if cap.GetMount() != nil {
			accessTypeMount = true
		}
	}

	// 真正的csi驱动程序还需要检查其他字段
	if accessTypeBlock && accessTypeMount {
		return nil, status.Error(codes.InvalidArgument, "cannot have both block and mount access type")
	}

	var requestedAccessType state.AccessType

	if accessTypeBlock {
		requestedAccessType = state.BlockAccess
	} else {
		// Default to mount.
		requestedAccessType = state.MountAccess
	}

	// 在操作全局status是.需要先加锁
	hp.mutex.Lock()
	defer hp.mutex.Unlock()

	capacity := int64(req.GetCapacityRange().GetRequiredBytes())
	topologies := []*csi.Topology{}
	if hp.config.EnableTopology {
		topologies = append(topologies, &csi.Topology{Segments: map[string]string{TopologyKeyNode: hp.config.NodeID}})
	}

}

// validateVolumeMutableParameters is a helper function to check if the mutable parameters are in the accepted list
func (hp *hostpath) validateVolumeMutableParameters(params map[string]string) error {
	if len(hp.config.AcceptedMutableParameterNames) == 0 {
		return nil
	}

	accepts := sets.New(hp.config.AcceptedMutableParameterNames...)
	unsupported := []string{}
	for k := range params {
		if !accepts.Has(k) {
			unsupported = append(unsupported, k)
		}
	}
	if len(unsupported) > 0 {
		return status.Errorf(codes.InvalidArgument, "invalid parameters: %v", unsupported)
	}
	return nil
}

func (hp *hostpath) validateControllerServiceRequest(c csi.ControllerServiceCapability_RPC_Type) error {
	if c == csi.ControllerServiceCapability_RPC_UNKNOWN {
		return nil
	}

	for _, cap := range hp.getControllerServiceCapabilities() {
		if c == cap.GetRpc().GetType() {
			return nil
		}
	}
	return status.Errorf(codes.InvalidArgument, "unsupported capability %s", c)
}
func (hp *hostpath) getControllerServiceCapabilities() []*csi.ControllerServiceCapability {
	var cl []csi.ControllerServiceCapability_RPC_Type
	if !hp.config.Ephemeral {
		cl = []csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_GET_VOLUME,
			csi.ControllerServiceCapability_RPC_GET_CAPACITY,
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
			csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
			csi.ControllerServiceCapability_RPC_CLONE_VOLUME,
			csi.ControllerServiceCapability_RPC_VOLUME_CONDITION,
			csi.ControllerServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
		}
		if hp.config.EnableVolumeExpansion && !hp.config.DisableControllerExpansion {
			cl = append(cl, csi.ControllerServiceCapability_RPC_EXPAND_VOLUME)
		}
		if hp.config.EnableAttach {
			cl = append(cl, csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
		}
		if hp.config.EnableControllerModifyVolume {
			cl = append(cl, csi.ControllerServiceCapability_RPC_MODIFY_VOLUME)
		}

	}

	var csc []*csi.ControllerServiceCapability

	for _, cap := range cl {
		csc = append(csc, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return csc
}