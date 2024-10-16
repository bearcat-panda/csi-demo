package hostpath

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/balancer/grpclb/state"
	"sync"
)

const (
	kib    int64 = 1024
	mib    int64 = kib * 1024
	gib    int64 = mib * 1024
	gib100 int64 = gib * 100
	tib    int64 = gib * 1024
	tib100 int64 = tib * 100

	// storageKind is the special parameter which requests
	// storage of a certain kind (only affects capacity checks).
	storageKind = "kind"
)

type hostpath struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer
	csi.UnimplementedGroupControllerServer
	config Config

	//访问state.必须要使用互斥锁
	mutex sync.Mutex
	state state.State
}

type Config struct {
	// csi driver名称
	DriverName 				string
	// csi endpoint
	EndPoint 		string
	NodeID string
	// csi 插件版本
	VendorVersion string
	// 用于存储驱动程序重启、卷和快照的状态信息的目录
	StateDir string
	// 每个节点的volume限制
	MaxVolumesPerNode             int64
	// 最大卷大小.以字节为单位
	MaxVolumeSize int64
	// 节点上可附加卷的最大数量。零表示没有限制。
	AttachLimit int64
	// 存储容量
	Capacity Capacity
	// 以临时模式发布卷，即使 kubelet 没有要求
	Ephemeral bool
	// 启用 RPC_PUBLISH_UNPUBLISH_VOLUME 功能
	EnableAttach bool
	// 启用 PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS 功能
	EnableTopology bool
	// 启用卷扩展功能。
	EnableVolumeExpansion bool
	// 启用 Controller modify volume 功能
	EnableControllerModifyVolume  bool
	// 可在持久卷上修改的参数名称的逗号分隔列表。仅当 enable-controller-modify-volume 为 true 时，才使用此选项。如果未设置，则所有参数都是可变的。
	AcceptedMutableParameterNames StringArray
	// 禁用 Controller 卷扩展功能。
	DisableControllerExpansion    bool
	// 禁用 Node 卷扩展功能。
	DisableNodeExpansion          bool
	// 在节点上扩展时允许的最大卷大小。默认大小与 max-volume-size 相同
	MaxVolumeExpansionSizeNode    int64
	// 可用于将卷生命周期的某些冲突转换为警告，而不是使不正确的 gRPC 调用失败
	CheckVolumeLifecycle          bool
}





















