package hostpath

import (
	"fmt"
	"github.com/bearcat-panda/csi-demo/pkg/state"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
	utilexec "k8s.io/utils/exec"
	"os"
	"path/filepath"
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

var (
	vendorVersion = "dev"
)

const (
	// Extension with which snapshot files will be saved.
	snapshotExt = ".snap"
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


// createVolume 分配容量，为 hostpath 卷创建目录，并将卷添加到列表中
func (hp *hostpath) createVolume(volID, name string, cap int64, volAccessType state.AccessType, ephemeral bool, kind string) (*state.Volume, error) {
	// 检查最大可用容量
	if cap > hp.config.MaxVolumeSize {
		return nil, status.Errorf(codes.OutOfRange, "Requested capacity %d exceeds maximum allowed %d", cap, hp.config.MaxVolumeSize)
	}

	// 判断是否配置了容量
	if hp.config.Capacity.Enabled() {
		if kind == "" {
			// 选择具有足够剩余容量的种类。
			for k, c := range hp.config.Capacity {
				// 判断已经使用的容量和要申请的容量. 是否超出总容量
				if hp.sumVolumeSizes(k) + cap <= c.Value() {
					kind = k
					break
				}
			}
		}

		if kind == "" {
			// 还是无法匹配.直接返回错误
			return nil, status.Errorf(codes.ResourceExhausted, "requested capacity %d of arbitrary storage exceeds all remaining capacity", cap)
		}
		used := hp.sumVolumeSizes(kind)
		available := hp.config.Capacity[kind]
		if used + cap > available.Value() {
			return nil, status.Errorf(codes.ResourceExhausted, "requested capacity %d exceeds remaining capacity for %q, %s out of %s already used",
				cap, kind, resource.NewQuantity(used, resource.BinarySI).String(), available.String())
		}
	} else if kind != "" {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("capacity tracking disabled, specifying kind %q is invalid", kind))
	}

	path := hp.getVolumePath(volID)

	switch volAccessType {
	case state.MountAccess:
		err := os.MkdirAll(path, 0777)
		if err != nil {
			return nil, err
		}
	case state.BlockAccess:
		executor := utilexec.New()
		size := fmt.Sprintf("%dM", cap/mib)
		// 创建块文件。
		_, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				out, err := executor.Command("fallocate", "-l", size, path).CombinedOutput()
				if err != nil {
					return nil, fmt.Errorf("failed to create block device: %v, %v", err, string(out))
				}
			} else {
				return nil, fmt.Errorf("failed to stat block device: %v, %v", path, err)
			}
		}

		// 将块文件与 loop 设备关联。
		volPathHandler := volumepathhandler.VolumePathHandler{}
		_, err = volPathHandler.AttachFileDevice(path)
		if err != nil {
			// 删除块文件，因为它将不再使用。
			if errDelete := os.Remove(path); errDelete != nil {
				klog.Errorf("failed to cleanup block file %s: %v", path, errDelete)
			}
			return nil, fmt.Errorf("failed to attach device %v: %v", path, err)
		}
	default:
		return nil, fmt.Errorf("unsupported access type %v", volAccessType)

	}

	volume := state.Volume{
		VolID: volID,
		VolName: name,
		VolSize: cap,
		VolPath: path,
		VolAccessType: volAccessType,
		Ephemeral: ephemeral,
		Kind: kind,
	}

	klog.V(4).Infof("adding hostpath volume: %s = %+v", volID, volume)
	if err := hp.state.UpdateVolume(volume); err != nil {
		return nil, err
	}
	return &volume, nil
}


// 获取当前类型volume已经被使用的容量
func (hp *hostpath) sumVolumeSizes(kind string) (sum int64) {
	for _, volume := range hp.state.GetVolumes() {
		if volume.Kind == kind {
			sum += volume.VolSize
		}
	}
	return
}

// getVolumePath 返回 hostpath 卷的规范路径
func (hp *hostpath) getVolumePath(volID string) string {
	return filepath.Join(hp.config.StateDir, volID)
}

// getSnapshotPath 返回快照存储位置的完整路径
func (hp *hostpath) getSnapshotPath(snapshotID string) string {
	return filepath.Join(hp.config.StateDir, fmt.Sprintf("%s%s", snapshotID, snapshotExt))
}

// 使用来自快照的数据填充volume
func (hp *hostpath) loadFromSnapshot(size int64, snapshotId, destPath string, mode state.AccessType) error {
	snapshot, err := hp.state.GetSnapshotByID(snapshotId)
	if err != nil {
		return err
	}

	if !snapshot.ReadyToUse {
		return fmt.Errorf("snapshot %v is not yet ready to use", snapshotId)
	}

	if snapshot.SizeBytes > size {
		return status.Errorf(codes.InvalidArgument, "snapshot %v size %v is greater than requested volume size %v", snapshotId, snapshot.SizeBytes, size)
	}
	snapshotPath := snapshot.Path

	var cmd []string
	switch mode {
	case state.MountAccess:
		// 解压缩一个 .tar.gz 格式的快照文件，将内容提取到指定的目标路径 destPath
		cmd = []string{"tar", "zxvf", snapshotPath, "-C", destPath}
	case state.BlockAccess:
		// 使用 dd 命令将快照文件的内容直接复制到目标路径，适合处理原始磁盘镜像或文件
		cmd = []string{"dd", "if=" + snapshotPath, "of=" + destPath}
	default:
		return status.Errorf(codes.InvalidArgument, "unknown accessType: %d", mode)
	}

	executor := utilexec.New()
	klog.V(4).Infof("Command Start: %v", cmd)
	out, err := executor.Command(cmd[0], cmd[1:]...).CombinedOutput()
	klog.V(4).Infof("Command Finish: %v", string(out))
	if err != nil {
		return fmt.Errorf("failed pre-poplulate data from snapshot %v: %w: %s", snapshotId, err, out)
	}
	return nil
}

// 使用本地数据填充volume
func (hp *hostpath) loadFromVolume(size int64, srcVolumeId, destPath string, mode state.AccessType) error {
	hostPathVolume, err := hp.state.GetVolumeByID(srcVolumeId)
	if err != nil {
		return err
	}
	if hostPathVolume.VolSize > size {
		return status.Errorf(codes.InvalidArgument, "volume %v size %v is greater than requested volume size %v", srcVolumeId, hostPathVolume.VolSize, size)
	}
	if mode != hostPathVolume.VolAccessType {
		return status.Errorf(codes.InvalidArgument, "volume %v mode is not compatible with requested mode", srcVolumeId)
	}

	switch mode {
	case state.MountAccess:
		return loadFromFileSystemVolume(hostPathVolume, destPath)
	case state.BlockAccess:
		return loadFromBlockVolume(hostPathVolume, destPath)
	default:
		return status.Errorf(codes.InvalidArgument, "unknow accessType: %d", mode)
	}
}

// 从系统文件加载数据.填充到volume
func loadFromFileSystemVolume(hosPathVolume state.Volume, destPath string) error {
	srcPath := hosPathVolume.VolPath
	// 判断目录是否为空
	isEmpty, err := hostPathIsEmpty(srcPath)
	if err != nil {
		return fmt.Errorf("failed verification check of source hostpath volume %v: %w", hosPathVolume.VolID, err)
	}

	// 如果源 hostpath 卷为空，则它是一个 noop，我们只需继续操作，否则 cp 调用将失败，并显示 a file stat 错误 DNE
	if !isEmpty{
		args :=[]string{"-a", srcPath + "/.", destPath + "/"}
		executor := utilexec.New()
		out, err := executor.Command("cp", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed pre-populate data from volume %v: %s: %w", hosPathVolume.VolID, out, err)
		}
	}

	return nil
}

// 从块文件加载数据.填充到volume
func loadFromBlockVolume(hostPathVolume state.Volume, destPath string) error {
	srcPath := hostPathVolume.VolPath
	args := []string{"if=" + srcPath, "of=" + destPath}
	executor := utilexec.New()
	out, err := executor.Command("dd", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed pre-populate data from volume %v: %w: %s", hostPathVolume.VolID, err, out)
	}
	return nil
}

// hostPathIsEmpty 是一个简单的检查，用于确定指定的 hostpath 目录是否为空。
func hostPathIsEmpty(p string) (bool, error) {
	f, err := os.Open(p)
	if err != nil {
		return true, fmt.Errorf("unable to open hostpath volume, error: %v", err)
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, nil
}

// deleteVolume 删除 hostpath 卷的目录。
func (hp *hostpath) deleteVolume(volID string) error {
	klog.V(4).Infof("starting to delete hostpath volume: %s", volID)

	vol, err := hp.state.GetVolumeByID(volID)
	if err != nil {
		// 如果找不到存储卷.直接返回ok
		return nil
	}

	if vol.VolAccessType == state.BlockAccess {
		volPathHandler := volumepathhandler.VolumePathHandler{}
		path := hp.getVolumePath(volID)
		klog.V(4).Infof("deleteing loop device for file %s if it exists", path)
		if err := volPathHandler.DetachFileDevice(path); err != nil{
			return fmt.Errorf("failed to remove loop device for file %s: %v", path, err)
		}
	}

	path := hp.getVolumePath(volID)
	if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := hp.state.DeleteVolume(volID); err != nil {
		return err
	}
	klog.V(4).Infof("deleted hostpath volume: %s = %+v", volID, vol)
	return nil
}







