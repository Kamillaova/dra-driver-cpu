/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/nri/pkg/stub"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/sysfs"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	drametadatav1beta1 "k8s.io/dynamic-resource-allocation/api/metadata/v1beta1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

const (
	// maxAttempts indicates the number of times the driver will try to recover itself before failing
	maxAttempts = 5
	// registrationPollInterval and registrationTimeout bound the wait for kubelet to
	// acknowledge the plugin at startup
	registrationPollInterval = 1 * time.Second
	registrationTimeout      = 30 * time.Second
)

const opIDLen = 8

// KubeletPlugin is an interface that describes the methods used from kubeletplugin.Helper.
type KubeletPlugin interface {
	PublishResources(context.Context, resourceslice.DriverResources) error
	RegistrationStatus() *registerapi.RegistrationStatus
	Stop()
}

type cdiManager interface {
	AddDevice(logger logr.Logger, deviceName string, envVar string, requests []store.RequestAllocation) error
	Refresh() error
	GetDeviceEnv(deviceName string) ([]string, error)
	// CCX-FORK: added; GetDeviceEnv is upstream's and now unused by the driver.
	GetDeviceAllocations(deviceName string) ([]store.RequestAllocation, error)
	// CCX-FORK: added, seeds the allocation store from disk before Start registers with the kubelet.
	PreparedClaimAllocations(logger logr.Logger) map[types.UID][]store.RequestAllocation
	RemoveDevice(logger logr.Logger, deviceName string) error
}

// CPUInfoProvider is an interface for getting CPU information.
type CPUInfoProvider interface {
	GetCPUTopology(logger logr.Logger) (*cpuinfo.CPUTopology, error)
}

// CPUDriver is the structure that holds all the driver runtime information.
type CPUDriver struct {
	driverName         string
	nodeName           string
	kubeClient         kubernetes.Interface
	draPlugin          KubeletPlugin
	nriPlugin          stub.Stub
	podConfigStore     *store.PodConfig
	cpuAllocationStore *store.CPUAllocation
	cdiMgr             cdiManager
	topology           deviceTopology
	cpuDeviceMode      string
	cpuDeviceGroupBy   string
	// fullPhysicalCPUsOnly mirrors Config.FullPhysicalCPUsOnly: whether the
	// operator has asked for whole-core allocation at all. Whether it is
	// actually in effect for a given device is per device -- see
	// deviceTopology.deviceThreadsPerCore -- since one device's non-uniform
	// cores must not disable it for another.
	fullPhysicalCPUsOnly    bool
	placementPolicy         coreselect.Policy
	claimTracker            *store.ClaimTracker
	pcieRootMapper          *store.PCIeRootMapper
	devicesPerResourceSlice int
	metrics                 cpumetrics.Recorder
	health                  healthTracker
	// containerUpdater pushes unsolicited container updates. Nil until Start
	// registers the NRI plugin, and left nil when the operator has not asserted
	// the runtime tolerates such updates.
	containerUpdater containerUpdater
	// reconcileTrigger carries coalesced reconcile requests to the worker
	// goroutine. Nil when no feature needs it.
	reconcileTrigger chan struct{}
	// reconcileSharedOnUnprepare widens shared containers as soon as a claim is
	// released rather than at their next lifecycle event.
	reconcileSharedOnUnprepare bool
	// defrag configures the defragmentation pass, and is zero when it is off.
	defrag defragOptions
	// partitions is the node's cores as described, always ending with the
	// implicit partition holding whatever the described ones left. Set in New,
	// read-only after that.
	partitions []device.Partition
	// degradedPartitions are the partitions whose declared thread arity the
	// machine contradicts, mapped to what would fix each. They publish no
	// devices; the rest of the node serves as usual.
	degradedPartitions map[string]string
	// defaultPartitionCPUs is where a container holding no claim runs: the
	// partitions of role "default", which on an undescribed node is every
	// allocatable CPU. An exclusive partition never hosts such a container.
	defaultPartitionCPUs cpuset.CPUSet
	// sysfs is kept so a defragmentation pass can re-read which CPUs are online;
	// the set read in New is only true of startup.
	sysfs sysfs.FS
	// lastMoved is when each claim was last moved, for the cooldown. Guarded by
	// applyMu.
	lastMoved map[types.UID]time.Time
	// pendingRound is a set of moves the runtime never confirmed, held so the
	// next pass can send it again. Guarded by applyMu.
	pendingRound *defragRound
	// applyMu serializes the work that decides which CPUs back a claim: the DRA
	// prepare and unprepare hooks, the NRI hooks that read a placement or record
	// container state, and the background worker's local phases. It also covers
	// the store pointers themselves, which Synchronize replaces wholesale.
	//
	// It must never be held across a call into the runtime. The runtime may be
	// holding, on behalf of an inbound hook of ours, the lock our outbound call
	// needs; if that hook is waiting here meanwhile, neither side can proceed.
	// Inbound hooks may hold it freely, since answering one makes no call out.
	applyMu sync.Mutex
	// publishMu serializes ResourceSlice publication, so that two publishers
	// cannot read the device order and then hand it to the controller in the
	// opposite order, leaving the older one standing. Taken before applyMu,
	// never after.
	publishMu sync.Mutex
	// publishedOccupancy is, per device, whether it held a claim when the device
	// order was last published, which is the only input that order has. Guarded
	// by applyMu.
	publishedOccupancy map[string]bool

	kubeletRootDir string
}

// deviceHealthEntry is the last known health of a single device.
type deviceHealthEntry struct {
	status  kubeletplugin.HealthStatus
	message string
}

// deviceTopology holds the CPU topology and device-to-CPU/socket/NUMA
// mappings. Set once in New() and read-only after that, except deviceSlices,
// whose order is a live quantity under cache grouping.
type deviceTopology struct {
	cpuTopology            *cpuinfo.CPUTopology
	deviceNameToCPUID      map[string]int
	deviceNameToSocketID   map[string]int
	deviceNameToNUMANodeID map[string]int
	// devicesByPartition is each publishing partition's devices, which is what
	// the published slices are cut from. A partition's devices never share a
	// slice with another's, so its taints cannot travel in another's.
	devicesByPartition [][]resourceapi.Device
	// deviceSlices is devicesByPartition as it was last published: ordered and
	// chunked. Order is a live quantity under cache grouping, so this is rebuilt
	// on publication rather than fixed in New. Guarded by applyMu.
	deviceSlices [][]resourceapi.Device
	reservedCPUs cpuset.CPUSet
	onlineCPUs   cpuset.CPUSet
	// deviceNameToCPUs is each grouped device's own allocatable CPUs, which is
	// where a claim allocated onto that device takes its CPUs from: the group's,
	// inside its partition, and without the cores whole-core allocation dropped.
	deviceNameToCPUs map[string]cpuset.CPUSet
	// deviceNameToRole is each grouped device's partition role, which is what
	// tells a claim's share of a pool from the CPUs it holds alone.
	deviceNameToRole map[string]string
	// deviceThreadsPerCore is each grouped device's own effective whole-core
	// allocation step (0 when the feature is off or that device has no single
	// thread count), from BuildGrouped. Keyed by device name, which a caller
	// preparing a claim already has (alloc.Device).
	deviceThreadsPerCore map[string]int
	// numaNodeThreadsPerCore is deviceThreadsPerCore reindexed by NUMA node ID,
	// derived once in New() from deviceThreadsPerCore plus whichever of
	// deviceNameToSocketID/deviceNameToNUMANodeID is populated. It exists only
	// for callers that plan by NUMA node and have no device name in hand
	// (defragSelector's callers); a NUMA node belongs to exactly one socket, so
	// this is well defined under either grouping.
	numaNodeThreadsPerCore map[int]int
}

// Providers group the interfaces the CPUDriver depends on
type Providers struct {
	CPUInfo   CPUInfoProvider
	SysFS     sysfs.FS
	K8SClient kubernetes.Interface
}

func (pr Providers) EnsureCPUInfo() CPUInfoProvider {
	if pr.CPUInfo == nil {
		return cpuinfo.NewSystemCPUInfo(pr.EnsureSysFS())
	}
	return pr.CPUInfo
}

func (pr Providers) EnsureSysFS() sysfs.FS {
	if pr.SysFS == nil {
		return sysfs.Host()
	}
	return pr.SysFS
}

// Config is the configuration for the CPUDriver.
type Config struct {
	DriverName       string
	NodeName         string
	ReservedCPUs     cpuset.CPUSet
	CPUDeviceMode    string
	CPUDeviceGroupBy string
	ExposePCIeRoots  bool
	Metrics          cpumetrics.Recorder
	// KubeletRootDir is the kubelet root directory, from which the registrar
	// and plugin data directories are derived. Required and absolute:
	// driverconfig.Resolve refuses an empty or relative value, and New takes it as
	// given rather than checking it again.
	KubeletRootDir string
	// PublishNodeAllocatableResourceMapping publishes KEP-5517 nodeAllocatableResources mappings in
	// ResourceSlice devices. Requires the DRANodeAllocatableResources feature gate to be enabled in the cluster.
	PublishNodeAllocatableResourceMapping bool
	// FullPhysicalCPUsOnly allocates whole physical cores, so a core's SMT siblings are never split
	// between two claims or between a claim and the shared pool. Grouped mode only.
	FullPhysicalCPUsOnly bool
	// CachePlacementStrategy is how a claim choosing among caches that fit it
	// picks one; empty means coreselect.Pack.
	CachePlacementStrategy coreselect.Policy
	// AssumeUnsolicitedUpdatesSafe permits pushing container updates the runtime did not ask for.
	// Required by every feature that reacts without waiting for a container lifecycle event.
	AssumeUnsolicitedUpdatesSafe bool
	// ReconcileSharedOnUnprepare widens shared containers onto released CPUs immediately.
	// Requires AssumeUnsolicitedUpdatesSafe.
	ReconcileSharedOnUnprepare bool
	// DefragEnabled moves running claims to recover uncore cache alignment.
	// Requires AssumeUnsolicitedUpdatesSafe and grouped mode by NUMA node or socket.
	DefragEnabled bool
	// DefragInterval is how often a pass runs, on top of the passes a prepare or
	// release triggers.
	DefragInterval time.Duration
	// DefragMaxMovesPerPass caps the moves one pass makes.
	DefragMaxMovesPerPass int
	// DefragMinGain is the smallest improvement in needlessly spanned uncore
	// caches worth moving anything for.
	DefragMinGain int
	// DefragClaimCooldown is how long a claim is left alone after being moved.
	DefragClaimCooldown time.Duration
	// CPUPartitions describes the node's cores, already parsed and validated as
	// far as that is possible without the node's topology. Empty leaves the whole
	// node in the implicit partition, which is what an undescribed node has.
	CPUPartitions []device.Partition
}

func (cfg Config) DevicesPerResourceSlice() int {
	if cfg.ExposePCIeRoots {
		// We use the lower "advanced features" limit because the driver
		// may set list-type attributes (StringValues) such as PCIe roots.
		return resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures
	}
	return resourceapi.ResourceSliceMaxDevices
}

// New creates and initializes a CPUDriver, preparing all internal state.
// No external listeners or goroutines are started; call Start to begin serving.
func New(logger logr.Logger, providers Providers, config *Config) (*CPUDriver, error) {
	logger = logger.WithValues("driver", config.DriverName)

	metricsRecorder := config.Metrics
	if metricsRecorder == nil {
		metricsRecorder = cpumetrics.Noop()
	}
	plugin := &CPUDriver{
		driverName: config.DriverName,
		nodeName:   config.NodeName,
		kubeClient: providers.K8SClient,
		topology: deviceTopology{
			deviceNameToCPUID:      make(map[string]int),
			deviceNameToSocketID:   make(map[string]int),
			deviceNameToNUMANodeID: make(map[string]int),
			reservedCPUs:           config.ReservedCPUs,
		},
		cpuDeviceMode:           config.CPUDeviceMode,
		cpuDeviceGroupBy:        config.CPUDeviceGroupBy,
		claimTracker:            store.NewClaimTracker(),
		pcieRootMapper:          store.NewPCIeRootMapper(),
		devicesPerResourceSlice: config.DevicesPerResourceSlice(),
		metrics:                 metricsRecorder,
		health:                  newHealthTracker(),
		kubeletRootDir:          config.KubeletRootDir,
	}
	sfs := providers.EnsureSysFS()
	plugin.sysfs = sfs

	onlineCPUs, err := cpuinfo.OnlineCPUs(logger, sfs)
	if err != nil {
		return nil, fmt.Errorf("failed to get online CPUs: %w", err)
	}
	logger.V(2).Info("detected online CPUs", "cpus", onlineCPUs.String())
	plugin.topology.onlineCPUs = onlineCPUs

	topo, err := providers.EnsureCPUInfo().GetCPUTopology(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to get CPU topology: %w", err)
	}
	if topo == nil {
		return nil, fmt.Errorf("failed to get CPU topology: topology is nil")
	}
	plugin.topology.cpuTopology = topo

	if config.ExposePCIeRoots {
		if err := plugin.pcieRootMapper.Probe(logger, sfs, onlineCPUs); err != nil {
			return nil, fmt.Errorf("failed to list PCIe domains: %w", err)
		}
	}

	// CCX-FORK: the cores an operator described as partitions. Those no workload
	// may claim -- the reserved ones and the pools -- join the effective reserved
	// set, so capacity, allocation and defragmentation exclude them.
	if len(config.CPUPartitions) > 0 {
		degraded, err := validatePartitions(topo, onlineCPUs, config.CPUPartitions)
		if err != nil {
			return nil, err
		}
		plugin.degradedPartitions = degraded
		plugin.topology.reservedCPUs = plugin.topology.reservedCPUs.Union(unclaimablePartitionCPUs(config.CPUPartitions))
		presentCPUs, err := cpuinfo.PresentCPUs(logger, sfs)
		if err != nil {
			logger.Info("cannot read the present CPU set, so offline CPUs are unaccounted for", "err", err)
		} else {
			verifyOfflineAccounting(logger, topo, presentCPUs, onlineCPUs, config.CPUPartitions)
		}
	}
	plugin.partitions = device.WithImplicitDefault(config.CPUPartitions, onlineCPUs.Difference(plugin.topology.reservedCPUs))
	plugin.defaultPartitionCPUs = cpuset.New()
	for _, partition := range plugin.partitions {
		if partition.Role == device.PARTITION_ROLE_DEFAULT {
			plugin.defaultPartitionCPUs = plugin.defaultPartitionCPUs.Union(partition.CPUs)
		}
	}
	if len(config.CPUPartitions) > 0 {
		verified := make(map[string]bool, len(plugin.partitions))
		for _, partition := range plugin.partitions {
			reason, bad := plugin.degradedPartitions[partition.Name]
			verified[partition.Name] = !bad
			if bad {
				logger.Info("partition publishes no devices: the machine does not match what it declares",
					"partition", partition.Name, "reason", reason)
				continue
			}
			logger.Info("resolved CPU partition", "partition", partition.Name, "role", partition.Role, "cpus", partition.CPUs.String())
		}
		metricsRecorder.SetPartitionState(verified)
		if implicit := plugin.partitions[len(plugin.partitions)-1]; implicit.CPUs.IsEmpty() {
			logger.Info("the implicit default partition holds no CPUs: a claim that names no partition has nowhere to land on this node")
		}
	}

	plugin.cpuAllocationStore = store.NewCPUAllocation(plugin.topology.cpuTopology, plugin.topology.reservedCPUs)
	plugin.refreshAllocationMetrics()
	plugin.podConfigStore = store.NewPodConfig()

	plugin.fullPhysicalCPUsOnly = config.FullPhysicalCPUsOnly
	if plugin.fullPhysicalCPUsOnly {
		if err := validateReservedCPUsAlignment(topo, config.ReservedCPUs); err != nil {
			return nil, err
		}
	}
	plugin.placementPolicy = config.CachePlacementStrategy
	if plugin.placementPolicy == "" {
		plugin.placementPolicy = coreselect.Pack
	}

	// Unsolicited updates deadlock runtimes with a pre-nri#301 Adaptation, and
	// the driver cannot tell from the handshake, so the operator asserts it.
	if config.AssumeUnsolicitedUpdatesSafe {
		plugin.reconcileSharedOnUnprepare = config.ReconcileSharedOnUnprepare
		// CCX-FORK: defragmentation, like the reconcile, presupposes unsolicited
		// updates.
		plugin.defrag = defragOptions{
			enabled:       config.DefragEnabled,
			interval:      config.DefragInterval,
			maxMoves:      config.DefragMaxMovesPerPass,
			minGain:       config.DefragMinGain,
			claimCooldown: config.DefragClaimCooldown,
		}
		if plugin.defrag.enabled {
			plugin.lastMoved = make(map[types.UID]time.Time)
		}
		if plugin.reconcileSharedOnUnprepare || plugin.defrag.enabled {
			plugin.reconcileTrigger = make(chan struct{}, 1)
		}
	} else {
		if config.ReconcileSharedOnUnprepare {
			logger.V(2).Info("shared container reconcile is inert: set assumeUnsolicitedUpdatesSafe to enable it")
		}
		if config.DefragEnabled {
			logger.Info("defragmentation is inert: set assumeUnsolicitedUpdatesSafe to enable it")
		}
	}

	var devices []resourceapi.Device
	// CCX-FORK: grouped devices are built per partition, so that one partition's
	// taints never travel in another's ResourceSlice.
	var devicesByPartition [][]resourceapi.Device

	if plugin.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
		publishable := slices.DeleteFunc(slices.Clone(plugin.partitions), func(p device.Partition) bool {
			_, degraded := plugin.degradedPartitions[p.Name]
			return degraded
		})
		built := device.BuildGrouped(logger, plugin.cpuDeviceGroupBy, plugin.topology.cpuTopology, plugin.topology.onlineCPUs, plugin.topology.reservedCPUs, plugin.pcieRootMapper, config.PublishNodeAllocatableResourceMapping, config.FullPhysicalCPUsOnly, publishable)
		devicesByPartition = built.ByPartition
		for _, partitionDevices := range devicesByPartition {
			devices = append(devices, partitionDevices...)
		}
		plugin.topology.deviceThreadsPerCore = built.ThreadsPerCore
		plugin.topology.deviceNameToCPUs = built.CPUs
		plugin.topology.deviceNameToRole = built.Roles
		switch plugin.cpuDeviceGroupBy {
		case device.GROUP_BY_SOCKET:
			plugin.topology.deviceNameToSocketID = built.NameToID
		case device.GROUP_BY_NUMA_NODE, device.GROUP_BY_UNCORE_CACHE:
			plugin.topology.deviceNameToNUMANodeID = built.NameToID
		}
		plugin.topology.numaNodeThreadsPerCore = numaNodeThreadsPerCore(plugin.topology.cpuTopology, plugin.cpuDeviceGroupBy, built.NameToID, built.ThreadsPerCore)
	} else {
		devices, plugin.topology.deviceNameToCPUID = device.Build(plugin.topology.cpuTopology, plugin.topology.reservedCPUs, plugin.pcieRootMapper, config.PublishNodeAllocatableResourceMapping)
		devicesByPartition = [][]resourceapi.Device{devices}
	}

	// A slice holding a tainted device may carry half as many devices as one
	// without, so the limit follows what was actually built rather than the
	// options alone.
	if slices.ContainsFunc(devices, func(d resourceapi.Device) bool { return len(d.Taints) > 0 }) {
		plugin.devicesPerResourceSlice = min(plugin.devicesPerResourceSlice, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
	}

	plugin.topology.devicesByPartition = devicesByPartition
	plugin.refreshDeviceOrder()
	logger.Info("chunked devices into ResourceSlices", "numDevices", len(devices),
		"devicesPerResourceSlice", plugin.devicesPerResourceSlice, "numResourceSlices", len(plugin.topology.deviceSlices),
		"exposePCIeRoots", config.ExposePCIeRoots)

	for _, d := range devices {
		plugin.health.devices[d.Name] = &deviceHealthEntry{
			status:  kubeletplugin.HealthStatusHealthy,
			message: "device initialized",
		}
	}

	return plugin, nil
}

// unclaimablePartitionCPUs is every CPU a partition holds that no claim may
// take: the reserved partitions, and the pools, which are reached by claiming
// a pool device rather than by taking exclusive CPUs.
func unclaimablePartitionCPUs(partitions []device.Partition) cpuset.CPUSet {
	unclaimable := cpuset.New()
	for _, partition := range partitions {
		if !partition.PublishesExclusiveDevices() {
			unclaimable = unclaimable.Union(partition.CPUs)
		}
	}
	return unclaimable
}

// validatePartitions checks the described cores against the node the driver
// stands on: the checks that need no topology already ran in driverconfig. It
// returns the partitions whose declared thread arity the machine contradicts,
// keyed by name, with what would fix each.
//
// A thread arity the platform did not provide degrades that partition rather
// than the node: the machine configuration that offlines siblings is separate
// from this configuration, the two are rolled out separately, and a dataplane
// partition an operator has not finished preparing must not take the node's
// virtual machines down with it. Everything else is a hard error, because it
// describes CPUs this node cannot offer at all.
//
// A partition names whole cores, which is what makes a device's thread arity
// its own: half a core in one partition and half in another leaves neither able
// to promise anything about SMT siblings. Under smt: false the offline siblings
// do not exist to the kernel, so the surviving thread is a whole core here --
// which is why the arity check runs first, or a partition still waiting for its
// siblings to be offlined would be reported as splitting cores instead.
func validatePartitions(topo *cpuinfo.CPUTopology, onlineCPUs cpuset.CPUSet, partitions []device.Partition) (map[string]string, error) {
	degraded := make(map[string]string)
	for _, partition := range partitions {
		if offline := partition.CPUs.Difference(onlineCPUs); !offline.IsEmpty() {
			return nil, fmt.Errorf("partition %q names offline CPUs: %s", partition.Name, offline.String())
		}
		if unknown := partition.CPUs.Difference(topo.CPUDetails.CPUs()); !unknown.IsEmpty() {
			return nil, fmt.Errorf("partition %q names CPUs this node does not have: %s", partition.Name, unknown.String())
		}
		if reason := verifyThreadArity(topo, partition); reason != "" {
			degraded[partition.Name] = reason
			continue
		}
		if split := partition.CPUs.Difference(topo.CPUDetails.CompleteCores(partition.CPUs)); !split.IsEmpty() {
			return nil, fmt.Errorf("partition %q splits physical cores on %s: a partition holds whole cores, so that what it says about threads per core is true of every core in it",
				partition.Name, split.String())
		}
	}
	return degraded, nil
}

// verifyThreadArity compares a partition's declared threads per core against
// what the kernel leaves online, and returns what would make the declaration
// true, or the empty string when it already is. A partition that declared
// nothing accepts whatever the platform provides.
//
// The surplus threads are named as Talos machine.sysfs keys, which is the
// configuration that offlines them; the kernel path they set is
// /sys/devices/system/cpu/cpuN/online.
func verifyThreadArity(topo *cpuinfo.CPUTopology, partition device.Partition) string {
	if partition.ThreadsPerCore == 0 {
		return ""
	}
	surplus := cpuset.New()
	for _, cpuID := range partition.CPUs.List() {
		siblings := topo.CPUDetails.SiblingsOf(cpuID)
		if siblings.Size() <= partition.ThreadsPerCore {
			continue
		}
		// The partition keeps the threads it named; the rest of the core is what
		// the platform was asked to take offline.
		surplus = surplus.Union(siblings.Difference(partition.CPUs))
	}
	if surplus.IsEmpty() {
		return ""
	}
	keys := make([]string, 0, surplus.Size())
	for _, cpuID := range surplus.List() {
		keys = append(keys, fmt.Sprintf("devices.system.cpu.cpu%d.online: \"0\"", cpuID))
	}
	return fmt.Sprintf("partition %q expects at most %d online thread(s) per core, but %s are online too; offline them with machine.sysfs %s",
		partition.Name, partition.ThreadsPerCore, surplus.String(), strings.Join(keys, ", "))
}

// verifyOfflineAccounting compares the CPUs the kernel knows about but does not
// run against the ones the partitions asked for. A surplus is an offline CPU
// nobody declared, which is a warning rather than an error: it costs capacity
// and says the machine and this configuration disagree, but every partition
// that did state an arity has already been checked against the kernel.
func verifyOfflineAccounting(logger logr.Logger, topo *cpuinfo.CPUTopology, presentCPUs, onlineCPUs cpuset.CPUSet, partitions []device.Partition) {
	offline := presentCPUs.Difference(onlineCPUs)
	if offline.IsEmpty() {
		return
	}
	nativeThreadsPerCore := 0
	for _, cpuID := range onlineCPUs.List() {
		if threads := topo.CPUDetails.SiblingsOf(cpuID).Size(); threads > nativeThreadsPerCore {
			nativeThreadsPerCore = threads
		}
	}
	if nativeThreadsPerCore <= 1 {
		// Every core the driver can see has one thread, and an offline thread is
		// in no core, so nothing here can tell a core the platform halved from a
		// core that never had a sibling.
		logger.Info("offline CPUs cannot be accounted for: every online core has one thread, so this node's own arity is unknowable from here",
			"offline", offline.String())
		return
	}
	accounted := 0
	for _, partition := range partitions {
		if partition.ThreadsPerCore == 0 || partition.ThreadsPerCore >= nativeThreadsPerCore {
			continue
		}
		cores := topo.CPUDetails.CompleteCores(partition.CPUs).Size() / partition.ThreadsPerCore
		accounted += cores * (nativeThreadsPerCore - partition.ThreadsPerCore)
	}
	if offline.Size() == accounted {
		return
	}
	logger.Info("offline CPUs the partitions do not account for: they are lost capacity, and the machine configuration and the partition list disagree about this node",
		"offline", offline.String(), "offlineCount", offline.Size(), "accountedFor", accounted, "nativeThreadsPerCore", nativeThreadsPerCore)
}

// validateReservedCPUsAlignment checks reservedCPUs against the node when
// fullPhysicalCPUsOnly is set: a reservation splitting a physical core leaves
// the orphaned sibling neither reserved nor usable by a whole-core claim,
// silently exposing it to the shared pool where a claimless container can
// SMT-contend whatever the claim on its sibling is running.
func validateReservedCPUsAlignment(topo *cpuinfo.CPUTopology, reservedCPUs cpuset.CPUSet) error {
	if split := reservedCPUs.Difference(topo.CPUDetails.CompleteCores(reservedCPUs)); !split.IsEmpty() {
		return fmt.Errorf("reservedCPUs %q splits physical cores on %s, which fullPhysicalCPUsOnly requires never happens: reserve entire cores or none of them",
			reservedCPUs.String(), split.String())
	}
	return nil
}

// numaNodeThreadsPerCore reindexes deviceThreadsPerCore by NUMA node ID, for
// callers that plan by NUMA node and have no device name in hand (defrag). A
// NUMA node belongs to exactly one socket, so the reindex is well defined
// whether the driver groups devices by NUMA node or by socket.
func numaNodeThreadsPerCore(topo *cpuinfo.CPUTopology, groupBy string, nameToID, deviceThreadsPerCore map[string]int) map[int]int {
	byNUMANode := make(map[int]int, len(nameToID))
	switch groupBy {
	case device.GROUP_BY_NUMA_NODE:
		for name, numaID := range nameToID {
			byNUMANode[numaID] = deviceThreadsPerCore[name]
		}
	case device.GROUP_BY_SOCKET:
		bySocket := make(map[int]int, len(nameToID))
		for name, socketID := range nameToID {
			bySocket[socketID] = deviceThreadsPerCore[name]
		}
		for _, numaID := range topo.CPUDetails.NUMANodes().List() {
			anyCPU := topo.CPUDetails.CPUsInNUMANodes(numaID).UnsortedList()[0]
			byNUMANode[numaID] = bySocket[topo.CPUDetails[anyCPU].SocketID]
		}
	case device.GROUP_BY_UNCORE_CACHE:
		// Several devices share a NUMA node here, so the node has a step only
		// where they agree on one. Disagreement gives zero, which is what
		// UniformThreadsPerCore means by "no single answer" and what a caller
		// planning by NUMA node reads as no whole-core promise.
		seen := make(map[int]bool, len(nameToID))
		for name, numaID := range nameToID {
			threads := deviceThreadsPerCore[name]
			if !seen[numaID] {
				byNUMANode[numaID] = threads
				seen[numaID] = true
				continue
			}
			if byNUMANode[numaID] != threads {
				byNUMANode[numaID] = 0
			}
		}
	}
	return byNUMANode
}

// registrarDir is the kubelet plugin registration directory, always
// <kubelet-root>/plugins_registry.
func registrarDir(kubeletRootDir string) string {
	return filepath.Join(kubeletRootDir, "plugins_registry")
}

// pluginDataDir is the per-driver directory where the DRA socket is created. It
// includes the driver name because the kubeletplugin contract requires it not
// to be shared with other kubelet plugins.
func pluginDataDir(kubeletRootDir, driverName string) string {
	return filepath.Join(kubeletRootDir, "plugins", driverName)
}

// unixPathMax is the longest pathname a Unix domain socket can be bound to:
// sun_path is 108 bytes and the kernel needs the terminating NUL.
const unixPathMax = 107

// checkSocketPathFits rejects a kubelet root that leaves no room for the socket
// the kubeletplugin helper binds underneath it. The registrar path is the longer
// of the two the helper binds, so checking it covers both.
//
// Not in Config.Validate with the root's other checks because the length depends
// on the driver name, which the config does not carry. sun_path is a byte
// buffer, so this counts bytes rather than characters.
//
// The name is <driver>-reg.sock while rolling updates are off. Turning them on
// puts the UID in it, which this budget and the chart's both have to follow, and
// RegistrarSocketFilename cannot pin the name back because the two options are
// mutually exclusive.
func checkSocketPathFits(kubeletRootDir, driverName string) error {
	socket := filepath.Join(registrarDir(kubeletRootDir), driverName+"-reg.sock")
	if len(socket) > unixPathMax {
		return fmt.Errorf("kubelet registrar socket path %q is %d bytes, over the %d-byte limit for a Unix socket path: kubeletRootDir has %d bytes to spend and is using %d",
			socket, len(socket), unixPathMax,
			unixPathMax-(len(socket)-len(kubeletRootDir)), len(kubeletRootDir))
	}
	return nil
}

// Start registers the plugin with kubelet, starts the NRI plugin, and begins
// async resource publication. Setup must have been called first.
func (cp *CPUDriver) Start(ctx context.Context) (<-chan error, error) {
	ctx, logger := ctxlog.WithValues(ctx, "driver", cp.driverName)

	asyncErr := make(chan error, 1)

	if err := checkSocketPathFits(cp.kubeletRootDir, cp.driverName); err != nil {
		return asyncErr, err
	}

	driverPluginPath := pluginDataDir(cp.kubeletRootDir, cp.driverName)
	if err := os.MkdirAll(driverPluginPath, 0750); err != nil {
		return asyncErr, fmt.Errorf("failed to create plugin path %s: %w", driverPluginPath, err)
	}

	cp.reportDegradedPartitions(ctx)

	cdiMgr, err := NewCdiManager(logger, cp.driverName, cdiSpecDir)
	if err != nil {
		return asyncErr, fmt.Errorf("failed to create CDI manager: %w", err)
	}
	cp.cdiMgr = cdiMgr
	cp.seedAllocationStoreFromDisk(logger)

	kubeletOpts := []kubeletplugin.Option{
		kubeletplugin.DriverName(cp.driverName),
		kubeletplugin.NodeName(cp.nodeName),
		kubeletplugin.KubeClient(cp.kubeClient),
		kubeletplugin.RegistrarDirectoryPath(registrarDir(cp.kubeletRootDir)),
		kubeletplugin.PluginDataDirectoryPath(driverPluginPath),
		kubeletplugin.EnableDeviceMetadata(true, []schema.GroupVersion{drametadatav1beta1.SchemeGroupVersion}),
		kubeletplugin.HealthService(true),
	}
	d, err := kubeletplugin.Start(ctx, cp, kubeletOpts...)
	if err != nil {
		return asyncErr, fmt.Errorf("start kubelet plugin: %w", err)
	}
	cp.draPlugin = d
	if err := waitForRegistration(ctx, d, registrarDir(cp.kubeletRootDir), registrationPollInterval, registrationTimeout); err != nil {
		return asyncErr, err
	}

	// register the NRI plugin
	nriOpts := []stub.Option{
		stub.WithPluginName(cp.driverName),
		stub.WithPluginIdx("00"),
		// https://github.com/containerd/nri/pull/173
		// Otherwise it silently exits the program
		stub.WithOnClose(func() {
			logger.Info("NRI plugin closed")
		}),
	}
	stub, err := stub.New(cp, nriOpts...)
	if err != nil {
		return asyncErr, fmt.Errorf("failed to create plugin stub: %w", err)
	}
	cp.nriPlugin = stub
	// CCX-FORK: upstream starts no worker here and never pushes an update the
	// runtime did not ask for, so it hands the stub to nothing.
	if cp.reconcileTrigger != nil {
		cp.containerUpdater = stub
		go cp.runReconcileWorker(ctx)
	}

	go func() {
		if err := runNRIPluginWithRetry(ctx, cp.nriPlugin, maxAttempts, nriRetryInitialBackoff, nriRetryMaxBackoff, nriRetryHealthyRunDuration); err != nil && ctx.Err() == nil {
			logger.Error(err, "NRI plugin failed to be restarted", "maxAttempts", maxAttempts)
			asyncErr <- err
		}
	}()

	// publish available resources
	go cp.PublishResources(ctx)

	// periodically (every healthResendInterval) resend device health so
	// the kubelet's lease on it does not expire (see WatchHealthStatus in
	// health.go)
	go cp.healthResendLoop(ctx)

	return asyncErr, nil
}

// reportDegradedPartitions puts each withheld partition on the node's own event
// stream, where an operator looking at the node they just configured will find
// it. The driver's log says the same thing, but only to whoever thinks to read
// a DaemonSet pod's log on the right node.
func (cp *CPUDriver) reportDegradedPartitions(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	if cp.kubeClient == nil {
		return
	}
	for _, partition := range slices.Sorted(maps.Keys(cp.degradedPartitions)) {
		event := &v1.Event{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: cp.nodeName + ".",
				Namespace:    metav1.NamespaceDefault,
			},
			// A Node has no namespace and the driver holds no reference to the
			// object, so this is the reference the kubelet itself writes for
			// node events: kind and name, with the name as the UID.
			InvolvedObject: v1.ObjectReference{Kind: "Node", Name: cp.nodeName, UID: types.UID(cp.nodeName)},
			Reason:         "CPUPartitionDegraded",
			Message:        cp.degradedPartitions[partition],
			Type:           v1.EventTypeWarning,
			Source:         v1.EventSource{Component: cp.driverName, Host: cp.nodeName},
			FirstTimestamp: metav1.Now(),
			LastTimestamp:  metav1.Now(),
			Count:          1,
		}
		if _, err := cp.kubeClient.CoreV1().Events(metav1.NamespaceDefault).Create(ctx, event, metav1.CreateOptions{}); err != nil {
			logger.Error(err, "cannot report a degraded partition as an event", "partition", partition)
		}
	}
}

// seedAllocationStoreFromDisk recovers claim allocations from CDI specs on
// disk before the kubelet plugin below registers and can replay Prepare
// calls its own checkpoint remembers but this driver's in-memory store does
// not. It only prevents a new allocation from colliding with an
// already-recorded one; reconciling against the runtime's actual committed
// state remains Synchronize's job (C38: CDI specs alone are not a safe
// general convergence source).
func (cp *CPUDriver) seedAllocationStoreFromDisk(logger logr.Logger) {
	if err := cp.cdiMgr.Refresh(); err != nil {
		logger.Error(err, "cannot seed the allocation store from disk: CDI cache refresh failed")
		return
	}
	for claimUID, requests := range cp.cdiMgr.PreparedClaimAllocations(logger) {
		cLogger := logger.WithValues("claimUID", claimUID)
		if err := cp.cpuAllocationStore.ReserveResourceClaimAllocation(cLogger, claimUID, requests, false); err != nil {
			cLogger.Error(err, "ignoring a recorded claim allocation inconsistent with another one during startup recovery")
			continue
		}
		cLogger.Info("recovered claim allocation from disk", "cpus", store.UnionOf(requests).String())
	}
	cp.refreshAllocationMetrics()
}

// Stop stops the CPUDriver.
func (cp *CPUDriver) Stop() {
	cp.health.Stop()
	cp.nriPlugin.Stop()
	cp.draPlugin.Stop()
}

func getDeviceAttributes(deviceSlices [][]resourceapi.Device, deviceName string) (map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, bool) {
	for _, slice := range deviceSlices {
		for _, dev := range slice {
			if dev.Name == deviceName {
				return dev.Attributes, true
			}
		}
	}
	return nil, false
}

// Shutdown is called when the runtime is shutting down.
func (cp *CPUDriver) Shutdown(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	logger.Info("runtime shutting down")
}

// waitForRegistration waits for kubelet to report the plugin as registered. On timeout
// it reports the last reason kubelet gave for refusing, or that it never reported at all.
func waitForRegistration(ctx context.Context, p KubeletPlugin, registrarPath string, interval, timeout time.Duration) error {
	logger := ctxlog.FromContext(ctx)
	var lastRejection string
	var sawStatus bool
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(context.Context) (bool, error) {
		status := p.RegistrationStatus()
		if status == nil {
			return false, nil
		}
		sawStatus = true
		// Kubelet retries from scratch, so keep the newest reason but do not stop here.
		// Only the newest survives to the error, so an earlier one is logged instead of lost.
		if status.Error != "" && status.Error != lastRejection {
			if lastRejection != "" {
				logger.Info("kubelet gave a new reason for refusing the plugin", "previous", lastRejection, "current", status.Error)
			}
			lastRejection = status.Error
		}
		return status.PluginRegistered, nil
	})
	// Only a timeout is worth explaining; a cancelled context is the caller shutting down.
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case lastRejection != "":
		return fmt.Errorf("kubelet did not register the plugin, last rejection was %q: %w", lastRejection, err)
	case sawStatus:
		return fmt.Errorf("kubelet did not register the plugin and reported no reason: %w", err)
	default:
		return fmt.Errorf("kubelet never reported a registration status, check that it watches %s: %w", registrarPath, err)
	}
}

type nriRunner interface {
	Run(context.Context) error
}

const (
	nriRetryInitialBackoff     = 1 * time.Second
	nriRetryMaxBackoff         = 30 * time.Second
	nriRetryHealthyRunDuration = nriRetryMaxBackoff
)

// runNRIPluginWithRetry keeps plugin connected, backing off between attempts
// so a down socket cannot burn through maxAttempts in microseconds. backoff
// doubles on each failure up to maxBackoff; a connection lasting at least
// healthyRunDuration resets both, so a past crash loop does not spend the
// budget a fresh failure needs.
func runNRIPluginWithRetry(ctx context.Context, plugin nriRunner, maxAttempts int, initialBackoff, maxBackoff, healthyRunDuration time.Duration) error {
	logger := ctxlog.FromContext(ctx)
	backoff := initialBackoff
	attempts := 0
	for {
		started := time.Now()
		err := plugin.Run(ctx)
		if ctx.Err() != nil {
			logger.Info("NRI plugin stopped", "reason", "context cancelled")
			return ctx.Err()
		}
		if time.Since(started) >= healthyRunDuration {
			attempts = 0
			backoff = initialBackoff
		}
		attempts++
		if err != nil {
			logger.Error(err, "NRI plugin failed, restarting", "attempt", attempts, "maxAttempts", maxAttempts, "backoff", backoff)
		}
		if attempts >= maxAttempts {
			return fmt.Errorf("NRI plugin failed %d times within %s, giving up", attempts, healthyRunDuration)
		}

		select {
		case <-ctx.Done():
			logger.Info("NRI plugin stopped", "reason", "context cancelled")
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// generateShortID generates a non-crypto safe unique ID in cases on which a full UUID would be a overkill.
func generateShortID(length int) string {
	const hexDigits = "0123456789abcdef"
	b := make([]byte, length)
	for i := range b {
		b[i] = hexDigits[rand.IntN(len(hexDigits))] //nolint:gosec
	}
	return string(b)
}
