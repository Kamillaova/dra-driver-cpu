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
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/nri/pkg/stub"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuallocator"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/sysfs"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	drametadatav1alpha1 "k8s.io/dynamic-resource-allocation/api/metadata/v1alpha1"
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
	// initialSyncWait bounds how long Prepare waits for the first Synchronize
	// after a restart. It covers the NRI connection plus the 2s NRI request
	// timeout, and stays well under kubelet's 45s DRA call timeout.
	initialSyncWait = 5 * time.Second
	// profileReadTimeout bounds each read of the node's profile label, and
	// profileReadInterval spaces the reads while the API server does not answer.
	profileReadTimeout  = 10 * time.Second
	profileReadInterval = 2 * time.Second
)

const opIDLen = 8

// KubeletPlugin is an interface that describes the methods used from kubeletplugin.Helper.
type KubeletPlugin interface {
	PublishResources(context.Context, resourceslice.DriverResources) error
	RegistrationStatus() *registerapi.RegistrationStatus
	Stop()
}

type cdiManager interface {
	AddDevice(logger logr.Logger, deviceName string, envVar string) error
	AddClaimDevices(logger logr.Logger, claimUID types.UID, envByDevice map[string]string, record cdiClaimRecord) error
	Refresh() error
	GetDeviceAllocations(claimUID types.UID) (cdiClaimRecord, error)
	ListClaims() []types.UID
	RemoveClaimDevices(logger logr.Logger, claimUID types.UID) error
}

// CPUInfoProvider is an interface for getting CPU information.
type CPUInfoProvider interface {
	GetCPUTopology(logger logr.Logger) (*cpuinfo.CPUTopology, error)
}

// CPUAllocator abstracts the behavior of a cpu allocator code. The caller MUST call
// * Allocate - to get the optimal (according to the backend policy) set of the given size within the available set
// * Validate - to ensure the optimal set returned by Allocate is consistent with in-flight and consolidated allocations
// GetPreferredCPUs is optional and allows the users (or external scheduler) to specify which available CPUs should
// be prioritized in the current allocation request.
// The caller CAN call it, and if it does, it MUST call it first, before Allocate; the caller should then feed the
// preferred CPUs to Allocate(). If the caller doesn't need or want to call GetPreferredCPUs, it must use an empty
// CPU set where preferred CPUs are required.
type CPUAllocator interface {
	// GetPreferredCPUs retrieves the allocation hint for the current allocation cycle.
	// An allocation hint is a CPU set that the allocator must honor: all CPUs allocated in this cycle must belong to
	// that set, or the allocation fails; if there are more preferred CPUs than required, the allocator is free
	// to pick any subset that fulfills the request. If there are less preferred CPUs than required, the allocator
	// is free to pick extra CPUs as it sees fit.
	// The preferred CPU set is per-request. Should the allocation result split across different results (e.g. count > 1),
	// The allocator must ensure that each allocation result is a non-overlapping subset of the preferred set.
	// The allocator can reject any hint entirely by design. If the allocator rejects hint entirely, it must
	// return cpuallocator.ErrUnsupportedPreferredCPUs.
	// If the allocator fails to fetch a hint, or detects an illegal hint, it must return a different custom error.
	GetPreferredCPUs(logger logr.Logger, allocation *resourceapi.AllocationResult, alloc resourceapi.DeviceRequestAllocationResult) (cpuset.CPUSet, error)
	// Allocate reserves <count> CPUs for the claim from the intersection of preferredCPUs and availableCPUs.
	// The allocator can pick CPUs from the provided set according to its own implementation, the result of each Allocate
	// call must be idempotent (calls with the same parameters must yield the same result).
	// if preferredCPUs is empty, the allocator is free to pick CPUs from availableCPUs according to its implementation.
	Allocate(logger logr.Logger, availableCPUs, preferredCPUs cpuset.CPUSet, count int) (cpuset.CPUSet, error)
	// Validate verifies performed allocation (cpus) is consistent with all the other active
	// claims already prepared on this node (preparedCPUs) and with the other allocations
	// performed in the same batch (assignedCPUs, claim asking for two or more allocations).
	// Returns error detailing the inconsistency, nil if the allocation is correct.
	Validate(cpus, assignedCPUs, preparedCPUs cpuset.CPUSet) error
}

// CPUDriver is the structure that holds all the driver runtime information.
type CPUDriver struct {
	// stateMu protects the store pointers and complete DRA/NRI state operations.
	// Acquire it before store locks; never call into the runtime while holding it.
	stateMu         timedMutex
	hasSynchronized bool
	// nriDisconnected is set when the NRI plugin's Run returns and cleared by
	// Synchronize, both under stateMu; Ready reads it without the lock. Until
	// the first Synchronize, hasSynchronized and initialSync cover it.
	nriDisconnected atomic.Bool
	// initialSync is closed when hasSynchronized first becomes true.
	initialSync     chan struct{}
	initialSyncWait time.Duration
	// runtimeReportsCDIDevices is whether the runtime NRI last configured the
	// plugin for fills in the CDI devices it injects into a container.
	runtimeReportsCDIDevices bool

	driverName              string
	nodeName                string
	kubeClient              kubernetes.Interface
	draPlugin               KubeletPlugin
	nriPlugin               stub.Stub
	cancelNRI               context.CancelFunc
	cancelRun               context.CancelCauseFunc
	podState                *store.PodState
	allocationState         *store.AllocationState
	cdiMgr                  cdiManager
	topology                deviceTopology
	cpuDeviceMode           string
	cpuDeviceGroupBy        string
	pcieRootMapper          *store.PCIeRootMapper
	devicesPerResourceSlice int
	metrics                 Recorder
	eventBroadcaster        record.EventBroadcaster
	eventRecorder           record.EventRecorder
	health                  healthTracker
	watchdog                watchdog
	kubeletRootDir          string
	cpuAllocator            CPUAllocator
	fullPhysicalCPUsOnly    bool
	// allowFractionalCapacityRequests is whether the shared devices book
	// amounts in millicores.
	allowFractionalCapacityRequests bool
	// wholeCorePicker is set under fullPhysicalCPUsOnly where the driver picks
	// the CPUs itself, which an external allocator's cpuset leaves it no room to.
	wholeCorePicker bool
	// reconcileTrigger is set under AssumeUnsolicitedUpdatesSafe and carries
	// coalesced requests to the worker that sends containerUpdater's updates.
	reconcileTrigger chan struct{}
	containerUpdater containerUpdater
	nodeConditions   *nodeConditionWriter
	// allocatableMappingDropped is never cleared: once the API server drops a
	// field, the plugin library stops sending it until the driver restarts.
	allocatableMappingDropped atomic.Bool
	// degradedPartitions are the partitions whose smt the kernel contradicts,
	// each with the contradiction. Set in configure, read-only after that.
	degradedPartitions map[string]string
	// taintDegradedPartitions is whether the degraded partitions publish their
	// devices tainted. Set in configure, read-only after that.
	taintDegradedPartitions bool
	// deviceTaintsDropped is never cleared: once the API server drops a field,
	// the plugin library stops sending it until the driver restarts.
	// taintsWithheld is set once the devices whose taint it dropped are
	// withheld.
	deviceTaintsDropped atomic.Bool
	taintsWithheld      atomic.Bool
	// reservedPartitions are the partitions of role reserved. Set in
	// configure, read-only after that.
	reservedPartitions []device.Partition
	// reservedHolds are the claims restored at start whose exclusive CPUs a
	// reserved partition holds, kept under stateMu until each is unprepared.
	// reservedHoldsClaims is whether there is one, for Ready, which takes no
	// lock.
	reservedHolds       map[types.UID]reservedHold
	reservedHoldsClaims atomic.Bool

	// window is set, under stateMu, while the node's profile is not read yet;
	// see Config.SelectProfile.
	window *unconfiguredWindow
	// reconnecting marks the next return of the NRI plugin's Run as the
	// driver's own reconnect, and resynced, set under stateMu, is closed by the
	// Synchronize that follows it. nriClosed receives a signal each time the
	// NRI stub's close callback has run.
	reconnecting        atomic.Bool
	resynced            chan struct{}
	nriClosed           chan struct{}
	providers           Providers
	profileLabel        string
	selectProfile       func(name string) (*Config, error)
	profileReadTimeout  time.Duration
	profileReadInterval time.Duration
	// profileName is the profile the node's label names, and empty where the
	// configuration declares no profiles.
	profileName string

	// reservation is the kubelet CPU reservation the node's partitions need.
	// Set in configure, read-only after that.
	reservation             reservationRequirement
	reservationReadTimeout  time.Duration
	reservationReadInterval time.Duration
	// reservationMissing is the last check's result, set under stateMu; Ready
	// reads it without the lock. reservedMilliCPU and reservationChecked are
	// kept under stateMu.
	reservationMissing atomic.Bool
	reservedMilliCPU   int64
	reservationChecked bool
	// publishMu orders the publishes, so that the last one sent carries the
	// last check's result.
	publishMu     sync.Mutex
	stopNodeWatch func()
	// nodeWatchDone is closed when the Node watch has stopped.
	nodeWatchDone <-chan struct{}
}

// deviceHealthEntry is the last known health of a single device.
type deviceHealthEntry struct {
	status  kubeletplugin.HealthStatus
	message string
}

// deviceTopology holds the CPU topology and device-to-CPU/socket/NUMA
// mappings. Set once in New(), read-only after that.
type deviceTopology struct {
	device.Inventory
	device.Mapping
	deviceSlices   [][]resourceapi.Device
	threadsPerCore map[string]int
	// partitionCPUs is the CPUs of each published device's partition, by
	// device name, and nil where the configuration uses no partitions.
	partitionCPUs map[string]cpuset.CPUSet
	// sharedDevices is the CPUs of each published device whose CPUs its
	// claims share, by device name.
	sharedDevices map[string]cpuset.CPUSet
	// degradedDevices is the partition of each device of a partition whose
	// smt the kernel contradicts, published tainted or withheld, by device
	// name.
	degradedDevices map[string]string
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
	Allocator        string
	ExposePCIeRoots  bool
	Metrics          Recorder
	// KubeletRootDir is the kubelet root directory, from which the registrar
	// and plugin data directories are derived. Required and absolute:
	// driverconfig.Resolve refuses an empty or relative value, and New takes it as
	// given rather than checking it again.
	KubeletRootDir string
	// PublishNodeAllocatableResourceMapping publishes KEP-5517 nodeAllocatableResources mappings in
	// ResourceSlice devices. Requires the DRANodeAllocatableResources feature gate to be enabled in the cluster.
	PublishNodeAllocatableResourceMapping bool
	// FullPhysicalCPUsOnly is driverconfig.Config.FullPhysicalCPUsOnly.
	FullPhysicalCPUsOnly bool
	// CPUPartitions is driverconfig.Config.Partitions: the declared partitions,
	// empty where the configuration uses none.
	CPUPartitions []device.Partition
	// AssumeUnsolicitedUpdatesSafe is driverconfig.Config.AssumeUnsolicitedUpdatesSafe.
	AssumeUnsolicitedUpdatesSafe bool
	// AllowFractionalCapacityRequests is driverconfig.Config.AllowFractionalCapacityRequests.
	AllowFractionalCapacityRequests bool
	// TaintDegradedPartitions is driverconfig.Config.TaintDegradedPartitions.
	TaintDegradedPartitions bool
	// CancelRun cancels the context given to Start, which refuses to run
	// without it. HandleError calls it with a cause wrapping ErrFatal, so that
	// the caller stops the driver.
	CancelRun context.CancelCauseFunc
	// ProfileLabel is the node label naming a profile. Start, once connected to
	// NRI, reads it before it registers with the kubelet, and without
	// SelectProfile refuses a node that carries it.
	ProfileLabel string
	// SelectProfile is set where the configuration declares profiles. New then
	// builds only what no profile decides, and Start builds the driver from the
	// Config SelectProfile returns for the node's ProfileLabel label, and only
	// then registers with the kubelet. An error from SelectProfile stops Start.
	SelectProfile func(name string) (*Config, error)
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
// With config.SelectProfile set, the state a profile decides is prepared by
// Start instead, once it has read the node's profile.
func New(logger logr.Logger, providers Providers, config *Config) (*CPUDriver, error) {
	logger = logger.WithValues("driver", config.DriverName)

	metricsRecorder := config.Metrics
	if metricsRecorder == nil {
		metricsRecorder = cpumetrics.Noop()
	}
	plugin := &CPUDriver{
		initialSync:         make(chan struct{}),
		initialSyncWait:     initialSyncWait,
		nriClosed:           make(chan struct{}, 1),
		driverName:          config.DriverName,
		nodeName:            config.NodeName,
		kubeClient:          providers.K8SClient,
		metrics:             metricsRecorder,
		health:              newHealthTracker(),
		kubeletRootDir:      config.KubeletRootDir,
		cancelRun:           config.CancelRun,
		providers:           providers,
		profileLabel:        config.ProfileLabel,
		selectProfile:       config.SelectProfile,
		profileReadTimeout:  profileReadTimeout,
		profileReadInterval: profileReadInterval,

		reservationReadTimeout:  reservationReadTimeout,
		reservationReadInterval: reservationReadInterval,
	}
	if config.AssumeUnsolicitedUpdatesSafe {
		plugin.reconcileTrigger = make(chan struct{}, 1)
	}
	if config.SelectProfile != nil {
		plugin.window = newUnconfiguredWindow()
		return plugin, nil
	}
	if err := plugin.configure(logger, providers, config); err != nil {
		return nil, err
	}
	return plugin, nil
}

// configure prepares the state that depends on the node's configuration: the
// topology, the allocator, the stores and the devices.
func (cp *CPUDriver) configure(logger logr.Logger, providers Providers, config *Config) error {
	cp.topology = deviceTopology{
		Inventory: device.Inventory{
			ReservedCPUs: config.ReservedCPUs,
		},
	}
	cp.cpuDeviceMode = config.CPUDeviceMode
	cp.cpuDeviceGroupBy = config.CPUDeviceGroupBy
	cp.pcieRootMapper = store.NewPCIeRootMapper()
	cp.devicesPerResourceSlice = config.DevicesPerResourceSlice()
	var conditions []nodeConditionSource
	if config.PublishNodeAllocatableResourceMapping {
		conditions = append(conditions, cp.allocatableMappingCondition())
	}
	if len(config.CPUPartitions) > 0 {
		conditions = append(conditions, cp.partitionDegradedCondition())
	}
	topo, err := providers.EnsureCPUInfo().GetCPUTopology(logger)
	if err != nil {
		return fmt.Errorf("failed to get CPU topology: %w", err)
	}
	if topo == nil {
		return fmt.Errorf("failed to get CPU topology: topology is nil")
	}
	cp.topology.CPUTopology = topo
	managedCPUs := cp.topology.ManagedCPUs()
	logger.V(2).Info("detected topology-validated CPUs", "cpus", managedCPUs.String())

	partitions, degraded, err := resolvePartitions(logger, providers.EnsureSysFS(), topo, config.CPUPartitions, config.ReservedCPUs)
	if err != nil {
		return err
	}
	cp.degradedPartitions = degraded
	cp.reservation = requiredReservation(topo.CPUDetails, partitions, len(config.CPUPartitions) > 0)
	if cp.reservation.cpus > 0 {
		logger.Info("the node's partitions need a kubelet CPU reservation", "requiredCPUs", cp.reservation.cpus)
		conditions = append(conditions, cp.reservationCondition())
	} else {
		cp.metrics.SetNodeReservationOK(true)
	}
	cp.nodeConditions = newNodeConditionWriter(providers.K8SClient, config.NodeName, &cp.stateMu, conditions,
		workqueue.DefaultTypedControllerRateLimiter[string]())
	cp.reservedPartitions = nil
	if len(config.CPUPartitions) > 0 {
		for _, partition := range partitions {
			if partition.IsReserved() {
				cp.reservedPartitions = append(cp.reservedPartitions, partition)
			}
		}
	}
	cp.taintDegradedPartitions = config.TaintDegradedPartitions
	withheld := make(map[string]bool, len(degraded))
	tainted := make(map[string]bool, len(degraded))
	if len(config.CPUPartitions) > 0 {
		for _, partition := range partitions {
			_, contradicted := degraded[partition.Name]
			withheld[partition.Name] = contradicted && !config.TaintDegradedPartitions
			tainted[partition.Name] = contradicted && config.TaintDegradedPartitions
			cp.metrics.SetPartitionVerified(partition.Name, !contradicted)
		}
	}
	cp.topology.ReservedCPUs = config.ReservedCPUs.Union(unclaimablePartitionCPUs(partitions))

	cp.fullPhysicalCPUsOnly = config.FullPhysicalCPUsOnly
	if config.FullPhysicalCPUsOnly {
		if err := validateReservedCPUsAlignment(topo, config.ReservedCPUs); err != nil {
			return err
		}
	}

	if config.ExposePCIeRoots {
		if err := cp.pcieRootMapper.Probe(logger, providers.EnsureSysFS(), managedCPUs); err != nil {
			return fmt.Errorf("failed to list PCIe domains: %w", err)
		}
	}

	cp.allocationState = store.NewAllocationState(cp.topology.CPUTopology, cp.topology.ReservedCPUs)
	if len(config.CPUPartitions) > 0 {
		cp.allocationState.SetClaimlessCPUs(defaultPartitionCPUs(partitions))
	}
	cp.refreshAllocationMetrics()
	cp.podState = store.NewPodState()

	exposeExtAttrs := false
	allocator := cpuallocator.ModeCPUManager
	logger.Info("creating CPU allocator", "method", config.Allocator)
	switch config.Allocator {
	case cpuallocator.ModeExternal:
		external := cpuallocator.NewExternal(config.DriverName, managedCPUs, config.ReservedCPUs)
		if config.FullPhysicalCPUsOnly {
			external.RequireWholeCores(topo)
		}
		cp.cpuAllocator = external
		exposeExtAttrs = true
		allocator = cpuallocator.ModeExternal
	default:
		cp.cpuAllocator = cpuallocator.NewCPUManager(config.DriverName, topo)
		cp.wholeCorePicker = config.FullPhysicalCPUsOnly
	}

	result, err := device.Build(device.BuildInput{
		Inventory:                             cp.topology.Inventory,
		Layout:                                device.FindLayout(cp.cpuDeviceMode, cp.cpuDeviceGroupBy),
		PCIeRootMapper:                        cp.pcieRootMapper,
		PublishNodeAllocatableResourceMapping: config.PublishNodeAllocatableResourceMapping,
		ExposeExtAttrs:                        exposeExtAttrs,
		FullPhysicalCPUsOnly:                  config.FullPhysicalCPUsOnly,
		Allocator:                             allocator,
		Partitions:                            partitions,
		PartitionsUsed:                        len(config.CPUPartitions) > 0,
		Withheld:                              withheld,
		Tainted:                               tainted,
		AllowFractionalCapacityRequests:       config.AllowFractionalCapacityRequests,
	})
	if err != nil {
		return err
	}
	if config.FullPhysicalCPUsOnly {
		for _, name := range slices.Sorted(maps.Keys(result.MixedCores)) {
			counts := result.MixedCores[name]
			logger.Info("fullPhysicalCPUsOnly publishes no request policy on a device whose cores differ in threads",
				"device", name, "coresWithTwoThreads", counts.TwoThreads, "coresWithOneThread", counts.OneThread)
		}
	}

	cp.topology.Mapping = result.Mapping
	cp.topology.threadsPerCore = result.ThreadsPerCore
	if len(config.CPUPartitions) > 0 {
		cp.topology.partitionCPUs = make(map[string]cpuset.CPUSet, len(result.DevicePartitions))
		for name, partition := range result.DevicePartitions {
			cp.topology.partitionCPUs[name] = partition.CPUs
		}
	}
	cp.allowFractionalCapacityRequests = config.AllowFractionalCapacityRequests
	cp.topology.sharedDevices = make(map[string]cpuset.CPUSet, len(result.Shared))
	for name := range result.Shared {
		cpus, ok := result.Mapping.NameToCPUs[name]
		if !ok {
			cpus = result.DevicePartitions[name].CPUs
		}
		cp.topology.sharedDevices[name] = cpus
	}
	cp.allocationState.SetSharedDevices(cp.topology.sharedDevices)
	cp.topology.degradedDevices = make(map[string]string, len(result.WithheldDevices))
	maps.Copy(cp.topology.degradedDevices, result.WithheldDevices)
	for name, partition := range result.DevicePartitions {
		if tainted[partition.Name] {
			cp.topology.degradedDevices[name] = partition.Name
		}
	}
	cp.topology.deviceSlices = device.ChunkDevices(result.DevicesByPartition, cp.devicesPerResourceSlice)
	logger.Info("chunked devices into ResourceSlices", "numDevices", len(result.Devices),
		"devicesPerResourceSlice", cp.devicesPerResourceSlice, "numResourceSlices", len(cp.topology.deviceSlices),
		"exposePCIeRoots", config.ExposePCIeRoots)

	for _, d := range result.Devices {
		cp.health.devices[d.Name] = &deviceHealthEntry{
			status:  kubeletplugin.HealthStatusHealthy,
			message: "device initialized",
		}
	}

	return nil
}

// validateReservedCPUsAlignment refuses a reservation that holds part of a
// physical core: the rest of the core could back no claim under
// fullPhysicalCPUsOnly and would share the core with the reserved threads.
func validateReservedCPUsAlignment(topo *cpuinfo.CPUTopology, reservedCPUs cpuset.CPUSet) error {
	reserved := reservedCPUs.Intersection(topo.CPUDetails.CPUs())
	if split := reserved.Difference(topo.CPUDetails.CompleteCores(reserved)); !split.IsEmpty() {
		return fmt.Errorf("reservedCPUs %q splits physical cores, which fullPhysicalCPUsOnly does not allow: reserve the sibling threads of CPUs %s too, or leave those CPUs out",
			reservedCPUs.String(), split.String())
	}
	return nil
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

// Start connects to NRI, reads the node's profile label, registers the plugin
// with kubelet, and begins async resource publication. Setup must have been
// called first. Where the node's profile is still to be read, the driver is
// built from it before the registration.
func (cp *CPUDriver) Start(ctx context.Context) (<-chan error, error) {
	ctx, logger := ctxlog.WithValues(ctx, "driver", cp.driverName)

	asyncErr := make(chan error, 1)

	if err := checkSocketPathFits(cp.kubeletRootDir, cp.driverName); err != nil {
		return asyncErr, err
	}
	if cp.cancelRun == nil {
		return asyncErr, errors.New("driver Config.CancelRun is not set: HandleError needs it to stop the driver on a fatal error")
	}

	driverPluginPath := pluginDataDir(cp.kubeletRootDir, cp.driverName)
	if err := os.MkdirAll(driverPluginPath, 0750); err != nil {
		return asyncErr, fmt.Errorf("failed to create plugin path %s: %w", driverPluginPath, err)
	}

	cdiMgr, err := NewCdiManager(logger, cp.driverName, cdiSpecDir)
	if err != nil {
		return asyncErr, fmt.Errorf("failed to create CDI manager: %w", err)
	}
	cp.cdiMgr = cdiMgr

	if cp.window != nil {
		return asyncErr, cp.startWithProfile(ctx, asyncErr)
	}

	cp.seedAllocationStoreFromDisk(logger)
	go cp.runWatchdog(ctx)
	cp.eventBroadcaster, cp.eventRecorder = newEventRecorder(ctx, cp.kubeClient, cp.driverName, cp.nodeName)

	if err := cp.startNRI(ctx, asyncErr); err != nil {
		return asyncErr, err
	}
	if err := cp.refuseProfileLabel(ctx, asyncErr); err != nil {
		return asyncErr, err
	}
	if err := cp.awaitReservation(ctx, asyncErr); err != nil {
		return asyncErr, err
	}
	if err := cp.register(ctx); err != nil {
		return asyncErr, err
	}
	cp.serve(ctx)

	return asyncErr, nil
}

// startWithProfile serves NRI unconfigured until the node's profile is read,
// then builds the driver from it, narrows the containers started meanwhile and
// registers with the kubelet.
func (cp *CPUDriver) startWithProfile(ctx context.Context, asyncErr chan error) error {
	go cp.runWatchdog(ctx)
	if err := cp.startNRI(ctx, asyncErr); err != nil {
		return err
	}
	bound, err := cp.configureFromProfile(ctx, asyncErr)
	if err != nil {
		return err
	}
	if err := cp.narrowWindowContainers(ctx, asyncErr, bound); err != nil {
		return err
	}
	if err := cp.awaitReservation(ctx, asyncErr); err != nil {
		return err
	}
	if err := cp.register(ctx); err != nil {
		return err
	}
	cp.serve(ctx)
	return nil
}

// register starts the kubelet plugin and waits for the kubelet to register it.
func (cp *CPUDriver) register(ctx context.Context) error {
	kubeletOpts := []kubeletplugin.Option{
		kubeletplugin.DriverName(cp.driverName),
		kubeletplugin.NodeName(cp.nodeName),
		kubeletplugin.KubeClient(cp.kubeClient),
		kubeletplugin.RegistrarDirectoryPath(registrarDir(cp.kubeletRootDir)),
		kubeletplugin.PluginDataDirectoryPath(pluginDataDir(cp.kubeletRootDir, cp.driverName)),
		kubeletplugin.EnableDeviceMetadata(true, []schema.GroupVersion{
			drametadatav1beta1.SchemeGroupVersion,
			drametadatav1alpha1.SchemeGroupVersion,
		}),
		kubeletplugin.HealthService(true),
	}
	d, err := kubeletplugin.Start(ctx, cp, kubeletOpts...)
	if err != nil {
		return fmt.Errorf("start kubelet plugin: %w", err)
	}
	cp.draPlugin = d
	return waitForRegistration(ctx, d, registrarDir(cp.kubeletRootDir), registrationPollInterval, registrationTimeout)
}

// startNRI creates the NRI plugin and keeps it connected.
func (cp *CPUDriver) startNRI(ctx context.Context, asyncErr chan<- error) error {
	logger := ctxlog.FromContext(ctx)
	nriOpts := []stub.Option{
		stub.WithPluginName(cp.driverName),
		stub.WithPluginIdx("00"),
		// https://github.com/containerd/nri/pull/173
		// Otherwise it silently exits the program
		stub.WithOnClose(func() {
			logger.Info("NRI plugin closed")
			cp.signalNRIClosed()
		}),
	}
	stub, err := stub.New(cp, nriOpts...)
	if err != nil {
		return fmt.Errorf("failed to create plugin stub: %w", err)
	}
	cp.nriPlugin = stub
	cp.startNRIPlugin(ctx, asyncErr)
	return nil
}

// serve starts the work of a registered driver: the Node conditions, the
// published resources and the device health, and where the partitions need a
// kubelet CPU reservation, the watch of the Node that checks it again.
func (cp *CPUDriver) serve(ctx context.Context) {
	cp.nodeConditions.run(ctx, cp.initialSync)
	if cp.reservation.cpus > 0 {
		cp.watchNode(ctx)
	}

	// publish available resources
	go cp.PublishResources(ctx)

	// periodically (every healthResendInterval) resend device health so
	// the kubelet's lease on it does not expire (see WatchHealthStatus in
	// health.go)
	go cp.healthResendLoop(ctx)
}

// seedAllocationStoreFromDisk restores the record of every claim whose CDI spec
// is on disk, before the kubelet or the runtime can call the driver: a claim
// prepared before a restart and named by no container yet would otherwise look
// free to the next Prepare. Restore, unlike Reserve, keeps two records that name
// the same CPUs, and Bind refuses both claims until one is unprepared.
func (cp *CPUDriver) seedAllocationStoreFromDisk(logger logr.Logger) {
	if err := cp.cdiMgr.Refresh(); err != nil {
		logger.Error(err, "failed to refresh CDI cache, continuing with available CDI devices")
	}
	restored := make(map[types.UID]store.ClaimRecord)
	for _, uid := range cp.cdiMgr.ListClaims() {
		caLogger := logger.WithValues("claimUID", uid)
		record, err := cp.cdiMgr.GetDeviceAllocations(uid)
		if err != nil {
			caLogger.Error(err, "not restoring claim with an unusable CDI spec")
			continue
		}
		restored[uid] = record.storeRecord()
		cp.allocationState.Restore(caLogger, uid, restored[uid])
		cp.reportMisplacedClaim(caLogger, record)
	}
	cp.recordReservedHolds(logger, restored)
	cp.refreshAllocationMetrics()
}

// reportMisplacedClaim counts a restored claim that holds CPUs outside the
// partition of a device its record names, or names a device this node no
// longer publishes: the partitions changed while it was prepared. It keeps
// its CPUs until it is unprepared.
func (cp *CPUDriver) reportMisplacedClaim(logger logr.Logger, record cdiClaimRecord) {
	if cp.topology.partitionCPUs == nil {
		return
	}
	for _, request := range record.Requests {
		for _, result := range request.Results {
			partitionCPUs, published := cp.topology.partitionCPUs[result.Device]
			if published && request.CPUs.IsSubsetOf(partitionCPUs) {
				continue
			}
			logger.Info("restored claim holds CPUs outside the partition of its device, and keeps them until it is unprepared",
				"claim", ctxlog.ObjectRef{Namespace: record.Namespace, Name: record.Name}, "request", request.Request,
				"cpus", request.CPUs.String(), "device", result.Device, "devicePublished", published)
			cp.metrics.RecordMisplacedClaim()
			return
		}
	}
}

// startNRIPlugin keeps the NRI plugin connected until Stop, not until ctx is
// cancelled, so that containers created while the DRA plugin shuts down still
// get their CPUs.
func (cp *CPUDriver) startNRIPlugin(ctx context.Context, asyncErr chan<- error) {
	logger := ctxlog.FromContext(ctx)
	ctx, cp.cancelNRI = context.WithCancel(context.WithoutCancel(ctx))
	if cp.reconcileTrigger != nil {
		cp.containerUpdater = cp.nriPlugin
		go cp.runReconcileWorker(ctx)
	}
	go func() {
		if err := runNRIPluginWithRetry(ctx, nriConnection{driver: cp, plugin: cp.nriPlugin}, maxAttempts, nriRetryInitialBackoff, nriRetryMaxBackoff, nriRetryHealthyRunDuration); err != nil && ctx.Err() == nil {
			logger.Error(err, "NRI plugin failed to be restarted", "maxAttempts", maxAttempts)
			asyncErr <- err
		}
	}()
}

// signalNRIClosed is the NRI stub's close callback.
func (cp *CPUDriver) signalNRIClosed() {
	select {
	case cp.nriClosed <- struct{}{}:
	default:
	}
}

// Stop stops the CPUDriver.
func (cp *CPUDriver) Stop() {
	cp.health.Stop()
	cp.eventBroadcaster.Shutdown()
	cp.draPlugin.Stop()
	if cp.stopNodeWatch != nil {
		cp.stopNodeWatch()
	}
	cp.nodeConditions.stop()
	cp.nodeConditions.writeStopped()
	cp.cancelNRI()
	cp.nriPlugin.Stop()
}

// Ready reports whether the kubelet has the plugin registered, the NRI plugin
// is connected, the watchdog finds nothing stuck, no reserved partition holds a
// prepared claim's CPUs and the node has its kubelet CPU reservation. It takes
// no lock, so a held stateMu cannot block a probe. Call it only once Start has
// returned.
func (cp *CPUDriver) Ready() bool {
	if cp.watchdog.stuck.Load() || cp.reservedHoldsClaims.Load() || cp.reservationMissing.Load() {
		return false
	}
	if status := cp.draPlugin.RegistrationStatus(); status == nil || !status.PluginRegistered {
		return false
	}
	select {
	case <-cp.initialSync:
	default:
		return false
	}
	return !cp.nriDisconnected.Load()
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
	defer cp.watchdog.track("Shutdown")()
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

type nriConnection struct {
	driver *CPUDriver
	plugin nriRunner
}

// Run marks NRI disconnected when the plugin's Run returns, which the NRI stub
// does once its connection to the runtime is gone.
func (c nriConnection) Run(ctx context.Context) error {
	err := c.plugin.Run(ctx)
	c.driver.stateMu.Lock()
	c.driver.nriDisconnected.Store(true)
	c.driver.stateMu.Unlock()
	if c.driver.reconnecting.CompareAndSwap(true, false) {
		// The stub's close callback for the stopped connection runs after Run
		// returns, and closes whatever connection the stub holds by then.
		select {
		case <-c.driver.nriClosed:
		case <-time.After(nriRetryInitialBackoff):
		case <-ctx.Done():
		}
		return errNRIReconnect
	}
	return err
}

// errNRIReconnect is the return of a Run the driver ended itself so that the
// runtime synchronizes it again.
var errNRIReconnect = errors.New("NRI connection closed by the driver to synchronize again")

const (
	nriRetryInitialBackoff     = 1 * time.Second
	nriRetryMaxBackoff         = 30 * time.Second
	nriRetryHealthyRunDuration = nriRetryMaxBackoff
)

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
		if errors.Is(err, errNRIReconnect) {
			logger.Info("reconnecting the NRI plugin")
			continue
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
			return fmt.Errorf("NRI plugin failed for %d times to be restarted", maxAttempts)
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
