/*
Copyright The Kubernetes Authors.

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

package device

import (
	"fmt"
	"sort"
	"strings"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
	"k8s.io/utils/cpuset"
)

const (
	CPUResourceName          = "cpu"
	CPUResourceQualifiedName = "dra.cpu/cpu"

	CPUDevicePrefix              = "cpudev"
	CPUDeviceSocketGroupedPrefix = "cpudevsocket"
	CPUDeviceNUMAGroupedPrefix   = "cpudevnuma"
	CPUDeviceMachineGrouped      = "cpudevmachine"
	CPUDeviceCacheGroupedPrefix  = "cpudevcache"
)

// Inventory holds the machine data and resource availability
// as configured. Represents a subset of the physical resources.
type Inventory struct {
	CPUTopology  *cpuinfo.CPUTopology
	ReservedCPUs cpuset.CPUSet
}

// ManagedCPUs returns CPUs reports the validated online CPUs.
func (inv Inventory) ManagedCPUs() cpuset.CPUSet {
	return inv.CPUTopology.CPUDetails.CPUs()
}

// AllocatableCPUs returns all the machine-level CPUs available for allocation
// NOTE this DOES NOT take into account currently allocated CPUs: it returns
// the maximum available theoretical pool depending only on static configuration
func (inv Inventory) AllocatableCPUs() cpuset.CPUSet {
	return inv.ManagedCPUs().Difference(inv.ReservedCPUs)
}

// Mapping includes the reverse lookup maps to bind the exposed DRA device names
// to the pool we need to allocate resources from.
// At most one lookup map is populated. LayoutMachine does not populate one, by design.
type Mapping struct {
	// NameToCPUID is populated for LayoutIndividual.
	NameToCPUID map[string]int
	// NameToSocketID is populated for LayoutSocket.
	NameToSocketID map[string]int
	// NameToNUMANodeID is populated for LayoutNUMANode.
	NameToNUMANodeID map[string]int
	// NameToCPUs is populated for LayoutUncoreCache, whose device is no
	// socket or NUMA node: it holds the CPUs each device publishes.
	NameToCPUs map[string]cpuset.CPUSet
}

// Layout controls how CPUs are exposed as DRA devices.
// It abstracts the driver configuration grouping mode and criteria on a single
// device layer selector.
type Layout int

const (
	// LayoutIndividual exposes each CPU as a separate device.
	LayoutIndividual Layout = iota
	// LayoutSocket exposes one device per CPU socket.
	LayoutSocket
	// LayoutNUMANode exposes one device per NUMA node.
	LayoutNUMANode
	// LayoutMachine exposes one device for all allocatable CPUs in the machine.
	LayoutMachine
	// LayoutUncoreCache exposes one device per uncore cache.
	LayoutUncoreCache
)

// FindLayout converts the driver configuration into a device layout.
func FindLayout(cpuDeviceMode, cpuDeviceGroupBy string) Layout {
	if cpuDeviceMode != CPU_DEVICE_MODE_GROUPED {
		return LayoutIndividual
	}
	switch cpuDeviceGroupBy {
	case GROUP_BY_SOCKET:
		return LayoutSocket
	case GROUP_BY_NUMA_NODE:
		return LayoutNUMANode
	case GROUP_BY_UNCORE_CACHE:
		return LayoutUncoreCache
	default:
		return LayoutMachine
	}
}

// BuildInput is the input for Build. It carries both input parameters and input data.
type BuildInput struct {
	// Inventory describes the CPU topology and CPU availability to expose.
	Inventory Inventory
	// Layout controls how CPUs are exposed as devices.
	Layout Layout
	// PCIeRootMapper provides the PCIe roots associated with CPUs.
	PCIeRootMapper *store.PCIeRootMapper
	// PublishNodeAllocatableResourceMapping enables node allocatable resource
	// mappings on the generated devices.
	PublishNodeAllocatableResourceMapping bool
	// Expose attributes intended for consumption of an external allocator:
	// - cpuset pertaining to a grouped device as attribute
	// - smt sibling maapping
	ExposeExtAttrs bool
	// FullPhysicalCPUsOnly leaves every core the reservation splits out of the
	// grouped devices, gives a device whose cores all have two threads a request
	// policy in whole cores, and refuses a device holding a core of more than two
	// threads. Every device publishes it.
	FullPhysicalCPUsOnly bool
	// Allocator is the allocator Prepare takes CPUs with, which every device
	// publishes.
	Allocator string
}

type BuildResult struct {
	// Devices are the generated DRA devices.
	Devices []resourceapi.Device
	Mapping Mapping
	// ThreadsPerCore is each grouped device's threads per core by device name,
	// 0 for a device whose cores differ in threads.
	ThreadsPerCore map[string]int
	// MixedCores counts, under FullPhysicalCPUsOnly, the cores of each size of
	// every grouped device whose cores differ in threads, by device name.
	MixedCores map[string]CoreCounts
}

// CoreCounts is the number of cores of two threads and of one in a device.
type CoreCounts struct {
	TwoThreads int
	OneThread  int
}

func Build(input BuildInput) (BuildResult, error) {
	if input.Layout == LayoutIndividual {
		nameToID := make(map[string]int)
		deviceInfos := cpuDeviceInfos(input.Inventory)
		for _, dev := range deviceInfos {
			nameToID[dev.name] = dev.cpu.CpuID
		}
		devices, err := createCPUDeviceSlices(deviceInfos, input.PCIeRootMapper, input.Inventory.CPUTopology.SMTEnabled, input.PublishNodeAllocatableResourceMapping)
		if err != nil {
			return BuildResult{}, err
		}
		addSettingAttributes(devices, input.FullPhysicalCPUsOnly, input.Allocator)
		if err := validateDeviceAttributeValueCount(devices); err != nil {
			return BuildResult{}, err
		}
		return BuildResult{
			Devices: devices,
			Mapping: Mapping{
				NameToCPUID: nameToID,
			},
		}, nil
	}

	var err error
	var res BuildResult
	switch input.Layout {
	case LayoutSocket:
		res.Mapping.NameToSocketID = make(map[string]int)
	case LayoutNUMANode:
		res.Mapping.NameToNUMANodeID = make(map[string]int)
	case LayoutUncoreCache:
		res.Mapping.NameToCPUs = make(map[string]cpuset.CPUSet)
	}
	res.ThreadsPerCore = make(map[string]int)
	res.MixedCores = make(map[string]CoreCounts)
	deviceInfos, err := groupedCPUDeviceInfos(input.Layout, input.Inventory, input.FullPhysicalCPUsOnly)
	if err != nil {
		return BuildResult{}, err
	}
	for _, dev := range deviceInfos {
		switch input.Layout {
		case LayoutSocket:
			res.Mapping.NameToSocketID[dev.name] = dev.socketID
		case LayoutNUMANode:
			res.Mapping.NameToNUMANodeID[dev.name] = dev.numaNodeID
		case LayoutUncoreCache:
			res.Mapping.NameToCPUs[dev.name] = dev.cpus
		}
		res.ThreadsPerCore[dev.name] = dev.threadsPerCore
		if !input.FullPhysicalCPUsOnly {
			continue
		}
		counts, err := countCores(input.Inventory.CPUTopology.CPUDetails, dev)
		if err != nil {
			return BuildResult{}, err
		}
		if dev.threadsPerCore == 0 && dev.cpus.Size() > 0 {
			res.MixedCores[dev.name] = counts
		}
	}
	res.Devices, err = createGroupedCPUDeviceSlices(input.Layout, deviceInfos, input.PCIeRootMapper, input.Inventory.CPUTopology, input.PublishNodeAllocatableResourceMapping, input.ExposeExtAttrs, input.FullPhysicalCPUsOnly)
	if err != nil {
		return BuildResult{}, err
	}
	addSettingAttributes(res.Devices, input.FullPhysicalCPUsOnly, input.Allocator)
	if err := validateDeviceAttributeValueCount(res.Devices); err != nil {
		return BuildResult{}, err
	}
	return res, nil
}

func groupedCPUNodeAllocatable(enabled bool) map[v1.ResourceName]resourceapi.NodeAllocatableResource {
	if !enabled {
		return nil
	}
	return map[v1.ResourceName]resourceapi.NodeAllocatableResource{
		v1.ResourceCPU: {
			Mapping: &resourceapi.NodeAllocatableMapping{
				CapacityKey:        new(resourceapi.QualifiedName(CPUResourceQualifiedName)),
				CapacityMultiplier: new(resource.MustParse("1")),
			},
		},
	}
}

func individualCPUNodeAllocatable(enabled bool) map[v1.ResourceName]resourceapi.NodeAllocatableResource {
	if !enabled {
		return nil
	}
	return map[v1.ResourceName]resourceapi.NodeAllocatableResource{
		v1.ResourceCPU: {
			Mapping: &resourceapi.NodeAllocatableMapping{
				DeviceMultiplier: new(resource.MustParse("1")),
			},
		},
	}
}

type groupedCPUDeviceInfo struct {
	name           string
	cpus           cpuset.CPUSet
	socketID       int
	numaNodeID     int
	cacheID        int
	threadsPerCore int
}

type cpuDeviceInfo struct {
	name string
	cpu  cpuinfo.CPUInfo
}

func groupedCPUDeviceInfos(layout Layout, machine Inventory, fullPhysicalCPUsOnly bool) ([]groupedCPUDeviceInfo, error) {
	var devices []groupedCPUDeviceInfo

	topo := machine.CPUTopology // shortcut
	usable := func(cpus cpuset.CPUSet) cpuset.CPUSet {
		if !fullPhysicalCPUsOnly {
			return cpus
		}
		return topo.CPUDetails.CompleteCores(cpus)
	}

	switch layout {
	case LayoutSocket:
		socketIDs := topo.CPUDetails.Sockets().List()
		for _, socketID := range socketIDs {
			allocatableCPUs := usable(topo.CPUDetails.CPUsInSockets(socketID).Difference(machine.ReservedCPUs))
			if allocatableCPUs.Size() == 0 {
				continue
			}
			devices = append(devices, groupedCPUDeviceInfo{
				name:           fmt.Sprintf("%s%03d", CPUDeviceSocketGroupedPrefix, socketID),
				cpus:           allocatableCPUs,
				socketID:       socketID,
				threadsPerCore: topo.CPUDetails.UniformThreadsPerCore(allocatableCPUs),
			})
		}
	case LayoutNUMANode:
		numaNodeIDs := topo.CPUDetails.NUMANodes().List()
		for _, numaID := range numaNodeIDs {
			allocatableCPUs := usable(topo.CPUDetails.CPUsInNUMANodes(numaID).Difference(machine.ReservedCPUs))
			if allocatableCPUs.Size() == 0 {
				continue
			}

			// All CPUs in a NUMA node belong to the same socket.
			anyCPU := allocatableCPUs.UnsortedList()[0]
			devices = append(devices, groupedCPUDeviceInfo{
				name:           fmt.Sprintf("%s%03d", CPUDeviceNUMAGroupedPrefix, numaID),
				cpus:           allocatableCPUs,
				socketID:       topo.CPUDetails[anyCPU].SocketID,
				numaNodeID:     numaID,
				threadsPerCore: topo.CPUDetails.UniformThreadsPerCore(allocatableCPUs),
			})
		}
	case LayoutMachine:
		// Use the topology-validated CPU set. GetCPUInfos filters online CPUs
		// whose topology is incomplete, and the allocation store is built from
		// the same CPUDetails map.
		allocatableCPUs := usable(machine.AllocatableCPUs())
		devices = append(devices, groupedCPUDeviceInfo{
			name:           CPUDeviceMachineGrouped,
			cpus:           allocatableCPUs,
			threadsPerCore: topo.CPUDetails.UniformThreadsPerCore(allocatableCPUs),
		})
	case LayoutUncoreCache:
		caches, unreported, spanning := uncoreCaches(topo.CPUDetails)
		var reasons []string
		if cpus := unreported.Difference(machine.ReservedCPUs); !cpus.IsEmpty() {
			reasons = append(reasons, "the kernel reports no uncore cache for CPUs "+cpus.String())
		}
		if cpus := spanning.Difference(machine.ReservedCPUs); !cpus.IsEmpty() {
			reasons = append(reasons, "CPUs "+cpus.String()+" share an uncore cache with CPUs of another NUMA node")
		}
		if len(reasons) > 0 {
			return nil, fmt.Errorf("groupBy %s needs each allocatable CPU in an uncore cache of one NUMA node, but %s; set groupBy to %s or %s on this node",
				GROUP_BY_UNCORE_CACHE, strings.Join(reasons, ", and "), GROUP_BY_NUMA_NODE, GROUP_BY_SOCKET)
		}
		for _, cache := range caches {
			allocatableCPUs := usable(cache.cpus.Difference(machine.ReservedCPUs))
			if allocatableCPUs.Size() == 0 {
				continue
			}
			// A cache listed lies in one NUMA node, so in one socket.
			anyCPU := allocatableCPUs.UnsortedList()[0]
			devices = append(devices, groupedCPUDeviceInfo{
				name:           fmt.Sprintf("%s%03d", CPUDeviceCacheGroupedPrefix, cache.id),
				cpus:           allocatableCPUs,
				socketID:       topo.CPUDetails[anyCPU].SocketID,
				numaNodeID:     cache.numaNodeID,
				cacheID:        cache.id,
				threadsPerCore: topo.CPUDetails.UniformThreadsPerCore(allocatableCPUs),
			})
		}
	}
	return devices, nil
}

// uncoreCache is one of the node's uncore caches: the id the kernel gives it,
// the NUMA node it lies in, and the CPUs that share it.
type uncoreCache struct {
	id         int
	numaNodeID int
	cpus       cpuset.CPUSet
}

// uncoreCaches lists the uncore caches of details that lie in one NUMA node,
// ordered by NUMA node and then by cache id. It returns apart the CPUs the
// kernel reports no cache for and the CPUs of caches that span NUMA nodes, for
// which a device could publish no NUMA node.
func uncoreCaches(details cpuinfo.CPUDetails) ([]uncoreCache, cpuset.CPUSet, cpuset.CPUSet) {
	cpusByCache := make(map[int][]int)
	var unknown []int
	for cpuID, info := range details {
		if info.UncoreCacheID == -1 {
			unknown = append(unknown, cpuID)
			continue
		}
		cpusByCache[info.UncoreCacheID] = append(cpusByCache[info.UncoreCacheID], cpuID)
	}
	spanning := cpuset.New()

	caches := make([]uncoreCache, 0, len(cpusByCache))
	for cacheID, cpuIDs := range cpusByCache {
		cpus := cpuset.New(cpuIDs...)
		numaNodes := details.KeepOnly(cpus).NUMANodes()
		if numaNodes.Size() != 1 {
			spanning = spanning.Union(cpus)
			continue
		}
		caches = append(caches, uncoreCache{id: cacheID, numaNodeID: numaNodes.List()[0], cpus: cpus})
	}
	sort.Slice(caches, func(i, j int) bool {
		if caches[i].numaNodeID != caches[j].numaNodeID {
			return caches[i].numaNodeID < caches[j].numaNodeID
		}
		return caches[i].id < caches[j].id
	})
	return caches, cpuset.New(unknown...), spanning
}

func countCores(details cpuinfo.CPUDetails, dev groupedCPUDeviceInfo) (CoreCounts, error) {
	var counts CoreCounts
	seen := make(map[cpuinfo.CoreLocation]bool)
	for _, cpuID := range dev.cpus.List() {
		loc, ok := details.CoreOf(cpuID)
		if !ok || seen[loc] {
			continue
		}
		seen[loc] = true
		switch siblings := details.SiblingsOf(cpuID); siblings.Size() {
		case 2:
			counts.TwoThreads++
		case 1:
			counts.OneThread++
		default:
			return CoreCounts{}, fmt.Errorf("device %q holds a core of %d threads (CPUs %s), where fullPhysicalCPUsOnly supports whole cores of 1 or 2 threads",
				dev.name, siblings.Size(), siblings.String())
		}
	}
	return counts, nil
}

// cpuDeviceInfos returns the stable individual CPU device enumeration used by
// both ResourceSlice publication and PrepareResourceClaims device lookup.
// Keep the ordering in one place so device names resolve to the same CPUs even
// when Prepare runs before the first ResourceSlice publication after restart.
func cpuDeviceInfos(machine Inventory) []cpuDeviceInfo {
	topo := machine.CPUTopology // shortcut

	reservedCPUs := make(map[int]bool)
	for _, cpuID := range machine.ReservedCPUs.List() {
		reservedCPUs[cpuID] = true
	}

	allCPUs := make([]cpuinfo.CPUInfo, 0, len(topo.CPUDetails))
	availableCPUs := []cpuinfo.CPUInfo{}
	for _, cpu := range topo.CPUDetails {
		allCPUs = append(allCPUs, cpu)
		if !reservedCPUs[cpu.CpuID] {
			availableCPUs = append(availableCPUs, cpu)
		}
	}
	sort.Slice(availableCPUs, func(i, j int) bool {
		return availableCPUs[i].CpuID < availableCPUs[j].CpuID
	})

	processedCpus := make(map[int]bool)
	coreGroups := [][]cpuinfo.CPUInfo{}
	cpuInfoMap := make(map[int]cpuinfo.CPUInfo)
	for _, info := range allCPUs {
		cpuInfoMap[info.CpuID] = info
	}

	for _, cpu := range availableCPUs {
		if processedCpus[cpu.CpuID] {
			continue
		}
		if cpu.SiblingCPUID == -1 || reservedCPUs[cpu.SiblingCPUID] {
			coreGroups = append(coreGroups, []cpuinfo.CPUInfo{cpu})
			processedCpus[cpu.CpuID] = true
		} else {
			coreGroups = append(coreGroups, []cpuinfo.CPUInfo{cpu, cpuInfoMap[cpu.SiblingCPUID]})
			processedCpus[cpu.CpuID] = true
			processedCpus[cpu.SiblingCPUID] = true
		}
	}

	sort.Slice(coreGroups, func(i, j int) bool {
		return coreGroups[i][0].CpuID < coreGroups[j][0].CpuID
	})

	devices := []cpuDeviceInfo{}
	devID := 0
	for _, group := range coreGroups {
		for _, cpu := range group {
			devices = append(devices, cpuDeviceInfo{
				name: fmt.Sprintf("%s%03d", CPUDevicePrefix, devID),
				cpu:  cpu,
			})
			devID++
		}
	}
	return devices
}

// createGroupedCPUDeviceSlices creates Device objects based on the CPU topology, grouped by a specific criteria.
func createGroupedCPUDeviceSlices(layout Layout, deviceInfos []groupedCPUDeviceInfo, pcieRootMapper *store.PCIeRootMapper, topo *cpuinfo.CPUTopology, nodeAllocatableResources, exposeExtAttrs, fullPhysicalCPUsOnly bool) ([]resourceapi.Device, error) {
	var devices []resourceapi.Device

	for _, deviceInfo := range deviceInfos {
		availableCPUs := int64(deviceInfo.cpus.Size())
		cpuCapacity := resourceapi.DeviceCapacity{Value: *resource.NewQuantity(availableCPUs, resource.DecimalSI)}
		if fullPhysicalCPUsOnly {
			cpuCapacity.RequestPolicy = wholeCoreRequestPolicy(availableCPUs, deviceInfo.threadsPerCore)
		}
		deviceCapacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			CPUResourceQualifiedName: cpuCapacity,
		}

		switch layout {
		case LayoutSocket:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSocketID:       {IntValue: new(int64(deviceInfo.socketID))},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeSMTEnabled:     {BoolValue: new(topo.SMTEnabled)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			if err := addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...); err != nil {
				return nil, err
			}
			if exposeExtAttrs {
				if err := addCPUIDsAttribute(deviceAttrs, deviceInfo.cpus); err != nil {
					return nil, err
				}
				if err := addSMTLayoutAttribute(deviceAttrs, topo); err != nil {
					return nil, err
				}
			}

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		case LayoutNUMANode:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				// Driver-specific/non-standard attributes
				AttributeSocketID:       {IntValue: new(int64(deviceInfo.socketID))},
				AttributeSMTEnabled:     {BoolValue: new(topo.SMTEnabled)},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			addCompatibilityAttributes(deviceAttrs, int64(deviceInfo.numaNodeID))
			if err := addNUMANodeAttribute(deviceAttrs, deviceInfo.numaNodeID); err != nil {
				return nil, err
			}
			if err := addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...); err != nil {
				return nil, err
			}
			if exposeExtAttrs {
				if err := addCPUIDsAttribute(deviceAttrs, deviceInfo.cpus); err != nil {
					return nil, err
				}
				if err := addSMTLayoutAttribute(deviceAttrs, topo); err != nil {
					return nil, err
				}
			}

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		case LayoutMachine:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSMTEnabled:     {BoolValue: new(topo.SMTEnabled)},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			if err := addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...); err != nil {
				return nil, err
			}
			if exposeExtAttrs {
				if err := addCPUIDsAttribute(deviceAttrs, deviceInfo.cpus); err != nil {
					return nil, err
				}
				if err := addSMTLayoutAttribute(deviceAttrs, topo); err != nil {
					return nil, err
				}
			}
			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		case LayoutUncoreCache:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSocketID:       {IntValue: new(int64(deviceInfo.socketID))},
				AttributeCacheL3ID:      {IntValue: new(int64(deviceInfo.cacheID))},
				AttributeSMTEnabled:     {BoolValue: new(topo.SMTEnabled)},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			addCompatibilityAttributes(deviceAttrs, int64(deviceInfo.numaNodeID))
			if err := addNUMANodeAttribute(deviceAttrs, deviceInfo.numaNodeID); err != nil {
				return nil, err
			}
			if err := addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...); err != nil {
				return nil, err
			}
			if exposeExtAttrs {
				if err := addCPUIDsAttribute(deviceAttrs, deviceInfo.cpus); err != nil {
					return nil, err
				}
				if err := addSMTLayoutAttribute(deviceAttrs, topo); err != nil {
					return nil, err
				}
			}

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		}
	}

	return devices, nil
}

// wholeCoreRequestPolicy has the scheduler round a request up to whole cores,
// since a request policy cannot refuse an amount that is not a multiple.
func wholeCoreRequestPolicy(capacityCPUs int64, threadsPerCore int) *resourceapi.CapacityRequestPolicy {
	if threadsPerCore <= 1 || capacityCPUs <= 0 {
		return nil
	}
	threads := int64(threadsPerCore)
	policy := &resourceapi.CapacityRequestPolicy{
		// A request naming no amount consumes the default: the whole device, as without a policy.
		Default:    resource.NewQuantity(capacityCPUs, resource.DecimalSI),
		ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: resource.NewQuantity(threads, resource.DecimalSI)},
	}
	// The API server refuses a step that takes min past the capacity.
	if capacityCPUs >= 2*threads {
		policy.ValidRange.Step = resource.NewQuantity(threads, resource.DecimalSI)
	}
	return policy
}

// createCPUDeviceSlices creates Device objects based on the CPU topology.
// It groups CPUs by physical core to assign consecutive device IDs to hyperthreads.
// This allows the DRA scheduler, which requests resources in contiguous blocks,
// to co-locate workloads on hyperthreads of the same core.
func createCPUDeviceSlices(deviceInfos []cpuDeviceInfo, pcieRootMapper *store.PCIeRootMapper, smtEnabled, nodeAllocatableResources bool) ([]resourceapi.Device, error) {
	var allDevices []resourceapi.Device
	for _, deviceInfo := range deviceInfos {
		cpu := deviceInfo.cpu
		deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			// Driver-specific/non-standard attributes
			AttributeSocketID:   {IntValue: new(int64(cpu.SocketID))},
			AttributeSMTEnabled: {BoolValue: new(smtEnabled)},
			AttributeCacheL3ID:  {IntValue: new(int64(cpu.UncoreCacheID))},
			AttributeCoreType:   {StringValue: new(cpu.CoreType.String())},
			AttributeCoreID:     {IntValue: new(int64(cpu.CoreID))},
			AttributeCPUID:      {IntValue: new(int64(cpu.CpuID))},
		}

		addCompatibilityAttributes(deviceAttrs, int64(cpu.NUMANodeID))
		if err := addNUMANodeAttribute(deviceAttrs, cpu.NUMANodeID); err != nil {
			return nil, err
		}
		if err := addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, cpu.CpuID); err != nil {
			return nil, err
		}

		cpuDevice := resourceapi.Device{
			Name:                     deviceInfo.name,
			Attributes:               deviceAttrs,
			Capacity:                 make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity),
			NodeAllocatableResources: individualCPUNodeAllocatable(nodeAllocatableResources),
		}
		allDevices = append(allDevices, cpuDevice)
	}
	return allDevices, nil
}

func addPCIeRootsAttribute(pcieRootMapper *store.PCIeRootMapper, attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, cpuIDs ...int) error {
	if len(cpuIDs) == 0 {
		return nil // nothing to do
	}
	// Note: union semantics are correct because kernel cpulistaffinity currently collapses to NUMA granularity;
	// grouped allocation at socket/NUMA level therefore covers all CPUs local to every reported root.
	// See docs/dev/topology-linux-sysfs.md for in-depth exploration about the topic.
	pcieRoots := pcieRootMapper.GetPCIeRootsForCPU(cpuIDs...)
	if len(pcieRoots) == 0 {
		return nil // nothing to do
	}
	attrs[deviceattribute.StandardDeviceAttributePCIeRoot] = resourceapi.DeviceAttribute{StringValues: pcieRoots}
	return nil
}

func validateDeviceAttributeValueCount(devices []resourceapi.Device) error {
	for _, device := range devices {
		if numDeviceAttributeValues(device.Attributes) > resourceapi.ResourceSliceMaxAttributeValuesPerDevice {
			return fmt.Errorf("device %q exceeds the DRA max attribute value limit=%d", device.Name, resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
		}
	}
	return nil
}

func numDeviceAttributeValues(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute) int {
	count := 0
	for _, attr := range attrs {
		listAttrLen := len(attr.BoolValues) + len(attr.IntValues) + len(attr.StringValues) + len(attr.VersionValues)
		if listAttrLen > 0 {
			count += listAttrLen
		} else {
			count++ // A non-list attribute has one scalar value.
		}
	}
	return count
}

func addCPUIDsAttribute(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, cpus cpuset.CPUSet) error {
	if cpus.Size() == 0 {
		return nil // nothing to do
	}
	cpuIDs := cpus.String()
	if len(cpuIDs) > resourceapi.DeviceAttributeMaxValueLength {
		return fmt.Errorf("cpus %q cannot be represented within the limit of DRA max value length=%d", cpus.String(), resourceapi.DeviceAttributeMaxValueLength)
	}
	attrs[AttributeCPUIDs] = resourceapi.DeviceAttribute{StringValue: &cpuIDs}
	return nil
}

func addSMTLayoutAttribute(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, topo *cpuinfo.CPUTopology) error {
	smtLayout := FormatSMTLayout(topo)
	if len(smtLayout) == 0 {
		return fmt.Errorf("SMT layout unexpectedly empty")
	}
	if len(smtLayout) > resourceapi.DeviceAttributeMaxValueLength {
		return fmt.Errorf("SMT layout %q cannot be represented within the limit of DRA max value length=%d", smtLayout, resourceapi.DeviceAttributeMaxValueLength)
	}
	attrs[AttributeSMTLayout] = resourceapi.DeviceAttribute{StringValue: new(smtLayout)}
	return nil
}

// LookupConsumedCapacity retrieves CPU capacity for driverName, falling back to CPUResourceQualifiedName for compatibility.
func LookupConsumedCapacity(consumedCapacity map[resourceapi.QualifiedName]resource.Quantity, driverName string) (resource.Quantity, bool) {
	if driverName != "" {
		if q, ok := LookupByQualifiedName(consumedCapacity, resourceapi.QualifiedName(driverName+"/"+CPUResourceName), driverName); ok {
			return q, true
		}
	}
	return LookupByQualifiedName(consumedCapacity, CPUResourceQualifiedName, driverName)
}

// ConsumedCPUCount returns the number of CPUs an allocation result consumes.
// The prepare loop and the external allocator's request total both rely on
// it, so a capacity one of them rejects cannot slip through the other.
func ConsumedCPUCount(result resourceapi.DeviceRequestAllocationResult, driverName string) (int, error) {
	quantity, ok := LookupConsumedCapacity(result.ConsumedCapacity, driverName)
	if !ok {
		return 0, fmt.Errorf("CPU capacity %q for device %q is missing", CPUResourceQualifiedName, result.Device)
	}
	if quantity.Sign() <= 0 {
		return 0, fmt.Errorf("CPU capacity for device %q must be positive, got %s", result.Device, quantity.String())
	}
	count := quantity.Value()
	if quantity.CmpInt64(count) != 0 {
		return 0, fmt.Errorf("CPU capacity for device %q must be a whole number, got %s", result.Device, quantity.String())
	}
	return int(count), nil
}
