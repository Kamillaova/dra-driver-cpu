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

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
	"k8s.io/utils/cpuset"
)

const (
	CPUResourceQualifiedName = "dra.cpu/cpu"

	CPUDevicePrefix              = "cpudev"
	CPUDeviceSocketGroupedPrefix = "cpudevsocket"
	CPUDeviceNUMAGroupedPrefix   = "cpudevnuma"
	CPUDeviceMachineGrouped      = "cpudevmachine"
)

func Build(topo *cpuinfo.CPUTopology, reservedCPUSet cpuset.CPUSet, pcieRootMapper *store.PCIeRootMapper, nodeAllocatableResources bool) ([]resourceapi.Device, map[string]int) {
	deviceInfos := cpuDeviceInfos(topo, reservedCPUSet)
	nameToID := make(map[string]int)
	for _, dev := range deviceInfos {
		nameToID[dev.name] = dev.cpu.CpuID
	}
	return createCPUDeviceSlices(deviceInfos, pcieRootMapper, topo.SMTEnabled, nodeAllocatableResources), nameToID
}

// BuildGrouped publishes one device per CPU group. wholeCoreStep is the whole-core
// allocation step from WholeCoreStep, or 0 when whole-core allocation is off; the
// caller decides it once so publication and allocation cannot disagree.
func BuildGrouped(logger logr.Logger, groupBy string, topo *cpuinfo.CPUTopology, onlineCPUs, reservedCPUSet cpuset.CPUSet, pcieRootMapper *store.PCIeRootMapper, nodeAllocatableResources bool, wholeCoreStep int) ([]resourceapi.Device, map[string]int) {
	deviceInfos := groupedCPUDeviceInfos(groupBy, topo, onlineCPUs, reservedCPUSet, wholeCoreStep > 1)
	nameToID := make(map[string]int)
	for _, dev := range deviceInfos {
		switch groupBy {
		case GROUP_BY_SOCKET:
			nameToID[dev.name] = dev.socketID
		case GROUP_BY_NUMA_NODE:
			nameToID[dev.name] = dev.numaNodeID
		}
	}
	return createGroupedCPUDeviceSlices(logger, groupBy, deviceInfos, pcieRootMapper, topo, nodeAllocatableResources, wholeCoreStep), nameToID
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
	name       string
	cpus       cpuset.CPUSet
	socketID   int
	numaNodeID int
}

type cpuDeviceInfo struct {
	name string
	cpu  cpuinfo.CPUInfo
}

func groupedCPUDeviceInfos(groupBy string, topo *cpuinfo.CPUTopology, onlineCPUs, reservedCPUs cpuset.CPUSet, wholeCoresOnly bool) []groupedCPUDeviceInfo {
	var devices []groupedCPUDeviceInfo
	// Under whole-core allocation a core split by the reservation cannot back a
	// claim, so its remaining threads are dropped here rather than published as
	// capacity the allocator would refuse to hand out.
	allocatable := func(cpus cpuset.CPUSet) cpuset.CPUSet {
		if !wholeCoresOnly {
			return cpus
		}
		return topo.CPUDetails.CompleteCores(cpus)
	}

	switch groupBy {
	case GROUP_BY_SOCKET:
		socketIDs := topo.CPUDetails.Sockets().List()
		for _, socketID := range socketIDs {
			allocatableCPUs := allocatable(topo.CPUDetails.CPUsInSockets(socketID).Difference(reservedCPUs))
			if allocatableCPUs.Size() == 0 {
				continue
			}
			devices = append(devices, groupedCPUDeviceInfo{
				name:     fmt.Sprintf("%s%03d", CPUDeviceSocketGroupedPrefix, socketID),
				cpus:     allocatableCPUs,
				socketID: socketID,
			})
		}
	case GROUP_BY_NUMA_NODE:
		numaNodeIDs := topo.CPUDetails.NUMANodes().List()
		for _, numaID := range numaNodeIDs {
			allocatableCPUs := allocatable(topo.CPUDetails.CPUsInNUMANodes(numaID).Difference(reservedCPUs))
			if allocatableCPUs.Size() == 0 {
				continue
			}

			// All CPUs in a NUMA node belong to the same socket.
			anyCPU := allocatableCPUs.UnsortedList()[0]
			devices = append(devices, groupedCPUDeviceInfo{
				name:       fmt.Sprintf("%s%03d", CPUDeviceNUMAGroupedPrefix, numaID),
				cpus:       allocatableCPUs,
				socketID:   topo.CPUDetails[anyCPU].SocketID,
				numaNodeID: numaID,
			})
		}
	case GROUP_BY_MACHINE:
		allocatableCPUs := allocatable(onlineCPUs.Difference(reservedCPUs))
		devices = append(devices, groupedCPUDeviceInfo{
			name: CPUDeviceMachineGrouped,
			cpus: allocatableCPUs,
		})
	}
	return devices
}

// cpuDeviceInfos returns the stable individual CPU device enumeration used by
// both ResourceSlice publication and PrepareResourceClaims device lookup.
// Keep the ordering in one place so device names resolve to the same CPUs even
// when Prepare runs before the first ResourceSlice publication after restart.
func cpuDeviceInfos(topo *cpuinfo.CPUTopology, reservedCPUSet cpuset.CPUSet) []cpuDeviceInfo {
	reservedCPUs := make(map[int]bool)
	for _, cpuID := range reservedCPUSet.List() {
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
func createGroupedCPUDeviceSlices(logger logr.Logger, groupBy string, deviceInfos []groupedCPUDeviceInfo, pcieRootMapper *store.PCIeRootMapper, topo *cpuinfo.CPUTopology, nodeAllocatableResources bool, wholeCoreStep int) []resourceapi.Device {
	logger.V(4).Info("creating grouped CPU devices")
	var devices []resourceapi.Device
	smtEnabled := topo.SMTEnabled

	for _, deviceInfo := range deviceInfos {
		availableCPUs := int64(deviceInfo.cpus.Size())
		deviceCapacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			CPUResourceQualifiedName: {
				Value:         *resource.NewQuantity(availableCPUs, resource.DecimalSI),
				RequestPolicy: wholeCoreRequestPolicy(availableCPUs, wholeCoreStep),
			},
		}

		switch groupBy {
		case GROUP_BY_SOCKET:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSocketID:   {IntValue: new(int64(deviceInfo.socketID))},
				AttributeNumCPUs:    {IntValue: new(availableCPUs)},
				AttributeSMTEnabled: {BoolValue: new(smtEnabled)},
			}
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		case GROUP_BY_NUMA_NODE:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				// DRA standard attributes first
				deviceattribute.StandardDeviceAttributeNUMANode: {IntValue: new(int64(deviceInfo.numaNodeID))},
				// Driver-specific/non-standard attributes next
				AttributeSocketID:   {IntValue: new(int64(deviceInfo.socketID))},
				AttributeSMTEnabled: {BoolValue: new(smtEnabled)},
				AttributeNumCPUs:    {IntValue: new(availableCPUs)},
			}
			addCompatibilityAttributes(deviceAttrs, int64(deviceInfo.numaNodeID))
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		case GROUP_BY_MACHINE:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSMTEnabled: {BoolValue: new(smtEnabled)},
				AttributeNumCPUs:    {IntValue: new(availableCPUs)},
			}
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)
			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
			})
		}
	}

	return devices
}

// createCPUDeviceSlices creates Device objects based on the CPU topology.
// It groups CPUs by physical core to assign consecutive device IDs to hyperthreads.
// This allows the DRA scheduler, which requests resources in contiguous blocks,
// to co-locate workloads on hyperthreads of the same core.
func createCPUDeviceSlices(deviceInfos []cpuDeviceInfo, pcieRootMapper *store.PCIeRootMapper, smtEnabled bool, nodeAllocatableResources bool) []resourceapi.Device {
	var allDevices []resourceapi.Device
	for _, deviceInfo := range deviceInfos {
		cpu := deviceInfo.cpu
		deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			// DRA standard attributes first
			deviceattribute.StandardDeviceAttributeNUMANode: {IntValue: new(int64(cpu.NUMANodeID))},
			// Driver-specific/non-standard attributes next
			AttributeSocketID:   {IntValue: new(int64(cpu.SocketID))},
			AttributeSMTEnabled: {BoolValue: new(smtEnabled)},
			AttributeCacheL3ID:  {IntValue: new(int64(cpu.UncoreCacheID))},
			AttributeCoreType:   {StringValue: new(cpu.CoreType.String())},
			AttributeCoreID:     {IntValue: new(int64(cpu.CoreID))},
			AttributeCPUID:      {IntValue: new(int64(cpu.CpuID))},
		}
		addCompatibilityAttributes(deviceAttrs, int64(cpu.NUMANodeID))
		addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, cpu.CpuID)

		cpuDevice := resourceapi.Device{
			Name:                     deviceInfo.name,
			Attributes:               deviceAttrs,
			Capacity:                 make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity),
			NodeAllocatableResources: individualCPUNodeAllocatable(nodeAllocatableResources),
		}
		allDevices = append(allDevices, cpuDevice)
	}
	return allDevices
}

func addPCIeRootsAttribute(pcieRootMapper *store.PCIeRootMapper, attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, cpuIDs ...int) {
	// Note: union semantics are correct because kernel cpulistaffinity currently collapses to NUMA granularity;
	// grouped allocation at socket/NUMA level therefore covers all CPUs local to every reported root.
	// See docs/dev/topology-linux-sysfs.md for in-depth exploration about the topic.
	pcieRoots := pcieRootMapper.GetPCIeRootsForCPU(cpuIDs...)
	if len(pcieRoots) == 0 {
		return
	}
	attrs[deviceattribute.StandardDeviceAttributePCIeRoot] = resourceapi.DeviceAttribute{StringValues: pcieRoots}
}

// WholeCoreStep returns the allocation step in CPUs when whole-core allocation is
// both requested and possible on this node, and 0 otherwise. Callers treat a step
// above 1 as "whole-core allocation is in effect".
//
// The step is the node's thread-per-core count, so it only exists when every
// allocatable core has the same number of threads. On a hybrid part, where
// performance cores are SMT and efficiency cores are not, there is no single step
// and the feature is disabled for the node.
//
// Disabling is deliberate rather than fatal: the driver config is usually
// fleet-wide, so refusing to start would take out every node of a mixed pool
// instead of the one option those nodes cannot honour.
func WholeCoreStep(logger logr.Logger, topo *cpuinfo.CPUTopology, onlineCPUs, reservedCPUs cpuset.CPUSet, fullPhysicalCPUsOnly bool) int {
	if !fullPhysicalCPUsOnly {
		return 0
	}
	allocatable := onlineCPUs.Difference(reservedCPUs)
	threadsPerCore := topo.CPUDetails.UniformThreadsPerCore(allocatable)
	switch threadsPerCore {
	case 0:
		logger.Info("fullPhysicalCPUsOnly disabled on this node: allocatable cores do not all have the same thread count",
			"allocatableCPUs", allocatable.String())
		return 0
	case 1:
		// Nothing to keep together, so leave capacity and allocation untouched
		// rather than publishing a request policy with a step of one CPU.
		logger.V(2).Info("fullPhysicalCPUsOnly is a no-op on this node: every core has one thread")
		return 0
	}
	return threadsPerCore
}

// wholeCoreRequestPolicy constrains a device's CPU capacity to whole-core
// multiples, so the scheduler rounds a request up instead of allocating an amount
// the driver would have to refuse.
//
// Default must be the full capacity, not the step: ValidRange requires a Default,
// and a claim that omits a capacity request consumes it. Upstream semantics for an
// omitted request are the whole device, so a Default of one core would silently
// shrink such claims.
//
// This diverges from the kubelet CPU Manager, which rejects a request that is not
// a whole-core multiple. DRA has no reject-unless-multiple primitive, and rounding
// resolves at scheduling time and is recorded in ConsumedCapacity, rather than
// failing admission once the pod is already bound.
func wholeCoreRequestPolicy(capacityCPUs int64, threadsPerCore int) *resourceapi.CapacityRequestPolicy {
	if threadsPerCore <= 1 || capacityCPUs <= 0 {
		return nil
	}
	step := resource.NewQuantity(int64(threadsPerCore), resource.DecimalSI)
	return &resourceapi.CapacityRequestPolicy{
		Default: resource.NewQuantity(capacityCPUs, resource.DecimalSI),
		ValidRange: &resourceapi.CapacityRequestPolicyRange{
			Min:  step,
			Step: step,
		},
	}
}
