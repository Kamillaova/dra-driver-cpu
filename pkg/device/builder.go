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
	CPUDevicePoolPrefix          = "cpudevpool"
)

// poolShareMilliCPUs is the smallest share of a pool a request may take, and
// what a request that names no amount takes. A request policy's default is what
// an omitted capacity request consumes, and for an exclusive device that is
// deliberately the whole device; on a pool the same rule would let one template
// omission swallow a resource every other claim is meant to share.
const poolShareMilliCPUs = 100

func Build(topo *cpuinfo.CPUTopology, reservedCPUSet cpuset.CPUSet, pcieRootMapper *store.PCIeRootMapper, nodeAllocatableResources bool) ([]resourceapi.Device, map[string]int) {
	deviceInfos := cpuDeviceInfos(topo, reservedCPUSet)
	nameToID := make(map[string]int)
	for _, dev := range deviceInfos {
		nameToID[dev.name] = dev.cpu.CpuID
	}
	return createCPUDeviceSlices(deviceInfos, pcieRootMapper, topo.SMTEnabled, nodeAllocatableResources), nameToID
}

// GroupedDevices is what grouped mode publishes, together with what a caller
// needs to resolve an allocation on one of those devices back to CPUs.
type GroupedDevices struct {
	// ByPartition holds each publishing partition's devices, in the order the
	// partitions were given.
	ByPartition [][]resourceapi.Device
	// NameToID maps a device name to the socket or NUMA node it groups, and is
	// empty under the other groupings.
	NameToID map[string]int
	// ThreadsPerCore is each device's effective whole-core allocation step, and
	// holds no entry for a device where whole-core allocation is off or has no
	// single thread count to work with.
	ThreadsPerCore map[string]int
	// CPUs is each device's own allocatable CPUs: the group's, inside the
	// partition, minus the reservation, and minus any core the reservation split
	// where whole cores are promised. A claim on that device takes CPUs from
	// here and nowhere else.
	CPUs map[string]cpuset.CPUSet
	// Roles is each device's partition role, so a caller resolving an allocation
	// knows whether the device grants CPUs the claim holds alone or a share of a
	// pool every other claim on it holds too.
	Roles map[string]string
}

// BuildGrouped publishes one device per CPU group of every partition that
// claims may take CPUs from. Each device's own thread-per-core count decides
// whether whole-core allocation applies to it, so one device's non-uniform
// cores never disable the feature for a different, uniform device on the same
// node; fullPhysicalCPUsOnly is only the operator's request for the feature,
// not a per-device guarantee.
//
// Devices are returned one group per partition, in the order partitions were
// given, so that a partition's taints never travel in another partition's
// ResourceSlice.
//
// CCX-FORK: upstream publishes one device per group over the whole node, with
// no partitions and no taints, and returns a flat device list.
func BuildGrouped(logger logr.Logger, groupBy string, topo *cpuinfo.CPUTopology, onlineCPUs, reservedCPUSet cpuset.CPUSet, pcieRootMapper *store.PCIeRootMapper, nodeAllocatableResources bool, fullPhysicalCPUsOnly bool, partitions []Partition) GroupedDevices {
	built := GroupedDevices{
		NameToID:       make(map[string]int),
		ThreadsPerCore: make(map[string]int),
		CPUs:           make(map[string]cpuset.CPUSet),
		Roles:          make(map[string]string),
	}
	for _, partition := range partitions {
		if partition.IsPool() {
			deviceInfos := poolDeviceInfos(topo, partition)
			for _, dev := range deviceInfos {
				built.CPUs[dev.name] = dev.cpus
				built.Roles[dev.name] = partition.Role
			}
			if devices := createPoolDeviceSlices(deviceInfos, partition); len(devices) > 0 {
				built.ByPartition = append(built.ByPartition, devices)
			}
			continue
		}
		if !partition.PublishesExclusiveDevices() {
			continue
		}
		deviceInfos := groupedCPUDeviceInfos(logger, groupBy, topo, onlineCPUs, reservedCPUSet, fullPhysicalCPUsOnly, partition, len(partitions) > 1)
		for _, dev := range deviceInfos {
			built.Roles[dev.name] = partition.Role
			switch groupBy {
			case GROUP_BY_SOCKET:
				built.NameToID[dev.name] = dev.socketID
			case GROUP_BY_NUMA_NODE:
				built.NameToID[dev.name] = dev.numaNodeID
			}
			if fullPhysicalCPUsOnly && dev.threadsPerCore > 1 {
				built.ThreadsPerCore[dev.name] = dev.threadsPerCore
			}
			built.CPUs[dev.name] = dev.cpus
		}
		devices := createGroupedCPUDeviceSlices(logger, groupBy, deviceInfos, pcieRootMapper, topo, nodeAllocatableResources, fullPhysicalCPUsOnly, partition)
		if len(devices) > 0 {
			built.ByPartition = append(built.ByPartition, devices)
		}
	}
	return built
}

// partitionDeviceName suffixes a group's device name with the partition it
// belongs to, so that a group split between partitions yields one device per
// partition with disjoint CPUs. The implicit partition keeps the plain name an
// unpartitioned node publishes.
func partitionDeviceName(name string, partition Partition) string {
	if !partition.Named() {
		return name
	}
	return name + "-" + partition.Name
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
	// threadsPerCore is this device's own uniform thread-per-core count (0 when
	// its cores do not all agree), independent of fullPhysicalCPUsOnly: it is a
	// hardware fact published on every device, whether or not whole-core
	// allocation is requested.
	threadsPerCore int
}

type cpuDeviceInfo struct {
	name string
	cpu  cpuinfo.CPUInfo
}

type poolDeviceInfo struct {
	name       string
	cpus       cpuset.CPUSet
	socketID   int
	numaNodeID int
}

// poolDeviceInfos splits a pool partition into one device per NUMA node it
// touches. A NUMA node is as fine as a pool is ever cut, whatever grouping the
// exclusive partitions use: what a pool offers a workload is locality, and
// below a NUMA node there is none left to promise.
func poolDeviceInfos(topo *cpuinfo.CPUTopology, partition Partition) []poolDeviceInfo {
	var devices []poolDeviceInfo
	for _, numaID := range topo.CPUDetails.NUMANodes().List() {
		cpus := partition.CPUs.Intersection(topo.CPUDetails.CPUsInNUMANodes(numaID))
		if cpus.IsEmpty() {
			continue
		}
		devices = append(devices, poolDeviceInfo{
			name:       partitionDeviceName(fmt.Sprintf("%s%03d", CPUDevicePoolPrefix, numaID), partition),
			cpus:       cpus,
			socketID:   topo.CPUDetails[cpus.UnsortedList()[0]].SocketID,
			numaNodeID: numaID,
		})
	}
	return devices
}

// createPoolDeviceSlices publishes a pool as devices every claim that asks for
// it may hold at once. Each grants its whole CPU set rather than a cut of it,
// so the capacity is an admission bound on how much work lands on the pool and
// not a promise of exclusivity, and it carries no node-allocatable mapping: the
// same CPUs already count once for the containers holding no claim.
func createPoolDeviceSlices(deviceInfos []poolDeviceInfo, partition Partition) []resourceapi.Device {
	var devices []resourceapi.Device
	for _, deviceInfo := range deviceInfos {
		deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			deviceattribute.StandardDeviceAttributeNUMANode: {IntValue: new(int64(deviceInfo.numaNodeID))},
			AttributeSocketID: {IntValue: new(int64(deviceInfo.socketID))},
			AttributeNumCPUs:  {IntValue: new(int64(deviceInfo.cpus.Size()))},
		}
		addCompatibilityAttributes(deviceAttrs, int64(deviceInfo.numaNodeID))
		addPartitionAttributes(deviceAttrs, partition)

		devices = append(devices, resourceapi.Device{
			Name:       deviceInfo.name,
			Attributes: deviceAttrs,
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				CPUResourceQualifiedName: {
					Value:         *resource.NewQuantity(int64(deviceInfo.cpus.Size()), resource.DecimalSI),
					RequestPolicy: poolShareRequestPolicy(),
				},
			},
			AllowMultipleAllocations: new(true),
			Taints:                   partitionTaints(partition),
		})
	}
	return devices
}

// poolShareRequestPolicy makes a share of a pool the unit a request is measured
// in, down to a tenth of a CPU.
func poolShareRequestPolicy() *resourceapi.CapacityRequestPolicy {
	share := resource.NewMilliQuantity(poolShareMilliCPUs, resource.DecimalSI)
	return &resourceapi.CapacityRequestPolicy{
		Default: share,
		ValidRange: &resourceapi.CapacityRequestPolicyRange{
			Min:  share,
			Step: share,
		},
	}
}

// CCX-FORK: upstream describes every group of the node in one call; here each
// call describes one partition's share of those groups.
func groupedCPUDeviceInfos(logger logr.Logger, groupBy string, topo *cpuinfo.CPUTopology, onlineCPUs, reservedCPUs cpuset.CPUSet, fullPhysicalCPUsOnly bool, partition Partition, nodeDescribed bool) []groupedCPUDeviceInfo {
	var devices []groupedCPUDeviceInfo
	// build reports a device's own thread-per-core count and, under whole-core
	// allocation, drops any core the reservation split rather than publishing it
	// as capacity the allocator would refuse to hand out. The count is computed
	// from this device's own CPUs alone, so another device's non-uniform cores
	// never disable whole-core allocation here, and it is reported even when
	// fullPhysicalCPUsOnly is off, since it is a hardware fact.
	// What a group leaves this partition: the group's CPUs, without the
	// reservation and without whatever another partition claims.
	allocatableIn := func(groupCPUs cpuset.CPUSet) cpuset.CPUSet {
		return groupCPUs.Difference(reservedCPUs).Intersection(partition.CPUs)
	}
	build := func(name string, rawCPUs cpuset.CPUSet) (cpuset.CPUSet, int) {
		threadsPerCore := topo.CPUDetails.UniformThreadsPerCore(rawCPUs)
		if !fullPhysicalCPUsOnly {
			return rawCPUs, threadsPerCore
		}
		switch threadsPerCore {
		case 0:
			logger.Info("fullPhysicalCPUsOnly disabled for this device: its allocatable cores do not all have the same thread count",
				"device", name, "allocatableCPUs", rawCPUs.String())
			return rawCPUs, threadsPerCore
		case 1:
			// Nothing to keep together, so leave capacity untouched rather than
			// publishing a request policy with a step of one CPU.
			logger.V(2).Info("fullPhysicalCPUsOnly is a no-op for this device: every core has one thread", "device", name)
			return rawCPUs, threadsPerCore
		}
		return topo.CPUDetails.CompleteCores(rawCPUs), threadsPerCore
	}

	switch groupBy {
	case GROUP_BY_SOCKET:
		socketIDs := topo.CPUDetails.Sockets().List()
		for _, socketID := range socketIDs {
			name := partitionDeviceName(fmt.Sprintf("%s%03d", CPUDeviceSocketGroupedPrefix, socketID), partition)
			rawCPUs := allocatableIn(topo.CPUDetails.CPUsInSockets(socketID))
			allocatableCPUs, threadsPerCore := build(name, rawCPUs)
			if allocatableCPUs.Size() == 0 {
				continue
			}
			devices = append(devices, groupedCPUDeviceInfo{
				name:           name,
				cpus:           allocatableCPUs,
				socketID:       socketID,
				threadsPerCore: threadsPerCore,
			})
		}
	case GROUP_BY_NUMA_NODE:
		numaNodeIDs := topo.CPUDetails.NUMANodes().List()
		for _, numaID := range numaNodeIDs {
			name := partitionDeviceName(fmt.Sprintf("%s%03d", CPUDeviceNUMAGroupedPrefix, numaID), partition)
			rawCPUs := allocatableIn(topo.CPUDetails.CPUsInNUMANodes(numaID))
			allocatableCPUs, threadsPerCore := build(name, rawCPUs)
			if allocatableCPUs.Size() == 0 {
				continue
			}

			// All CPUs in a NUMA node belong to the same socket.
			anyCPU := allocatableCPUs.UnsortedList()[0]
			devices = append(devices, groupedCPUDeviceInfo{
				name:           name,
				cpus:           allocatableCPUs,
				socketID:       topo.CPUDetails[anyCPU].SocketID,
				numaNodeID:     numaID,
				threadsPerCore: threadsPerCore,
			})
		}
	case GROUP_BY_MACHINE:
		name := partitionDeviceName(CPUDeviceMachineGrouped, partition)
		rawCPUs := allocatableIn(onlineCPUs)
		allocatableCPUs, threadsPerCore := build(name, rawCPUs)
		// A node with one partition publishes the machine device whatever the
		// reservation leaves in it, which is what an unpartitioned node has
		// always done; once the cores are described, an empty partition is a
		// description of nothing and publishes nothing.
		if allocatableCPUs.IsEmpty() && nodeDescribed {
			break
		}
		devices = append(devices, groupedCPUDeviceInfo{
			name:           name,
			cpus:           allocatableCPUs,
			threadsPerCore: threadsPerCore,
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

	cpuInfoMap := make(map[int]cpuinfo.CPUInfo, len(topo.CPUDetails))
	availableCPUs := []cpuinfo.CPUInfo{}
	for _, cpu := range topo.CPUDetails {
		cpuInfoMap[cpu.CpuID] = cpu
		if !reservedCPUs[cpu.CpuID] {
			availableCPUs = append(availableCPUs, cpu)
		}
	}
	sort.Slice(availableCPUs, func(i, j int) bool {
		return availableCPUs[i].CpuID < availableCPUs[j].CpuID
	})

	// Grouped by SiblingCPUSet rather than the 2-way-only SiblingCPUID, so a
	// core with more than two threads (POWER SMT4/8) gets every one of its
	// siblings in the same group instead of becoming as many singleton groups
	// as it has threads.
	processedCpus := make(map[int]bool)
	coreGroups := [][]cpuinfo.CPUInfo{}
	for _, cpu := range availableCPUs {
		if processedCpus[cpu.CpuID] {
			continue
		}
		var group []cpuinfo.CPUInfo
		for _, siblingID := range cpu.SiblingCPUSet.List() {
			if reservedCPUs[siblingID] || processedCpus[siblingID] {
				continue
			}
			sibling, ok := cpuInfoMap[siblingID]
			if !ok {
				continue
			}
			group = append(group, sibling)
			processedCpus[siblingID] = true
		}
		coreGroups = append(coreGroups, group)
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
//
// smtEnabled and the published thread count are per device, from that device's
// own threadsPerCore, not the node-wide topology: a reduced- or non-uniform-SMT
// device must not advertise the same answer as a full-SMT one. The request
// policy stays gated by fullPhysicalCPUsOnly, since it is a promise about
// allocation, not a description of the hardware.
//
// CCX-FORK: upstream publishes neither the partition attributes nor the taint.
func createGroupedCPUDeviceSlices(logger logr.Logger, groupBy string, deviceInfos []groupedCPUDeviceInfo, pcieRootMapper *store.PCIeRootMapper, topo *cpuinfo.CPUTopology, nodeAllocatableResources bool, fullPhysicalCPUsOnly bool, partition Partition) []resourceapi.Device {
	logger.V(4).Info("creating grouped CPU devices")
	var devices []resourceapi.Device

	for _, deviceInfo := range deviceInfos {
		availableCPUs := int64(deviceInfo.cpus.Size())
		smtEnabled := deviceInfo.threadsPerCore > 1
		requestPolicyStep := 0
		if fullPhysicalCPUsOnly {
			requestPolicyStep = deviceInfo.threadsPerCore
		}
		deviceCapacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			CPUResourceQualifiedName: {
				Value:         *resource.NewQuantity(availableCPUs, resource.DecimalSI),
				RequestPolicy: wholeCoreRequestPolicy(availableCPUs, requestPolicyStep),
			},
		}

		switch groupBy {
		case GROUP_BY_SOCKET:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSocketID:       {IntValue: new(int64(deviceInfo.socketID))},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeSMTEnabled:     {BoolValue: new(smtEnabled)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			addPartitionAttributes(deviceAttrs, partition)
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
				Taints:                   partitionTaints(partition),
			})
		case GROUP_BY_NUMA_NODE:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				// DRA standard attributes first
				deviceattribute.StandardDeviceAttributeNUMANode: {IntValue: new(int64(deviceInfo.numaNodeID))},
				// Driver-specific/non-standard attributes next
				AttributeSocketID:       {IntValue: new(int64(deviceInfo.socketID))},
				AttributeSMTEnabled:     {BoolValue: new(smtEnabled)},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			addCompatibilityAttributes(deviceAttrs, int64(deviceInfo.numaNodeID))
			addPartitionAttributes(deviceAttrs, partition)
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)

			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
				Taints:                   partitionTaints(partition),
			})
		case GROUP_BY_MACHINE:
			deviceAttrs := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
				AttributeSMTEnabled:     {BoolValue: new(smtEnabled)},
				AttributeNumCPUs:        {IntValue: new(availableCPUs)},
				AttributeThreadsPerCore: {IntValue: new(int64(deviceInfo.threadsPerCore))},
			}
			addPartitionAttributes(deviceAttrs, partition)
			addUncoreCacheAttributes(topo, deviceAttrs, deviceInfo.cpus)
			addPCIeRootsAttribute(pcieRootMapper, deviceAttrs, deviceInfo.cpus.UnsortedList()...)
			devices = append(devices, resourceapi.Device{
				Name:                     deviceInfo.name,
				Attributes:               deviceAttrs,
				Capacity:                 deviceCapacity,
				AllowMultipleAllocations: new(true),
				NodeAllocatableResources: groupedCPUNodeAllocatable(nodeAllocatableResources),
				Taints:                   partitionTaints(partition),
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
