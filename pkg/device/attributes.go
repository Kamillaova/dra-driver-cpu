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
	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/utils/cpuset"
)

const (
	// CPU_DEVICE_MODE_GROUPED exposes a single device for a group of CPUs.
	CPU_DEVICE_MODE_GROUPED = "grouped"
	// CPU_DEVICE_MODE_INDIVIDUAL exposes each CPU as a separate device.
	CPU_DEVICE_MODE_INDIVIDUAL = "individual"
)

const (
	// GROUP_BY_SOCKET groups CPUs by socket.
	GROUP_BY_SOCKET = "socket"
	// GROUP_BY_NUMA_NODE groups CPUs by NUMA node.
	GROUP_BY_NUMA_NODE = "numanode"
	// GROUP_BY_MACHINE groups CPUs by the entire machine.
	GROUP_BY_MACHINE = "machine"
	// GROUP_BY_UNCORE_CACHE groups CPUs by the uncore cache they share.
	GROUP_BY_UNCORE_CACHE = "uncorecache"
)

// CCX-FORK: upstream spells the names out here. A scheduler plugin reading
// these attributes cannot import this package without importing the whole
// driver, so the names live in the nested api module both repositories pin and
// this block is what the driver's own code keeps using.
const (
	AttributeSocketID               resourceapi.QualifiedName = api.AttributeSocketID
	AttributeSMTEnabled             resourceapi.QualifiedName = api.AttributeSMTEnabled
	AttributeCacheL3ID              resourceapi.QualifiedName = api.AttributeCacheL3ID
	AttributeCoreType               resourceapi.QualifiedName = api.AttributeCoreType
	AttributeCoreID                 resourceapi.QualifiedName = api.AttributeCoreID
	AttributeCPUID                  resourceapi.QualifiedName = api.AttributeCPUID
	AttributeNumCPUs                resourceapi.QualifiedName = api.AttributeNumCPUs
	AttributeThreadsPerCore         resourceapi.QualifiedName = api.AttributeThreadsPerCore
	AttributeLargestUncoreCacheCPUs resourceapi.QualifiedName = api.AttributeLargestUncoreCacheCPUs
	AttributeUncoreCachesInGroup    resourceapi.QualifiedName = api.AttributeUncoreCachesInGroup
	AttributePartition              resourceapi.QualifiedName = api.AttributePartition
	AttributeRole                   resourceapi.QualifiedName = api.AttributeRole
	AttributeAllocatedNumCPUs       resourceapi.QualifiedName = api.AttributeAllocatedNumCPUs
	AttributeCPUSet                 resourceapi.QualifiedName = api.AttributeCPUSet
	AttributeRelocatable            resourceapi.QualifiedName = api.AttributeRelocatable
	AttributeAlignment              resourceapi.QualifiedName = api.AttributeAlignment
)

// addPartitionAttributes names the partition a device's CPUs come from and what
// that partition is for. Published on every grouped device, including the
// implicit partition of an unpartitioned node, so that a device class selecting
// a partition means the same thing on every node in the fleet.
func addPartitionAttributes(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, partition Partition) {
	attrs[AttributePartition] = resourceapi.DeviceAttribute{StringValue: new(partition.Name)}
	attrs[AttributeRole] = resourceapi.DeviceAttribute{StringValue: new(partition.Role)}
}

// partitionTaints keeps a named partition's devices away from every claim that
// does not tolerate that partition by name. The implicit partition is untainted,
// so a claim that names no partition is allocated there and nowhere else.
func partitionTaints(partition Partition) []resourceapi.DeviceTaint {
	if !partition.Named() {
		return nil
	}
	return []resourceapi.DeviceTaint{{
		Key:    PartitionTaintKey,
		Value:  partition.Name,
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	}}
}

// addCompatibilityAttributes add attributes to enable compatibility (e.g. alignment) with other
// DRA resource drivers leveraging attributes which are not kubernetes standard.
// This is the "staging area" which enables attribute sharing until (or before) they become standard.
func addCompatibilityAttributes(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, numaID int64) {
	attrs["dra.net/numaNode"] = resourceapi.DeviceAttribute{IntValue: new(numaID)}
	attrs["dra.cpu/numaNodeID"] = resourceapi.DeviceAttribute{IntValue: new(numaID)}
}

// addUncoreCacheAttributes publishes the group's uncore cache geometry, so a
// claim can select nodes where cache-aligned placement is possible at all. The
// driver chooses which CPUs back a grouped claim, but it cannot align a claim
// larger than any single cache, and that limit varies by node type.
//
// groupCPUs must already exclude reserved CPUs: a cache half-consumed by the
// reservation cannot host a full-size claim, so the counts are of allocatable
// CPUs rather than of the topology.
//
// Nothing is published when any CPU reports an unknown cache (-1), rather than
// publishing a count derived from partial information.
func addUncoreCacheAttributes(topo *cpuinfo.CPUTopology, attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, groupCPUs cpuset.CPUSet) {
	cpusPerCache := make(map[int]int)
	for _, cpuID := range groupCPUs.List() {
		info, ok := topo.CPUDetails[cpuID]
		if !ok || info.UncoreCacheID == -1 {
			return
		}
		cpusPerCache[info.UncoreCacheID]++
	}
	if len(cpusPerCache) == 0 {
		return
	}

	largest := 0
	for _, n := range cpusPerCache {
		if n > largest {
			largest = n
		}
	}

	attrs[AttributeLargestUncoreCacheCPUs] = resourceapi.DeviceAttribute{IntValue: new(int64(largest))}
	attrs[AttributeUncoreCachesInGroup] = resourceapi.DeviceAttribute{IntValue: new(int64(len(cpusPerCache)))}
}
