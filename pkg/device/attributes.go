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
)

const (
	AttributeSocketID   resourceapi.QualifiedName = "dra.cpu/socketID"
	AttributeSMTEnabled resourceapi.QualifiedName = "dra.cpu/smtEnabled"
	AttributeCacheL3ID  resourceapi.QualifiedName = "dra.cpu/cacheL3ID"
	AttributeCoreType   resourceapi.QualifiedName = "dra.cpu/coreType"
	AttributeCoreID     resourceapi.QualifiedName = "dra.cpu/coreID"
	AttributeCPUID      resourceapi.QualifiedName = "dra.cpu/cpuID"
	AttributeNumCPUs    resourceapi.QualifiedName = "dra.cpu/numCPUs"
	// AttributeLargestUncoreCacheCPUs is the largest number of allocatable CPUs
	// sharing one uncore cache within a grouped device, i.e. the biggest claim
	// the group could satisfy from a single cache. It is not a per-cache size:
	// caches within a group can differ, so a claim asking for cache-aligned CPUs
	// must compare against the largest.
	AttributeLargestUncoreCacheCPUs resourceapi.QualifiedName = "dra.cpu/largestUncoreCacheCPUs"
	// AttributeUncoreCachesInGroup is how many uncore caches contribute
	// allocatable CPUs to a grouped device. One means cache alignment within the
	// group is trivially satisfied.
	AttributeUncoreCachesInGroup resourceapi.QualifiedName = "dra.cpu/uncoreCachesInGroup"
	// AttributeAllocatedNumCPUs is a metadata-only attribute (not published in
	// ResourceSlice) that indicates how many CPUs were allocated to a specific
	// claim from a grouped device's capacity.
	AttributeAllocatedNumCPUs resourceapi.QualifiedName = "dra.cpu/allocatedNumCPUs"
)

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
