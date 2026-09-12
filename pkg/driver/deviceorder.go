/*
Copyright 2026 The Kubernetes Authors.

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
	"maps"
	"slices"
	"sort"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
)

// chunkDevices cuts each partition's devices into ResourceSlice-sized chunks,
// in the order they are published. A partition's devices are never mixed with
// another's, so a named partition's taints stay in its own slices.
//
// Called with applyMu held.
func (cp *CPUDriver) chunkDevices(occupied map[string]bool) [][]resourceapi.Device {
	var chunks [][]resourceapi.Device
	for _, partitionDevices := range cp.topology.devicesByPartition {
		if len(partitionDevices) == 0 {
			continue
		}
		ordered := cp.orderCacheDevices(partitionDevices, occupied)
		chunks = append(chunks, slices.Collect(slices.Chunk(ordered, cp.devicesPerResourceSlice))...)
	}
	return chunks
}

// orderCacheDevices puts the caches that hold a claim before the ones that hold
// none under the pack strategy, and after them under spread. The allocator
// takes the first device that fits, so under pack a claim meets an already
// tenanted cache first and the clean ones stay whole for the claims that need a
// whole cache; under spread it meets a clean cache first and gets an uncontended
// L3 while the node has slack.
//
// Whether a cache holds a claim is the whole of the order, not how full it is:
// the slices are republished when a cache changes between the two states, so a
// finer key would go stale between claims and claim a precision first fit never
// had. The driver's own selector, not this order, decides which CPUs inside a
// device a claim gets.
//
// NUMA node is the primary key whatever the strategy orders inside it. The
// allocator walks devices in slice order and backtracks when a NUMA constraint
// fails, and that backtracking stays bounded only while a node's devices are
// one run.
//
// Called with applyMu held.
func (cp *CPUDriver) orderCacheDevices(devices []resourceapi.Device, occupied map[string]bool) []resourceapi.Device {
	if cp.cpuDeviceGroupBy != device.GROUP_BY_UNCORE_CACHE || len(devices) == 0 {
		return devices
	}
	// A pool's devices are not caches: every claim that asks for one holds all of
	// it, so there is no emptiness to order them by and no cache for a claim to
	// meet first. They are published as they were built.
	if cp.topology.deviceNameToRole[devices[0].Name] == device.PARTITION_ROLE_SHARED {
		return devices
	}
	ordered := slices.Clone(devices)
	occupiedFirst := cp.placementPolicy != coreselect.Spread
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i].Name, ordered[j].Name
		if leftNUMA, rightNUMA := cp.topology.deviceNameToNUMANodeID[left], cp.topology.deviceNameToNUMANodeID[right]; leftNUMA != rightNUMA {
			return leftNUMA < rightNUMA
		}
		return occupied[left] == occupiedFirst && occupied[right] != occupiedFirst
	})
	return ordered
}

// occupiedDevices is the input the published order is a function of. Called
// with applyMu held.
func (cp *CPUDriver) occupiedDevices() map[string]bool {
	prepared := cp.cpuAllocationStore.GetPreparedCPUs()
	occupied := make(map[string]bool, len(cp.topology.deviceNameToCPUs))
	for name, cpus := range cp.topology.deviceNameToCPUs {
		occupied[name] = !cpus.Intersection(prepared).IsEmpty()
	}
	return occupied
}

// refreshDeviceOrder returns the chunks to publish. The order and the occupancy
// it was computed from are recorded together, because a later hook decides
// whether to publish again by comparing the two; recording one without the other
// is what would make that comparison lie.
//
// Called with applyMu held.
func (cp *CPUDriver) refreshDeviceOrder() [][]resourceapi.Device {
	if cp.topology.devicesByPartition == nil {
		return nil
	}
	occupied := cp.occupiedDevices()
	cp.publishedOccupancy = occupied
	cp.topology.deviceSlices = cp.chunkDevices(occupied)
	return cp.topology.deviceSlices
}

// cacheOrderIsStale reports whether a cache has changed between holding a claim
// and holding none since the slices were last published, which is when the
// order they carry stops matching the node.
//
// Called with applyMu held.
func (cp *CPUDriver) cacheOrderIsStale() bool {
	if cp.cpuDeviceGroupBy != device.GROUP_BY_UNCORE_CACHE || cp.draPlugin == nil {
		return false
	}
	return !maps.Equal(cp.occupiedDevices(), cp.publishedOccupancy)
}
