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
	"strconv"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
)

// chunkDevices cuts each partition's devices into ResourceSlice-sized chunks,
// in the order they are published, and reports how many of them are published at
// their capacity floor. A partition's devices are never mixed with another's, so
// a named partition's taints stay in its own slices.
//
// Called with applyMu held.
func (cp *CPUDriver) chunkDevices(occupied map[string]bool, poisoned map[int]bool, mirror capacityMirror, frontier map[string]string, frontierInput map[string]string) ([][]resourceapi.Device, int) {
	var chunks [][]resourceapi.Device
	floored := 0
	for _, partitionDevices := range cp.topology.devicesByPartition {
		if len(partitionDevices) == 0 {
			continue
		}
		mirrored, atFloor := applyCapacityMirror(cp.orderCacheDevices(partitionDevices, occupied), mirror)
		floored += atFloor
		ordered := cp.fenceDevices(mirrored, poisoned)
		withFrontier := cp.attachFrontierAttributes(ordered, frontier, frontierInput)
		chunks = append(chunks, slices.Collect(slices.Chunk(withFrontier, cp.devicesPerResourceSlice))...)
	}
	return chunks, floored
}

// fenceDevices taints every device reaching into a NUMA node the driver has
// stopped vouching for, so the scheduler stops sending claims to CPUs whose
// owner it cannot name. The taint goes when the node reopens.
//
// Its own key, distinct from the partition taint: a toleration written for a
// partition must not also tolerate a fence, and nothing is expected to tolerate
// this one at all.
//
// Called with applyMu held.
func (cp *CPUDriver) fenceDevices(devices []resourceapi.Device, poisoned map[int]bool) []resourceapi.Device {
	if len(poisoned) == 0 {
		return devices
	}
	fenced := slices.Clone(devices)
	for i, dev := range fenced {
		for _, numaNodeID := range cp.poisonedNUMANodesOf(cp.topology.deviceNameToCPUs[dev.Name]) {
			dev.Taints = append(slices.Clone(dev.Taints), resourceapi.DeviceTaint{
				Key:    device.PoisonTaintKey,
				Value:  strconv.Itoa(numaNodeID),
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			})
		}
		fenced[i] = dev
	}
	return fenced
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
	if cp.topology.deviceIsPool(devices[0].Name) {
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

// refreshDeviceOrder returns the chunks to publish. The order and the inputs
// it was computed from are recorded together, because a later hook decides
// whether to publish again by comparing them; recording one without the others
// is what would make that comparison lie.
//
// Called with applyMu held.
func (cp *CPUDriver) refreshDeviceOrder() [][]resourceapi.Device {
	if cp.topology.devicesByPartition == nil {
		return nil
	}
	occupied, poisoned, mirror := cp.occupiedDevices(), cp.poisonedNUMANodes(), cp.capacityMirror()
	frontier, frontierInput := cp.frontier()
	cp.publishedOccupancy, cp.publishedPoison, cp.publishedCorrection = occupied, poisoned, mirror.corrections()
	cp.publishedFrontier, cp.publishedFrontierInput = frontier, frontierInput
	chunks, floored := cp.chunkDevices(occupied, poisoned, mirror, frontier, frontierInput)
	cp.topology.deviceSlices = chunks
	cp.metrics.SetFlooredCapacityDevices(floored)
	return cp.topology.deviceSlices
}

// publishedSlicesAreStale reports whether what the slices carry has stopped
// describing the node: a NUMA node fenced or reopened since they went out, a
// capacity whose correction has changed, a cache that has changed between
// holding a claim and holding none, or a frontier whose rounds or input digest
// differ from what was published.
//
// Called with applyMu held.
func (cp *CPUDriver) publishedSlicesAreStale() bool {
	if cp.draPlugin == nil {
		return false
	}
	if !maps.Equal(cp.poisonedNUMANodes(), cp.publishedPoison) {
		return true
	}
	// Before the grouping check: a claim leaves the device its allocation charged
	// under every grouping the driver may move a claim under, and only the device
	// order is particular to caches.
	if !maps.Equal(cp.capacityMirror().corrections(), cp.publishedCorrection) {
		return true
	}
	if cp.cpuDeviceGroupBy != device.GROUP_BY_UNCORE_CACHE {
		return false
	}
	if !maps.Equal(cp.occupiedDevices(), cp.publishedOccupancy) {
		return true
	}
	rounds, input := cp.frontier()
	return !maps.Equal(rounds, cp.publishedFrontier) || !maps.Equal(input, cp.publishedFrontierInput)
}
