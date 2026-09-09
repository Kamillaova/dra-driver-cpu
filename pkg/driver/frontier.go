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
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

func scopeFrontierKey(partition string, numaNodeID int) string {
	return fmt.Sprintf("%s/%d", partition, numaNodeID)
}

func (cp *CPUDriver) frontier() (map[string]string, map[string]string) {
	if cp.topology.cpuTopology == nil || cp.topology.devicesByPartition == nil || cp.cpuDeviceGroupBy != device.GROUP_BY_UNCORE_CACHE {
		return nil, nil
	}
	online := cp.topology.onlineCPUs
	if online.IsEmpty() && cp.topology.cpuTopology != nil {
		online = cp.topology.cpuTopology.CPUDetails.CPUs()
	}
	allocatable := cp.defragAllocatable(online)
	partitions := cp.defragPartitions(allocatable)
	if len(partitions) == 0 {
		return nil, nil
	}

	occupied := cp.occupiedDevices()
	partDevices := cp.partitionDevicesMap()
	rounds := make(map[string]string)
	input := make(map[string]string)

	for _, partition := range partitions {
		partitionDevices := partDevices[partition.Name]
		if len(partitionDevices) == 0 {
			continue
		}
		numaNodes := cp.partitionNUMANodes(partitionDevices)
		for _, numaNodeID := range numaNodes {
			key := scopeFrontierKey(partition.Name, numaNodeID)
			if cp.nodeIsPoisoned(numaNodeID) {
				rounds[key] = unreachableFrontier()
				input[key] = ""
				continue
			}
			scope := defragScope{numaNodeID: numaNodeID, partition: partition.Name}
			view, ok := cp.defragView(logr.Discard(), scope, online)
			if !ok {
				rounds[key] = unreachableFrontier()
				input[key] = ""
				continue
			}
			numaDevices := cp.filterDevicesByNUMANode(partitionDevices, numaNodeID)
			ordered := cp.orderCacheDevices(numaDevices, occupied)

			fields := make([]string, api.RepairRoundsFields)
			for k := 1; k <= api.RepairRoundsFields; k++ {
				fields[k-1] = cp.simulateSplitLandingAndSearch(view, k, ordered)
			}
			rounds[key] = strings.Join(fields, ",")

			uids := make([]types.UID, 0, len(view.placements))
			for _, p := range view.placements {
				uids = append(uids, p.ClaimUID)
			}
			input[key] = api.FrontierInputDigest(uids)
		}
	}
	return rounds, input
}

func unreachableFrontier() string {
	return strings.Repeat(api.RepairRoundsUnreachable+",", api.RepairRoundsFields-1) + api.RepairRoundsUnreachable
}

func (cp *CPUDriver) partitionDevicesMap() map[string][]resourceapi.Device {
	res := make(map[string][]resourceapi.Device)
	for _, partitionDevices := range cp.topology.devicesByPartition {
		if len(partitionDevices) == 0 {
			continue
		}
		pAttr, ok := partitionDevices[0].Attributes[device.AttributePartition]
		if !ok || pAttr.StringValue == nil {
			continue
		}
		res[*pAttr.StringValue] = partitionDevices
	}
	return res
}

func (cp *CPUDriver) partitionNUMANodes(devices []resourceapi.Device) []int {
	seen := make(map[int]bool)
	var nodes []int
	for _, dev := range devices {
		nodeID, ok := cp.topology.deviceNameToNUMANodeID[dev.Name]
		if !ok || seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		nodes = append(nodes, nodeID)
	}
	sort.Ints(nodes)
	return nodes
}

func (cp *CPUDriver) filterDevicesByNUMANode(devices []resourceapi.Device, numaNodeID int) []resourceapi.Device {
	var filtered []resourceapi.Device
	for _, dev := range devices {
		if cp.topology.deviceNameToNUMANodeID[dev.Name] == numaNodeID && !cp.topology.deviceIsPool(dev.Name) {
			filtered = append(filtered, dev)
		}
	}
	return filtered
}

func (cp *CPUDriver) simulateSplitLandingAndSearch(view defragScopeView, k int, orderedDevices []resourceapi.Device) string {
	largestCacheCPUs := 0
	for _, cacheID := range view.topology.Caches() {
		if sz := view.topology.CPUsInCache(cacheID).Size(); sz > largestCacheCPUs {
			largestCacheCPUs = sz
		}
	}
	if largestCacheCPUs <= 0 {
		return api.RepairRoundsUnreachable
	}
	totalNeeded := k * largestCacheCPUs
	if totalNeeded > view.topology.CPUs().Size() {
		return api.RepairRoundsUnreachable
	}

	type splitSpec struct {
		numCaches    int
		cpusPerCache int
	}
	var candidates []splitSpec
	if largestCacheCPUs >= 2 && largestCacheCPUs%2 == 0 {
		candidates = append(candidates, splitSpec{numCaches: 2 * k, cpusPerCache: largestCacheCPUs / 2})
	}
	if largestCacheCPUs >= 4 && largestCacheCPUs%4 == 0 {
		candidates = append(candidates, splitSpec{numCaches: 4 * k, cpusPerCache: largestCacheCPUs / 4})
	}

	sel := cp.defragSelector(logr.Discard(), view.threadsPerCore)
	var landingCPUs cpuset.CPUSet
	for _, spec := range candidates {
		allocated, ok := cp.tryFirstAvailableSplit(view, spec.numCaches, spec.cpusPerCache, orderedDevices, sel)
		if ok {
			landingCPUs = allocated
			break
		}
	}
	if landingCPUs.IsEmpty() {
		return api.RepairRoundsUnreachable
	}

	simUID := types.UID("__frontier_simulated__")
	simPlacement := defrag.Placement{ClaimUID: simUID, CPUs: landingCPUs}
	simPlacements := append(slices.Clone(view.placements), simPlacement)
	simFree := view.free.Difference(landingCPUs)

	goal := defrag.GoalMakeClaimWhole{ClaimUID: simUID}
	opts := defrag.ExactOptions{
		AllowSwaps: cp.defrag.allowTransientOverlap,
		MaxDepth:   3,
		Eligible: func(uid types.UID) bool {
			if uid == simUID {
				return true
			}
			return cp.claimMovableForExact(uid)
		},
	}
	inFlight := cp.allocatedUnpreparedCPUs(view.scope.numaNodeID).Intersection(view.topology.CPUs())
	plan, err := defrag.ExactSearch(view.topology, simPlacements, simFree, inFlight, goal, sel, opts)
	if err != nil || !plan.Status.Feasible() || len(plan.Moves) == 0 {
		return api.RepairRoundsUnreachable
	}
	rounds := planRounds(plan.Moves)
	if rounds < 1 || rounds > 3 {
		return api.RepairRoundsUnreachable
	}
	return strconv.Itoa(rounds)
}

func (cp *CPUDriver) tryFirstAvailableSplit(view defragScopeView, numCaches int, cpusPerCache int, orderedDevices []resourceapi.Device, sel defrag.Selector) (cpuset.CPUSet, bool) {
	var placed cpuset.CPUSet
	cachesUsed := 0
	seenCaches := make(map[int]bool)

	for _, dev := range orderedDevices {
		devCPUs := cp.topology.deviceNameToCPUs[dev.Name]
		if devCPUs.IsEmpty() {
			continue
		}
		cacheID, ok := view.topology.CacheOf(devCPUs.List()[0])
		if !ok || seenCaches[cacheID] {
			continue
		}
		freeInCache := view.free.Intersection(view.topology.CPUsInCache(cacheID))
		if freeInCache.Size() >= cpusPerCache {
			var picked cpuset.CPUSet
			if sel != nil {
				p, err := sel(freeInCache, cpusPerCache)
				if err == nil && p.Size() == cpusPerCache {
					picked = p
				}
			}
			if picked.IsEmpty() {
				picked = cpuset.New(freeInCache.List()[:cpusPerCache]...)
			}
			seenCaches[cacheID] = true
			cachesUsed++
			placed = placed.Union(picked)
			if cachesUsed == numCaches {
				return placed, true
			}
		}
	}
	return cpuset.New(), false
}

func planRounds(moves []defrag.Move) int {
	exchanges := make(map[int]bool)
	rounds := 0
	for _, m := range moves {
		if m.Exchange == 0 {
			rounds++
		} else if !exchanges[m.Exchange] {
			exchanges[m.Exchange] = true
			rounds++
		}
	}
	return rounds
}

func (cp *CPUDriver) attachFrontierAttributes(devices []resourceapi.Device, frontierByScope map[string]string, inputByScope map[string]string) []resourceapi.Device {
	if cp.cpuDeviceGroupBy != device.GROUP_BY_UNCORE_CACHE || len(devices) == 0 {
		return devices
	}
	if cp.topology.deviceIsPool(devices[0].Name) {
		return devices
	}
	rAttr, ok := devices[0].Attributes[device.AttributeRole]
	if !ok || rAttr.StringValue == nil || (*rAttr.StringValue != device.PARTITION_ROLE_DEFAULT && *rAttr.StringValue != device.PARTITION_ROLE_EXCLUSIVE) {
		return devices
	}

	attached := slices.Clone(devices)
	for i, dev := range attached {
		numaNodeID := cp.topology.deviceNameToNUMANodeID[dev.Name]
		pAttr, ok := dev.Attributes[device.AttributePartition]
		if !ok || pAttr.StringValue == nil {
			continue
		}
		key := scopeFrontierKey(*pAttr.StringValue, numaNodeID)
		repairRounds, ok := frontierByScope[key]
		if !ok {
			repairRounds = unreachableFrontier()
		}
		attrs := maps.Clone(dev.Attributes)
		if attrs == nil {
			attrs = make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)
		}
		attrs[device.AttributeRepairRounds] = resourceapi.DeviceAttribute{
			StringValue: new(repairRounds),
		}
		if digest, ok := inputByScope[key]; ok && digest != "" {
			attrs[device.AttributeFrontierInput] = resourceapi.DeviceAttribute{
				StringValue: new(digest),
			}
		}
		dev.Attributes = attrs
		attached[i] = dev
	}
	return attached
}
