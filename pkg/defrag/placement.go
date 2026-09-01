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

// Package defrag models how CPU claims are spread over a node's uncore caches
// and plans moves that reduce that spread.
//
// Nothing here touches the driver, the API server or the runtime: a plan is a
// pure function of a placement snapshot and the node's topology.
package defrag

import (
	"fmt"
	"sort"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// Placement is the CPUs one claim holds within one NUMA node.
//
// A claim spanning several NUMA nodes has one Placement per node, because a move
// may only shuffle CPUs within a node: the driver never sets cpuset.mems, so a
// claim's memory locality is exactly its CPUs' NUMA footprint.
type Placement struct {
	ClaimUID types.UID
	CPUs     cpuset.CPUSet
}

// Topology is the uncore cache geometry of one NUMA node, over the CPUs the
// driver may place claims on.
type Topology struct {
	numaNodeID  int
	cpus        cpuset.CPUSet
	cacheOfCPU  map[int]int
	cpusInCache map[int]cpuset.CPUSet
	cacheIDs    []int
	// Cache capacities in descending order, which is the order MinSpread has to
	// consume them in.
	capacities []int
}

// NewTopology describes numaNodeID over the subset of allocatable that lies in
// it. Pass the CPUs the driver may place claims on -- online and not reserved --
// rather than the CPUs that happen to be free: a cache's capacity includes the
// CPUs the claims occupying it already hold.
//
// It fails when the node has no allocatable CPUs, and when any of them reports no
// uncore cache: neither node can be defragmented, and the caller should skip it.
// A node whose CPUs all share one cache is described normally -- its cost is then
// identically zero, so it needs no special case.
func NewTopology(topo *cpuinfo.CPUTopology, numaNodeID int, allocatable cpuset.CPUSet) (*Topology, error) {
	cpus := allocatable.Intersection(topo.CPUDetails.CPUsInNUMANodes(numaNodeID))
	if cpus.IsEmpty() {
		return nil, fmt.Errorf("NUMA node %d has no allocatable CPUs", numaNodeID)
	}

	t := &Topology{
		numaNodeID:  numaNodeID,
		cpus:        cpus,
		cacheOfCPU:  make(map[int]int, cpus.Size()),
		cpusInCache: make(map[int]cpuset.CPUSet),
	}
	cpuIDsInCache := map[int][]int{}
	for cpuID, info := range topo.CPUDetails.KeepOnly(cpus) {
		if info.UncoreCacheID == -1 {
			return nil, fmt.Errorf("NUMA node %d reports no uncore cache for CPU %d", numaNodeID, cpuID)
		}
		t.cacheOfCPU[cpuID] = info.UncoreCacheID
		cpuIDsInCache[info.UncoreCacheID] = append(cpuIDsInCache[info.UncoreCacheID], cpuID)
	}

	for cacheID := range cpuIDsInCache {
		t.cacheIDs = append(t.cacheIDs, cacheID)
	}
	sort.Ints(t.cacheIDs)
	for _, cacheID := range t.cacheIDs {
		inCache := cpuset.New(cpuIDsInCache[cacheID]...)
		t.cpusInCache[cacheID] = inCache
		t.capacities = append(t.capacities, inCache.Size())
	}
	sort.Sort(sort.Reverse(sort.IntSlice(t.capacities)))
	return t, nil
}

// NUMANodeID returns the node this topology describes.
func (t *Topology) NUMANodeID() int { return t.numaNodeID }

// CPUs returns the allocatable CPUs of this node. Every CPU handed to Spread,
// ExcessSpread or Cost must be one of these.
func (t *Topology) CPUs() cpuset.CPUSet { return t.cpus }

// Caches returns the node's uncore cache IDs in ascending order.
func (t *Topology) Caches() []int {
	return append([]int(nil), t.cacheIDs...)
}

// CPUsInCache returns the node's allocatable CPUs belonging to one cache.
func (t *Topology) CPUsInCache(cacheID int) cpuset.CPUSet {
	return t.cpusInCache[cacheID]
}

// CacheOf returns the cache a CPU belongs to, and whether the CPU is allocatable
// in this node at all.
func (t *Topology) CacheOf(cpuID int) (int, bool) {
	cacheID, ok := t.cacheOfCPU[cpuID]
	return cacheID, ok
}

// Spread returns how many uncore caches a placement touches. CPUs outside this
// node's allocatable set are not counted, so check CPUs() first if the caller
// cannot rule them out.
func (t *Topology) Spread(cpus cpuset.CPUSet) int {
	caches := map[int]struct{}{}
	for _, cpuID := range cpus.UnsortedList() {
		if cacheID, ok := t.cacheOfCPU[cpuID]; ok {
			caches[cacheID] = struct{}{}
		}
	}
	return len(caches)
}

// MinSpread returns the fewest uncore caches that can hold numCPUs, which is the
// spread an ideally placed claim of that size achieves.
//
// It counts allocatable capacity rather than the caches' physical sizes. A cache
// holding reserved CPUs cannot take a claim as large as it looks, and measuring
// it by its full size would put the minimum out of reach and make every claim
// there look permanently misplaced.
//
// A request larger than the whole node returns the node's cache count: all of
// them, which is as close to an answer as there is.
func (t *Topology) MinSpread(numCPUs int) int {
	if numCPUs <= 0 {
		return 0
	}
	remaining := numCPUs
	for i, capacity := range t.capacities {
		remaining -= capacity
		if remaining <= 0 {
			return i + 1
		}
	}
	return len(t.capacities)
}

// ExcessSpread returns how many more uncore caches a placement touches than a
// claim of its size has to. Zero means it is placed as well as its size allows.
func (t *Topology) ExcessSpread(cpus cpuset.CPUSet) int {
	excess := t.Spread(cpus) - t.MinSpread(cpus.Size())
	if excess < 0 {
		return 0
	}
	return excess
}

// Cost is the node's total avoidable cache spread, and the quantity a plan
// exists to reduce. It is zero exactly when no claim on the node can be improved
// by moving it, and bounded below, so a node that reaches zero stays there.
func (t *Topology) Cost(placements []Placement) int {
	total := 0
	for _, p := range placements {
		total += t.ExcessSpread(p.CPUs)
	}
	return total
}

// PlacementsByNUMANode splits each claim's CPUs into one placement per NUMA node
// it occupies. Placements are ordered by claim UID so a plan built from them does
// not depend on map iteration order.
func PlacementsByNUMANode(topo *cpuinfo.CPUTopology, allocations map[types.UID]cpuset.CPUSet) map[int][]Placement {
	byNode := map[int][]Placement{}
	claimUIDs := make([]types.UID, 0, len(allocations))
	for claimUID := range allocations {
		claimUIDs = append(claimUIDs, claimUID)
	}
	sort.Slice(claimUIDs, func(i, j int) bool { return claimUIDs[i] < claimUIDs[j] })

	for _, claimUID := range claimUIDs {
		perNode := map[int][]int{}
		for _, cpuID := range allocations[claimUID].UnsortedList() {
			info, ok := topo.CPUDetails[cpuID]
			if !ok {
				continue
			}
			perNode[info.NUMANodeID] = append(perNode[info.NUMANodeID], cpuID)
		}
		for numaNodeID, cpuIDs := range perNode {
			byNode[numaNodeID] = append(byNode[numaNodeID], Placement{
				ClaimUID: claimUID,
				CPUs:     cpuset.New(cpuIDs...),
			})
		}
	}
	return byNode
}
