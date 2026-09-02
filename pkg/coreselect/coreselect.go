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

// Package coreselect picks CPUs a whole physical core at a time.
//
// It exists separately from pkg/cpumanager because that package mirrors
// kubelet's allocator, whose core-level helpers key on CoreID alone and so
// conflate same-numbered cores on different sockets. Selection here keys on the
// full physical-core identity from pkg/cpuinfo.
package coreselect

import (
	"fmt"
	"sort"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/utils/cpuset"
)

// Policy is how a claim that fits inside one uncore cache chooses among the
// caches that can hold it. Claims larger than any cache always take the largest
// caches first under either policy: spanning fewer caches beats spreading.
type Policy string

const (
	// Pack fills the fullest cache that still fits the claim, keeping clean
	// caches clean for larger claims at the price of co-tenancy in the used
	// ones.
	Pack Policy = "pack"
	// Spread fills the least-tenanted cache that fits the claim, giving each
	// claim its own cache while there is slack, at the price of clean caches:
	// a later whole-cache claim relies on defragmentation to consolidate the
	// small tenants back together. Tenancy, not raw free space: a cache
	// diminished by the configured carve-outs but hosting nothing still beats
	// a bigger cache that already hosts a claim.
	Spread Policy = "spread"
)

// TakeWholeCores is TakeWholeCoresPolicy with the Pack policy, which is the
// packing the upstream allocator produces.
func TakeWholeCores(topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	return TakeWholeCoresPolicy(topo, available, numCPUs, Pack, cpuset.New())
}

// TakeWholeCoresPolicy returns exactly numCPUs CPUs drawn from available as whole
// physical cores, so no core is ever split between this result and anything else.
//
// Cores are chosen to span as few uncore caches as possible; among the caches
// that can hold the remainder, the policy decides which one. When no single
// cache can hold it, the largest cache is used regardless of policy, since
// spanning fewer caches beats spreading evenly.
//
// carved names CPUs permanently withheld by configuration (the reserved set,
// a static shared pool). Spread needs it to tell an untouched-but-diminished
// cache from one another claim already lives in; Pack ignores it.
//
// numCPUs must be a multiple of the core size; available is filtered to complete
// cores first, so a core the caller has partly consumed elsewhere is not offered.
func TakeWholeCoresPolicy(topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int, policy Policy, carved cpuset.CPUSet) (cpuset.CPUSet, error) {
	if numCPUs == 0 {
		return cpuset.New(), nil
	}
	if numCPUs < 0 {
		return cpuset.New(), fmt.Errorf("cannot take a negative number of CPUs (%d)", numCPUs)
	}

	details := topo.CPUDetails
	usable := details.CompleteCores(available)
	if usable.Size() < numCPUs {
		return cpuset.New(), fmt.Errorf("not enough whole cores available to satisfy request: requested=%d, available in complete cores=%d", numCPUs, usable.Size())
	}

	coresByCache := groupCoresByCache(details, usable)
	tenancy := map[int]int{}
	if policy == Spread {
		// CPUs neither offered nor carved away are held by someone.
		taken := details.CPUs().Difference(available).Difference(carved)
		for _, cpuID := range taken.List() {
			tenancy[details[cpuID].UncoreCacheID]++
		}
	}

	result := cpuset.New()
	remaining := numCPUs
	for remaining > 0 {
		cacheID, ok := pickCache(coresByCache, tenancy, remaining, policy)
		if !ok {
			return cpuset.New(), fmt.Errorf("failed to take %d CPUs as whole cores: %d CPUs still needed", numCPUs, remaining)
		}
		cores := coresByCache[cacheID]
		for len(cores) > 0 && remaining > 0 {
			core := cores[0]
			cores = cores[1:]
			if core.Size() > remaining {
				// numCPUs is a whole-core multiple and every core here has the
				// same size, so this cannot happen; refuse rather than split.
				return cpuset.New(), fmt.Errorf("request of %d CPUs is not a whole multiple of the %d-CPU core size", numCPUs, core.Size())
			}
			result = result.Union(core)
			remaining -= core.Size()
		}
		if len(cores) == 0 {
			delete(coresByCache, cacheID)
		} else {
			coresByCache[cacheID] = cores
		}
	}
	return result, nil
}

// groupCoresByCache buckets the complete cores in usable by uncore cache, each
// bucket ordered by lowest CPU ID so selection is deterministic. CPUs whose cache
// is unknown are grouped together under that unknown id.
func groupCoresByCache(details cpuinfo.CPUDetails, usable cpuset.CPUSet) map[int][]cpuset.CPUSet {
	seen := make(map[cpuinfo.CoreLocation]struct{})
	byCache := make(map[int][]cpuset.CPUSet)
	for _, cpuID := range usable.List() {
		loc, ok := details.CoreOf(cpuID)
		if !ok {
			continue
		}
		if _, dup := seen[loc]; dup {
			continue
		}
		seen[loc] = struct{}{}
		byCache[details[cpuID].UncoreCacheID] = append(byCache[details[cpuID].UncoreCacheID], details.SiblingsOf(cpuID).Intersection(usable))
	}
	for cacheID := range byCache {
		cores := byCache[cacheID]
		sort.Slice(cores, func(i, j int) bool {
			return cores[i].List()[0] < cores[j].List()[0]
		})
	}
	return byCache
}

// pickCache returns the cache to draw the next cores from: under Pack the
// smallest one that can still hold all of remaining; under Spread the one with
// the fewest tenant CPUs, most free space breaking that tie. When none can hold
// it, the largest overall, under either policy. Remaining ties break on the
// lower cache id so the choice is deterministic.
func pickCache(coresByCache map[int][]cpuset.CPUSet, tenancy map[int]int, remaining int, policy Policy) (int, bool) {
	type candidate struct {
		id, free, tenants int
	}
	var chosen, largest *candidate

	prefer := func(a, b *candidate) bool {
		if policy == Spread {
			if a.tenants != b.tenants {
				return a.tenants < b.tenants
			}
			if a.free != b.free {
				return a.free > b.free
			}
		} else if a.free != b.free {
			return a.free < b.free
		}
		return a.id < b.id
	}

	for cacheID, cores := range coresByCache {
		free := 0
		for _, core := range cores {
			free += core.Size()
		}
		if free == 0 {
			continue
		}
		c := &candidate{id: cacheID, free: free, tenants: tenancy[cacheID]}
		if largest == nil || c.free > largest.free || (c.free == largest.free && c.id < largest.id) {
			largest = c
		}
		if c.free >= remaining && (chosen == nil || prefer(c, chosen)) {
			chosen = c
		}
	}
	if chosen != nil {
		return chosen.id, true
	}
	if largest != nil {
		return largest.id, true
	}
	return 0, false
}

// TakeSpreadCPUs is the Spread policy at single-CPU granularity, for
// deployments without whole-core allocation. Caches are ranked exactly as
// TakeWholeCoresPolicy ranks them -- fewest tenant CPUs first, carve-outs not
// counting as tenants -- and inside a cache fully free cores are drained before
// a partly used core is split, so two claims share a physical core's siblings
// only when nothing else is left in the cache the policy chose.
func TakeSpreadCPUs(topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int, carved cpuset.CPUSet) (cpuset.CPUSet, error) {
	if numCPUs == 0 {
		return cpuset.New(), nil
	}
	if numCPUs < 0 {
		return cpuset.New(), fmt.Errorf("cannot take a negative number of CPUs (%d)", numCPUs)
	}
	if available.Size() < numCPUs {
		return cpuset.New(), fmt.Errorf("not enough CPUs available to satisfy request: requested=%d, available=%d", numCPUs, available.Size())
	}

	details := topo.CPUDetails
	byCache := map[int][]cpuset.CPUSet{}
	seen := map[cpuinfo.CoreLocation]struct{}{}
	for _, cpuID := range available.List() {
		loc, ok := details.CoreOf(cpuID)
		if !ok {
			continue
		}
		if _, dup := seen[loc]; dup {
			continue
		}
		seen[loc] = struct{}{}
		byCache[details[cpuID].UncoreCacheID] = append(byCache[details[cpuID].UncoreCacheID], details.SiblingsOf(cpuID).Intersection(available))
	}
	for cacheID := range byCache {
		cores := byCache[cacheID]
		sort.Slice(cores, func(i, j int) bool {
			ci, cj := cores[i], cores[j]
			wholeI := details.SiblingsOf(ci.List()[0]).IsSubsetOf(available)
			wholeJ := details.SiblingsOf(cj.List()[0]).IsSubsetOf(available)
			if wholeI != wholeJ {
				return wholeI
			}
			return ci.List()[0] < cj.List()[0]
		})
	}

	tenancy := map[int]int{}
	taken := details.CPUs().Difference(available).Difference(carved)
	for _, cpuID := range taken.List() {
		tenancy[details[cpuID].UncoreCacheID]++
	}

	result := cpuset.New()
	remaining := numCPUs
	for remaining > 0 {
		cacheID, ok := pickCache(byCache, tenancy, remaining, Spread)
		if !ok {
			return cpuset.New(), fmt.Errorf("failed to take %d CPUs: %d still needed", numCPUs, remaining)
		}
		cores := byCache[cacheID]
		for len(cores) > 0 && remaining > 0 {
			core := cores[0]
			cores = cores[1:]
			for _, cpuID := range core.List() {
				if remaining == 0 {
					break
				}
				result = result.Union(cpuset.New(cpuID))
				remaining--
			}
		}
		if len(cores) == 0 {
			delete(byCache, cacheID)
		} else {
			byCache[cacheID] = cores
		}
	}
	return result, nil
}
