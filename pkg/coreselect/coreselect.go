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

// TakeWholeCores returns exactly numCPUs CPUs drawn from available as whole
// physical cores, so no core is ever split between this result and anything else.
//
// Cores are chosen to span as few uncore caches as possible: the remainder is
// satisfied from the smallest cache that can still hold all of it, which fills
// partly-used caches before opening clean ones and leaves whole caches free for
// larger claims. When no single cache can hold the remainder the largest cache is
// used, since spanning fewer caches beats spreading evenly.
//
// numCPUs must be a multiple of the core size; available is filtered to complete
// cores first, so a core the caller has partly consumed elsewhere is not offered.
func TakeWholeCores(topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
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

	result := cpuset.New()
	remaining := numCPUs
	for remaining > 0 {
		cacheID, ok := pickCache(coresByCache, remaining)
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

// pickCache returns the cache to draw the next cores from: the smallest one that
// can still hold all of remaining, falling back to the largest when none can.
// Ties break on the lower cache id so the choice is deterministic.
func pickCache(coresByCache map[int][]cpuset.CPUSet, remaining int) (int, bool) {
	bestFit, bestFitFree := 0, 0
	largest, largestFree := 0, 0
	found, fits := false, false

	for cacheID, cores := range coresByCache {
		free := 0
		for _, core := range cores {
			free += core.Size()
		}
		if free == 0 {
			continue
		}
		if !found || free > largestFree || (free == largestFree && cacheID < largest) {
			largest, largestFree, found = cacheID, free, true
		}
		if free >= remaining && (!fits || free < bestFitFree || (free == bestFitFree && cacheID < bestFit)) {
			bestFit, bestFitFree, fits = cacheID, free, true
		}
	}
	if fits {
		return bestFit, true
	}
	return largest, found
}
