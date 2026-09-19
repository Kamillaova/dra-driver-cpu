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

package coreselect_test

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

// smtCaches returns caches x coresPerCache x 2 threads on one socket and NUMA
// node. Cores are numbered 0..n-1; thread 1 of core c is CPU c+n.
func smtCaches(caches, coresPerCache int) *cpuinfo.CPUTopology {
	cores := caches * coresPerCache
	details := cpuinfo.CPUDetails{}
	for cpu := range cores * 2 {
		core := cpu % cores
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        core,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: core / coresPerCache,
			SiblingCPUID:  (cpu + cores) % (cores * 2),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: cores * 2, NumCores: cores, NumSockets: 1, NumNUMANodes: 1,
		NumUncoreCache: caches, SMTEnabled: true, CPUDetails: details,
	}
}

// cachesTouched counts the distinct uncore caches a result spans.
func cachesTouched(topo *cpuinfo.CPUTopology, cpus cpuset.CPUSet) int {
	seen := map[int]struct{}{}
	for _, cpu := range cpus.List() {
		seen[topo.CPUDetails[cpu].UncoreCacheID] = struct{}{}
	}
	return len(seen)
}

// requireWholeCores fails unless every core the result touches is fully included.
func requireWholeCores(t *testing.T, topo *cpuinfo.CPUTopology, cpus cpuset.CPUSet) {
	t.Helper()
	require.Equal(t, cpus, topo.CPUDetails.CompleteCores(cpus),
		"result %s splits a physical core", cpus.String())
}

func TestTakeWholeCoresNeverSplitsACore(t *testing.T) {
	// 2 caches x 4 cores x 2 threads = 16 CPUs.
	topo := smtCaches(2, 4)
	available := topo.CPUDetails.CPUs()

	for _, numCPUs := range []int{2, 4, 6, 8, 10, 16} {
		got, err := coreselect.TakeWholeCores(topo, available, numCPUs)
		require.NoError(t, err, "numCPUs=%d", numCPUs)
		require.Equal(t, numCPUs, got.Size(), "numCPUs=%d returned %s", numCPUs, got.String())
		requireWholeCores(t, topo, got)
	}
}

func TestTakeWholeCoresMinimisesCachesSpanned(t *testing.T) {
	topo := smtCaches(2, 4) // each cache holds 8 CPUs
	available := topo.CPUDetails.CPUs()

	// Anything up to a whole cache must stay inside one cache.
	for _, numCPUs := range []int{2, 4, 6, 8} {
		got, err := coreselect.TakeWholeCores(topo, available, numCPUs)
		require.NoError(t, err)
		require.Equal(t, 1, cachesTouched(topo, got),
			"numCPUs=%d spanned %d caches: %s", numCPUs, cachesTouched(topo, got), got.String())
	}

	// Larger than one cache needs two, but no more than two.
	got, err := coreselect.TakeWholeCores(topo, available, 12)
	require.NoError(t, err)
	require.Equal(t, 2, cachesTouched(topo, got))
}

func TestTakeWholeCoresFillsPartlyUsedCacheFirst(t *testing.T) {
	topo := smtCaches(2, 4)
	// Cache 0 has one core taken already (CPUs 0 and 8), cache 1 is pristine.
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(0, 8))

	// A 4-CPU claim fits in either, and must take the already-used cache so the
	// clean one stays whole for a bigger claim.
	got, err := coreselect.TakeWholeCores(topo, available, 4)
	require.NoError(t, err)
	requireWholeCores(t, topo, got)
	require.Equal(t, 1, cachesTouched(topo, got))
	require.Equal(t, 0, topo.CPUDetails[got.List()[0]].UncoreCacheID,
		"expected the partly-used cache 0, got %s", got.String())

	// Cache 1 is therefore still able to satisfy a whole-cache claim.
	rest, err := coreselect.TakeWholeCores(topo, available.Difference(got), 8)
	require.NoError(t, err)
	require.Equal(t, 1, cachesTouched(topo, rest))
}

func TestTakeWholeCoresIgnoresHalfCores(t *testing.T) {
	topo := smtCaches(1, 4) // 8 CPUs, cores 0-3, siblings 4-7
	// CPU 4 is withheld, so core 0 is unpairable and must not be used at all.
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(4))

	got, err := coreselect.TakeWholeCores(topo, available, 6)
	require.NoError(t, err)
	requireWholeCores(t, topo, got)
	require.False(t, got.Contains(0), "CPU 0's sibling is unavailable, so it is unusable")

	// Only three whole cores remain, so seven CPUs cannot be satisfied even
	// though seven CPUs are nominally available.
	require.Equal(t, 7, available.Size())
	_, err = coreselect.TakeWholeCores(topo, available, 8)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not enough whole cores")
}

func TestTakeWholeCoresIsDeterministic(t *testing.T) {
	topo := smtCaches(4, 4)
	available := topo.CPUDetails.CPUs()

	first, err := coreselect.TakeWholeCores(topo, available, 10)
	require.NoError(t, err)
	for range 20 {
		again, err := coreselect.TakeWholeCores(topo, available, 10)
		require.NoError(t, err)
		require.Equal(t, first, again, "selection must not depend on map iteration order")
	}
}

func TestTakeWholeCoresEdgeCases(t *testing.T) {
	topo := smtCaches(1, 2)

	got, err := coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), 0)
	require.NoError(t, err)
	require.Equal(t, cpuset.New(), got)

	_, err = coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), -2)
	require.Error(t, err)

	_, err = coreselect.TakeWholeCores(topo, cpuset.New(), 2)
	require.Error(t, err)

	// An odd request cannot be met with two-thread cores.
	_, err = coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), 3)
	require.Error(t, err)
}

func TestTakeWholeCoresDistinguishesSockets(t *testing.T) {
	// Two sockets whose core_id numbering restarts, the case kubelet's
	// CPUsInCores conflates: socket 0 core 0 is {0,4}, socket 1 core 0 is {2,6}.
	details := cpuinfo.CPUDetails{
		0: {CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		1: {CpuID: 1, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		2: {CpuID: 2, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		3: {CpuID: 3, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		4: {CpuID: 4, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		5: {CpuID: 5, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		6: {CpuID: 6, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		7: {CpuID: 7, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
	}
	topo := &cpuinfo.CPUTopology{
		NumCPUs: 8, NumCores: 4, NumSockets: 2, NumNUMANodes: 2, NumUncoreCache: 2,
		SMTEnabled: true, CPUDetails: details,
	}

	// Restricted to socket 0, the result must never reach into socket 1's
	// same-numbered cores.
	socket0 := cpuset.New(0, 1, 4, 5)
	got, err := coreselect.TakeWholeCores(topo, socket0, 4)
	require.NoError(t, err)
	require.True(t, got.IsSubsetOf(socket0), "leaked outside the available set: %s", got.String())
	requireWholeCores(t, topo, got)
}

// TestTakeWholeCoresSpreadPrefersTheEmptiestCache: under Spread a claim that
// fits in one cache opens the cleanest cache instead of packing into a used
// one, so each small tenant gets a cache of its own while there is slack.
func TestTakeWholeCoresSpreadPrefersTheEmptiestCache(t *testing.T) {
	topo := smtCaches(2, 4)                                          // cache 0: CPUs 0-3,8-11; cache 1: CPUs 4-7,12-15
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(0, 8)) // one core of cache 0 taken

	got, err := coreselect.TakeWholeCoresPolicy(topo, available, 2, coreselect.Spread, cpuset.New())
	require.NoError(t, err)
	require.Equal(t, 1, cachesTouched(topo, got))
	require.True(t, got.IsSubsetOf(cpuset.New(4, 5, 6, 7, 12, 13, 14, 15)),
		"spread must open the untouched cache, got %s", got.String())

	packed, err := coreselect.TakeWholeCoresPolicy(topo, available, 2, coreselect.Pack, cpuset.New())
	require.NoError(t, err)
	require.True(t, packed.IsSubsetOf(cpuset.New(1, 2, 3, 9, 10, 11)),
		"pack must fill the partly used cache, got %s", packed.String())
}

// TestTakeWholeCoresSpreadScattersOneClaimPerCache: consecutive small takes land
// in distinct caches until every cache hosts one.
func TestTakeWholeCoresSpreadScattersOneClaimPerCache(t *testing.T) {
	topo := smtCaches(4, 4)
	available := topo.CPUDetails.CPUs()

	seen := map[int]bool{}
	for i := 0; i < 4; i++ {
		got, err := coreselect.TakeWholeCoresPolicy(topo, available, 2, coreselect.Spread, cpuset.New())
		require.NoError(t, err, "take %d", i)
		require.Equal(t, 1, cachesTouched(topo, got))
		cache := topo.CPUDetails[got.List()[0]].UncoreCacheID
		require.False(t, seen[cache], "take %d landed in an already used cache %d: %s", i, cache, got.String())
		seen[cache] = true
		available = available.Difference(got)
	}
}

// TestTakeWholeCoresSpreadStillMinimisesSpan: a claim no cache can hold takes
// the largest caches first under either policy; spread never trades an extra
// cache for emptiness.
func TestTakeWholeCoresSpreadStillMinimisesSpan(t *testing.T) {
	topo := smtCaches(3, 4)                                           // cache 0: CPUs 0-3,12-15; cache 1: 4-7,16-19; cache 2: 8-11,20-23
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(0, 12)) // cache 0 down to 6

	got, err := coreselect.TakeWholeCoresPolicy(topo, available, 12, coreselect.Spread, cpuset.New())
	require.NoError(t, err)
	require.Equal(t, 2, cachesTouched(topo, got), "12 CPUs must span exactly two caches, got %s", got.String())
	require.True(t, got.Intersection(cpuset.New(1, 2, 3, 13, 14, 15)).IsEmpty(),
		"the two full caches must be used before the drained one, got %s", got.String())
}

// TestTakeWholeCoresSpreadRanksByTenancyNotFreeSpace: a
// cache diminished by the configured carve-outs hosts nothing, yet by raw free
// space it loses the tie to every cache already hosting a claim, and the last
// small tenant ends up sharing while an untouched cache sits there. Spread
// ranks by tenants, with the carve-outs declared so they do not count as one.
func TestTakeWholeCoresSpreadRanksByTenancyNotFreeSpace(t *testing.T) {
	topo := smtCaches(3, 4)                                  // cache 0: CPUs 0-3,12-15; cache 1: 4-7,16-19; cache 2: 8-11,20-23
	carved := cpuset.New(0, 1, 12, 13)                       // two cores of cache 0 carved by config
	tenant1, tenant2 := cpuset.New(4, 16), cpuset.New(8, 20) // one claim in each other cache
	available := topo.CPUDetails.CPUs().Difference(carved).Difference(tenant1).Difference(tenant2)

	got, err := coreselect.TakeWholeCoresPolicy(topo, available, 2, coreselect.Spread, carved)
	require.NoError(t, err)
	cache := topo.CPUDetails[got.List()[0]].UncoreCacheID
	require.Equal(t, 0, cache,
		"the carved-but-untenanted cache 0 must beat the roomier caches that already host a claim, got %s", got.String())

	// Without the carve declared, those CPUs read as a tenant and cache 0
	// loses -- the misranking this test exists to keep dead.
	got, err = coreselect.TakeWholeCoresPolicy(topo, available, 2, coreselect.Spread, cpuset.New())
	require.NoError(t, err)
	require.NotEqual(t, 0, topo.CPUDetails[got.List()[0]].UncoreCacheID)
}

// TestTakeSpreadCPUsScattersAndAvoidsSplittingCores: the thread-granular
// spread. One-CPU claims land in distinct caches, and inside a cache a fully
// free core is drained before a partly used one is split.
func TestTakeSpreadCPUsScattersAndAvoidsSplittingCores(t *testing.T) {
	topo := smtCaches(3, 2) // cache 0: cores 0-1 (CPUs 0,1,6,7); cache 1: cores 2-3; cache 2: cores 4-5
	available := topo.CPUDetails.CPUs()

	seen := map[int]bool{}
	for i := 0; i < 3; i++ {
		got, err := coreselect.TakeSpreadCPUs(topo, available, 1, cpuset.New())
		require.NoError(t, err, "take %d", i)
		cache := topo.CPUDetails[got.List()[0]].UncoreCacheID
		require.False(t, seen[cache], "take %d landed in already used cache %d", i, cache)
		seen[cache] = true
		available = available.Difference(got)
	}

	// The fourth take shares a cache; it must open that cache's untouched core
	// rather than the taken core's idle sibling.
	got, err := coreselect.TakeSpreadCPUs(topo, available, 1, cpuset.New())
	require.NoError(t, err)
	cpu := got.List()[0]
	sibling := topo.CPUDetails.SiblingsOf(cpu)
	require.True(t, sibling.IsSubsetOf(available),
		"the take split a partly used core while a whole one was free: got %d", cpu)
}

// TestTakeSpreadCPUsRanksByTenancyAtThreadGranularity: the carve-outs do not
// count as tenants for the thread-granular path either.
func TestTakeSpreadCPUsRanksByTenancyAtThreadGranularity(t *testing.T) {
	topo := smtCaches(2, 2) // cache 0: CPUs 0,1,4,5; cache 1: CPUs 2,3,6,7
	carved := cpuset.New(0, 4)
	tenant := cpuset.New(2)
	available := topo.CPUDetails.CPUs().Difference(carved).Difference(tenant)

	got, err := coreselect.TakeSpreadCPUs(topo, available, 1, carved)
	require.NoError(t, err)
	require.Equal(t, 0, topo.CPUDetails[got.List()[0]].UncoreCacheID,
		"the carved-but-untenanted cache must win, got %s", got.String())
}
