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

package defrag_test

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// topologyOf builds a machine from a per-NUMA-node list of cache sizes:
// {{4, 4}, {4, 4}} is two nodes of two 4-CPU caches each. CPU and cache IDs are
// numbered consecutively across the machine, and every CPU is its own core.
func topologyOf(nodes ...[]int) *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	cpuID, cacheID := 0, 0
	for numaNodeID, caches := range nodes {
		for _, size := range caches {
			for range size {
				details[cpuID] = cpuinfo.CPUInfo{
					CpuID:         cpuID,
					CoreID:        cpuID,
					SocketID:      0,
					NUMANodeID:    numaNodeID,
					UncoreCacheID: cacheID,
				}
				cpuID++
			}
			cacheID++
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: cpuID, NumCores: cpuID, NumSockets: 1,
		NumNUMANodes: len(nodes), NumUncoreCache: cacheID, CPUDetails: details,
	}
}

// allBut returns every CPU of topo except the given ones.
func allBut(topo *cpuinfo.CPUTopology, excluded ...int) cpuset.CPUSet {
	return topo.CPUDetails.CPUs().Difference(cpuset.New(excluded...))
}

func requireTopology(t *testing.T, topo *cpuinfo.CPUTopology, numaNodeID int, allocatable cpuset.CPUSet) *defrag.Topology {
	t.Helper()
	got, err := defrag.NewTopology(topo, numaNodeID, allocatable)
	require.NoError(t, err)
	return got
}

func TestNewTopologyRejectsUndefragmentableNodes(t *testing.T) {
	noCacheInfo := topologyOf([]int{4})
	info := noCacheInfo.CPUDetails[2]
	info.UncoreCacheID = -1
	noCacheInfo.CPUDetails[2] = info

	testCases := []struct {
		name          string
		topo          *cpuinfo.CPUTopology
		numaNodeID    int
		allocatable   cpuset.CPUSet
		expectedError string
	}{
		{
			name:          "node does not exist",
			topo:          topologyOf([]int{4, 4}),
			numaNodeID:    1,
			allocatable:   cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			expectedError: "NUMA node 1 has no allocatable CPUs",
		},
		{
			name:          "every CPU of the node is reserved",
			topo:          topologyOf([]int{4}, []int{4}),
			numaNodeID:    1,
			allocatable:   cpuset.New(0, 1, 2, 3),
			expectedError: "NUMA node 1 has no allocatable CPUs",
		},
		{
			// Without cache IDs there is no spread to measure, so the node must
			// be skipped rather than treated as a single cache.
			name:          "a CPU reports no uncore cache",
			topo:          noCacheInfo,
			numaNodeID:    0,
			allocatable:   cpuset.New(0, 1, 2, 3),
			expectedError: "NUMA node 0 reports no uncore cache for CPU 2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := defrag.NewTopology(tc.topo, tc.numaNodeID, tc.allocatable)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
			require.Nil(t, got)
		})
	}
}

func TestTopologyGeometryExcludesUnallocatableCPUs(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4})
	// CPUs 0 and 1 are reserved, so cache 0 is only two CPUs wide here.
	got := requireTopology(t, topo, 0, allBut(topo, 0, 1))

	require.Equal(t, 0, got.NUMANodeID())
	require.Equal(t, []int{0, 1, 2, 3}, got.Caches())
	require.Equal(t, cpuset.New(2, 3), got.CPUsInCache(0))
	require.Equal(t, cpuset.New(4, 5, 6, 7), got.CPUsInCache(1))
	require.Equal(t, cpuset.New(12, 13, 14, 15), got.CPUsInCache(3))
	require.True(t, got.CPUsInCache(99).IsEmpty())

	cacheID, ok := got.CacheOf(2)
	require.True(t, ok)
	require.Equal(t, 0, cacheID)
	cacheID, ok = got.CacheOf(15)
	require.True(t, ok)
	require.Equal(t, 3, cacheID)

	_, ok = got.CacheOf(0)
	require.False(t, ok, "a reserved CPU is not allocatable in this node")
	_, ok = got.CacheOf(99)
	require.False(t, ok)
}

func TestMinSpread(t *testing.T) {
	testCases := []struct {
		name        string
		topo        *cpuinfo.CPUTopology
		reserved    []int
		expected    map[int]int
		description string
	}{
		{
			name:     "four equal caches",
			topo:     topologyOf([]int{4, 4, 4, 4}),
			expected: map[int]int{0: 0, 1: 1, 4: 1, 5: 2, 8: 2, 9: 3, 16: 4, 17: 4},
		},
		{
			name:     "unequal caches are consumed largest first",
			topo:     topologyOf([]int{8, 4, 2}),
			expected: map[int]int{1: 1, 8: 1, 9: 2, 12: 2, 13: 3, 14: 3, 15: 3},
		},
		{
			name:     "one cache",
			topo:     topologyOf([]int{8}),
			expected: map[int]int{0: 0, 1: 1, 8: 1, 9: 1},
		},
		{
			// Reserving CPUs narrows the cache that holds them, so the minimum
			// counts what is actually placeable there.
			name:     "reserved CPUs narrow their cache",
			topo:     topologyOf([]int{4, 4, 4, 4}),
			reserved: []int{0, 1},
			expected: map[int]int{4: 1, 5: 2, 12: 3, 13: 4, 14: 4, 15: 4},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := requireTopology(t, tc.topo, 0, allBut(tc.topo, tc.reserved...))
			for numCPUs, expected := range tc.expected {
				require.Equal(t, expected, got.MinSpread(numCPUs), "MinSpread(%d)", numCPUs)
			}
			require.Equal(t, 0, got.MinSpread(-1))
		})
	}
}

func TestSpreadAndExcessSpread(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4})
	got := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	testCases := []struct {
		name            string
		cpus            cpuset.CPUSet
		expectedSpread  int
		expectedExcess  int
		expectedMinimum int
	}{
		{
			name: "empty", cpus: cpuset.New(),
			expectedSpread: 0, expectedMinimum: 0, expectedExcess: 0,
		},
		{
			name: "a whole cache", cpus: cpuset.New(0, 1, 2, 3),
			expectedSpread: 1, expectedMinimum: 1, expectedExcess: 0,
		},
		{
			name: "part of one cache", cpus: cpuset.New(1, 2),
			expectedSpread: 1, expectedMinimum: 1, expectedExcess: 0,
		},
		{
			name: "split across two caches when one would do", cpus: cpuset.New(0, 1, 4, 5),
			expectedSpread: 2, expectedMinimum: 1, expectedExcess: 1,
		},
		{
			name: "one CPU per cache", cpus: cpuset.New(0, 4, 8, 12),
			expectedSpread: 4, expectedMinimum: 1, expectedExcess: 3,
		},
		{
			// Two caches is the best a claim this size can do, so it is not
			// misplaced despite spanning two.
			name: "two whole caches", cpus: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			expectedSpread: 2, expectedMinimum: 2, expectedExcess: 0,
		},
		{
			name: "eight CPUs over four caches", cpus: cpuset.New(0, 1, 4, 5, 8, 9, 12, 13),
			expectedSpread: 4, expectedMinimum: 2, expectedExcess: 2,
		},
		{
			name: "CPUs outside the node are not counted", cpus: cpuset.New(0, 1, 99),
			expectedSpread: 1, expectedMinimum: 1, expectedExcess: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedSpread, got.Spread(tc.cpus))
			require.Equal(t, tc.expectedMinimum, got.MinSpread(tc.cpus.Size()))
			require.Equal(t, tc.expectedExcess, got.ExcessSpread(tc.cpus))
		})
	}
}

func TestExcessSpreadReachesZeroWhenEveryCacheHoldsAReservedCPU(t *testing.T) {
	// Measured by their physical size both caches could hold all eight CPUs, so
	// the minimum would read as one cache and this claim would look permanently
	// misplaced -- and defrag would keep trying to reach a placement that does
	// not exist. Measured by what is placeable, two caches is the minimum and the
	// claim is already there.
	topo := topologyOf([]int{8, 8})
	got := requireTopology(t, topo, 0, allBut(topo, 0, 8))

	claim := cpuset.New(1, 2, 3, 4, 5, 6, 7, 9)
	require.Equal(t, 2, got.Spread(claim))
	require.Equal(t, 2, got.MinSpread(8))
	require.Equal(t, 0, got.ExcessSpread(claim))
	require.Equal(t, 0, got.Cost([]defrag.Placement{{ClaimUID: "claim-1", CPUs: claim}}))
}

func TestCost(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4})
	got := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	require.Equal(t, 0, got.Cost(nil))
	require.Equal(t, 3, got.Cost([]defrag.Placement{
		{ClaimUID: "aligned", CPUs: cpuset.New(0, 1)},
		{ClaimUID: "split-two", CPUs: cpuset.New(2, 4)},
		{ClaimUID: "split-three", CPUs: cpuset.New(5, 8, 12)},
	}))
}

func TestCostIsZeroForClaimsAloneInEveryCache(t *testing.T) {
	// Each claim is individually as well placed as its size allows, so the cost
	// is zero even though no cache is free and no larger claim could ever be
	// satisfied here. Cost alone therefore cannot ask for consolidation: that is
	// why planning compares against an ideal packing rather than only descending
	// this gradient.
	topo := topologyOf([]int{4, 4, 4, 4})
	got := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	scattered := []defrag.Placement{
		{ClaimUID: "claim-1", CPUs: cpuset.New(0, 1)},
		{ClaimUID: "claim-2", CPUs: cpuset.New(4, 5)},
		{ClaimUID: "claim-3", CPUs: cpuset.New(8, 9)},
		{ClaimUID: "claim-4", CPUs: cpuset.New(12, 13)},
	}
	require.Equal(t, 0, got.Cost(scattered))
}

func TestPlacementsByNUMANode(t *testing.T) {
	topo := topologyOf([]int{4, 4}, []int{4, 4})

	// claim-b straddles both nodes; the CPU 99 it also names does not exist.
	allocations := map[types.UID]cpuset.CPUSet{
		"claim-c": cpuset.New(2, 3),
		"claim-a": cpuset.New(0, 1),
		"claim-b": cpuset.New(4, 8, 99),
		"claim-d": cpuset.New(12, 13),
	}

	// Repeated to catch any dependence on map iteration order.
	for range 8 {
		got := defrag.PlacementsByNUMANode(topo, allocations)
		require.Len(t, got, 2)
		require.Equal(t, []defrag.Placement{
			{ClaimUID: "claim-a", CPUs: cpuset.New(0, 1)},
			{ClaimUID: "claim-b", CPUs: cpuset.New(4)},
			{ClaimUID: "claim-c", CPUs: cpuset.New(2, 3)},
		}, got[0])
		require.Equal(t, []defrag.Placement{
			{ClaimUID: "claim-b", CPUs: cpuset.New(8)},
			{ClaimUID: "claim-d", CPUs: cpuset.New(12, 13)},
		}, got[1])
	}

	require.Empty(t, defrag.PlacementsByNUMANode(topo, nil))
}
