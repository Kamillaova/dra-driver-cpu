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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

// cacheDevice names the device published for one cache of smtPoolInfos: caches
// 0 and 1 are on NUMA node 0, caches 2 and 3 on NUMA node 1.
func cacheDevice(cacheID int) string {
	return fmt.Sprintf("%s%03d", devattr.CPUDeviceCacheGroupedPrefix, cacheID)
}

func publishedDeviceNames(devices []resourceapi.Device) []string {
	names := make([]string, 0, len(devices))
	for _, dev := range devices {
		names = append(names, dev.Name)
	}
	return names
}

func newCacheGroupedDriver(t *testing.T, policy coreselect.Policy) (*CPUDriver, *mockKubeletPlugin) {
	t.Helper()
	infos := smtPoolInfos()
	cp, err := New(testr.New(t), Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:   testSysFS(infos),
	}, &Config{
		DriverName:             testDriverName,
		NodeName:               testNodeName,
		KubeletRootDir:         "/var/lib/kubelet",
		CPUDeviceMode:          devattr.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:       devattr.GROUP_BY_UNCORE_CACHE,
		CachePlacementStrategy: policy,
	})
	require.NoError(t, err)
	plugin := &mockKubeletPlugin{}
	cp.draPlugin = plugin
	cp.cdiMgr = newMockCdiMgr()
	return cp, plugin
}

// TestPublishedCacheDeviceOrder: the allocator takes the first device that
// fits, so the published order is where the placement strategy acts on a new
// claim. Whichever way it orders the caches of a NUMA node, that node's devices
// stay one run, or the allocator's backtracking on a NUMA constraint is
// unbounded.
func TestPublishedCacheDeviceOrder(t *testing.T) {
	logger := testr.New(t)
	for _, tc := range []struct {
		name   string
		policy coreselect.Policy
		want   []string
	}{
		{
			name:   "pack offers the cache that already holds a claim first",
			policy: coreselect.Pack,
			want:   []string{cacheDevice(0), cacheDevice(1), cacheDevice(3), cacheDevice(2)},
		},
		{
			name:   "spread offers the caches holding nothing first",
			policy: coreselect.Spread,
			want:   []string{cacheDevice(1), cacheDevice(0), cacheDevice(2), cacheDevice(3)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp, plugin := newCacheGroupedDriver(t, tc.policy)
			// A claim on cache 0 of NUMA node 0 and one on cache 3 of NUMA node 1,
			// so that each node has one tenanted cache and one clean one.
			require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-numa0",
				store.ClaimRecord{Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 8), Role: store.RoleExclusive}}}, false))
			require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-numa1",
				store.ClaimRecord{Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(6, 14), Role: store.RoleExclusive}}}, false))

			cp.PublishResources(context.Background())

			require.NotNil(t, plugin.publishedResources)
			pool, ok := plugin.publishedResources.Pools[testNodeName]
			require.True(t, ok)
			require.Len(t, pool.Slices, 1)
			require.Equal(t, tc.want, publishedDeviceNames(pool.Slices[0].Devices))
		})
	}
}

// TestPublishedCacheDeviceOrderKeepsNUMANodesContiguous: spread reverses the
// caches of a node, and must still not interleave the two nodes.
func TestPublishedCacheDeviceOrderKeepsNUMANodesContiguous(t *testing.T) {
	logger := testr.New(t)
	cp, plugin := newCacheGroupedDriver(t, coreselect.Spread)
	// One claim per NUMA node, each on that node's first cache, so spread has to
	// move something inside both nodes.
	require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-numa0",
		store.ClaimRecord{Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0), Role: store.RoleExclusive}}}, false))
	require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-numa1",
		store.ClaimRecord{Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(4), Role: store.RoleExclusive}}}, false))

	cp.PublishResources(context.Background())

	pool := plugin.publishedResources.Pools[testNodeName]
	require.Len(t, pool.Slices, 1)
	require.Equal(t, []string{cacheDevice(1), cacheDevice(0), cacheDevice(3), cacheDevice(2)},
		publishedDeviceNames(pool.Slices[0].Devices))

	var numaNodes []int
	for _, dev := range pool.Slices[0].Devices {
		numaNodes = append(numaNodes, cp.topology.deviceNameToNUMANodeID[dev.Name])
	}
	require.Equal(t, []int{0, 0, 1, 1}, numaNodes)
}

// TestCacheDeviceOrderRepublishedOnAnEmptinessChange: the order is a function
// of which caches hold a claim, so it goes out again exactly when one of them
// changes -- not on every claim, which would republish the same order.
func TestCacheDeviceOrderRepublishedOnAnEmptinessChange(t *testing.T) {
	ctx := context.Background()
	cp, plugin := newCacheGroupedDriver(t, coreselect.Pack)

	cp.PublishResources(ctx)
	require.EqualValues(t, 1, plugin.publishCalls.Load())

	firstOnCache1 := testClaim("claim-1", testDriverName, testNodeName, map[string]int64{cacheDevice(1): 2})
	results, err := cp.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{firstOnCache1})
	require.NoError(t, err)
	require.NoError(t, results["claim-1"].Err)
	require.Eventually(t, func() bool { return plugin.publishCalls.Load() == 2 }, time.Second, 5*time.Millisecond,
		"cache 1 went from holding nothing to holding a claim, so the order it is published in changed")
	require.Equal(t, []string{cacheDevice(1), cacheDevice(0), cacheDevice(2), cacheDevice(3)},
		publishedDeviceNames(plugin.publishedResources.Pools[testNodeName].Slices[0].Devices))

	secondOnCache1 := testClaim("claim-2", testDriverName, testNodeName, map[string]int64{cacheDevice(1): 2})
	results, err = cp.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{secondOnCache1})
	require.NoError(t, err)
	require.NoError(t, results["claim-2"].Err)
	require.Never(t, func() bool { return plugin.publishCalls.Load() != 2 }, 50*time.Millisecond, 5*time.Millisecond,
		"the second claim landed where the first already is, so nothing about the order changed")

	_, err = cp.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{
		{UID: "claim-1"}, {UID: "claim-2"},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return plugin.publishCalls.Load() == 3 }, time.Second, 5*time.Millisecond,
		"the last claim left cache 1, so it is a clean cache again")
	require.Equal(t, []string{cacheDevice(0), cacheDevice(1), cacheDevice(2), cacheDevice(3)},
		publishedDeviceNames(plugin.publishedResources.Pools[testNodeName].Slices[0].Devices))
}

// TestNUMANodeThreadsPerCoreUnderCacheGrouping: the defragmentation planner and
// the /placements dry run ask this map for a NUMA node's whole-core step, and
// under cache grouping several devices share a node. The node has a step only
// where they agree on one.
func TestNUMANodeThreadsPerCoreUnderCacheGrouping(t *testing.T) {
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: smtPoolInfos()}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)

	// Caches 0 and 1 are NUMA node 0's, caches 2 and 3 NUMA node 1's.
	nameToID := map[string]int{
		cacheDevice(0): 0, cacheDevice(1): 0,
		cacheDevice(2): 1, cacheDevice(3): 1,
	}

	agreeing := numaNodeThreadsPerCore(topo, devattr.GROUP_BY_UNCORE_CACHE, nameToID, map[string]int{
		cacheDevice(0): 2, cacheDevice(1): 2,
		cacheDevice(2): 2, cacheDevice(3): 2,
	})
	require.Equal(t, map[int]int{0: 2, 1: 2}, agreeing)

	// A partition whose siblings are offline leaves one cache of NUMA node 0 at
	// one thread per core, so that node promises nothing.
	disagreeing := numaNodeThreadsPerCore(topo, devattr.GROUP_BY_UNCORE_CACHE, nameToID, map[string]int{
		cacheDevice(0): 2, cacheDevice(1): 1,
		cacheDevice(2): 2, cacheDevice(3): 2,
	})
	require.Equal(t, map[int]int{0: 0, 1: 2}, disagreeing,
		"a node whose devices disagree has no single step, whatever order the map is walked in")
}
