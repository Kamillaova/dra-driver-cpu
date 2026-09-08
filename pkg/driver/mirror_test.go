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
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// newMirrorDriver is a driver over smtPoolInfos: 16 CPUs, eight two-thread
// cores, two NUMA nodes, four uncore caches of two cores each. Whole cores are
// what gives a cache device a request policy, and with it a capacity floor.
func newMirrorDriver(t *testing.T, groupBy string, wholeCores bool) (*CPUDriver, *mockKubeletPlugin, *prometheus.Registry) {
	t.Helper()
	infos := smtPoolInfos()
	reg := prometheus.NewRegistry()
	cp, err := New(testr.New(t), Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:   testSysFS(infos),
	}, &Config{
		DriverName:           testDriverName,
		NodeName:             testNodeName,
		KubeletRootDir:       "/var/lib/kubelet",
		CPUDeviceMode:        devattr.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:     groupBy,
		FullPhysicalCPUsOnly: wholeCores,
		Metrics:              cpumetrics.New(reg),
	})
	require.NoError(t, err)
	plugin := &mockKubeletPlugin{}
	cp.draPlugin = plugin
	cp.cdiMgr = newMockCdiMgr()
	return cp, plugin, reg
}

// publishedCapacity is the CPU capacity of every device the driver last handed
// to the publishing controller.
func publishedCapacity(t *testing.T, plugin *mockKubeletPlugin) map[string]int64 {
	t.Helper()
	require.NotNil(t, plugin.publishedResources)
	pool, ok := plugin.publishedResources.Pools[testNodeName]
	require.True(t, ok)
	capacities := map[string]int64{}
	for _, slice := range pool.Slices {
		for _, dev := range slice.Devices {
			capacity, ok := dev.Capacity[devattr.CPUResourceQualifiedName]
			require.True(t, ok, "device %q publishes no CPU capacity", dev.Name)
			capacities[dev.Name] = capacity.Value.Value()
		}
	}
	return capacities
}

// publishedTaintKeys is the taint keys of one published device.
func publishedTaintKeys(t *testing.T, plugin *mockKubeletPlugin, deviceName string) []string {
	t.Helper()
	require.NotNil(t, plugin.publishedResources)
	for _, slice := range plugin.publishedResources.Pools[testNodeName].Slices {
		for _, dev := range slice.Devices {
			if dev.Name != deviceName {
				continue
			}
			keys := make([]string, 0, len(dev.Taints))
			for _, taint := range dev.Taints {
				keys = append(keys, taint.Key)
			}
			return keys
		}
	}
	t.Fatalf("device %q was not published", deviceName)
	return nil
}

// placeCharged records a prepared claim charged to one device and running on
// cpus, which need not be that device's.
func placeCharged(t *testing.T, cp *CPUDriver, claimUID types.UID, charged string, cpus cpuset.CPUSet) {
	t.Helper()
	record := relocatableOn(cpus)
	record.Recorded = map[string]int{charged: cpus.Size()}
	require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(testr.New(t), claimUID, record, false))
}

// TestCapacityMirrorPublishesTheFreeCPUsExactly walks the whole point of the
// mirror: a scheduler subtracting what its own records charged from what the
// driver publishes has to arrive at the CPUs really free on that device, and a
// claim's allocation cannot be moved with the claim.
func TestCapacityMirrorPublishesTheFreeCPUsExactly(t *testing.T) {
	logger := testr.New(t)
	cp, plugin, _ := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, false)
	cache0, cache1 := cacheDevice(0), cacheDevice(1)
	origin, target := cpuset.New(0, 8), cpuset.New(2, 10)

	// Charged two CPUs on cache 0 and running there, which is where it landed.
	placeCharged(t, cp, "claim-a", cache0, origin)
	cp.PublishResources(context.Background())
	require.Equal(t, int64(4), publishedCapacity(t, plugin)[cache0],
		"a claim on the cache its allocation charged needs no correction: 4 published, 2 consumed, 2 free")

	require.NoError(t, cp.cpuAllocationStore.BeginRebind(logger, "claim-a", target))
	require.NoError(t, cp.cpuAllocationStore.CommitRebind(logger, "claim-a"))
	cp.PublishResources(context.Background())

	capacities := publishedCapacity(t, plugin)
	// Cache 0 is empty but still charged 2, so it publishes 6: the scheduler
	// subtracts its 2 and sees the 4 CPUs that are really free.
	require.Equal(t, int64(6), capacities[cache0])
	// Cache 1 holds 2 CPUs nobody charged it for, so it publishes 2: the
	// scheduler subtracts nothing and sees the 2 CPUs that are really free.
	require.Equal(t, int64(2), capacities[cache1])
	require.Equal(t, int64(4), capacities[cacheDevice(2)], "a cache with no tenant is published as it is")
}

// TestCapacityMirrorShrinksTheDestinationBeforeTheMove: the round publishes
// before it touches a container, so the destination has to be short of the CPUs
// the claim is about to take while the origin is still counted as holding it.
// Both halves are pessimistic, which is the only safe direction while the driver
// cannot say which of the two the container is on.
func TestCapacityMirrorShrinksTheDestinationBeforeTheMove(t *testing.T) {
	logger := testr.New(t)
	cp, plugin, _ := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, false)
	cache0, cache1 := cacheDevice(0), cacheDevice(1)

	placeCharged(t, cp, "claim-a", cache0, cpuset.New(0, 8))
	require.NoError(t, cp.cpuAllocationStore.BeginRebind(logger, "claim-a", cpuset.New(2, 10)))
	cp.PublishResources(context.Background())

	capacities := publishedCapacity(t, plugin)
	require.Equal(t, int64(4), capacities[cache0], "no departure is credited until the runtime confirms")
	require.Equal(t, int64(2), capacities[cache1], "the destination is short of the CPUs the move is about to take")
}

// TestCapacityMirrorShrinksBothCachesOfAnExchange: an exchange takes no free
// CPUs, so what it costs is the room each side no longer has for anyone else
// until it is settled.
func TestCapacityMirrorShrinksBothCachesOfAnExchange(t *testing.T) {
	logger := testr.New(t)
	cp, plugin, _ := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, false)
	cache0, cache1 := cacheDevice(0), cacheDevice(1)
	inCache0, inCache1 := cpuset.New(0, 8), cpuset.New(2, 10)

	placeCharged(t, cp, "claim-a", cache0, inCache0)
	placeCharged(t, cp, "claim-b", cache1, inCache1)
	require.NoError(t, cp.cpuAllocationStore.BeginSwap(logger, map[types.UID]cpuset.CPUSet{
		"claim-a": inCache1,
		"claim-b": inCache0,
	}))
	cp.PublishResources(context.Background())

	capacities := publishedCapacity(t, plugin)
	require.Equal(t, int64(2), capacities[cache0])
	require.Equal(t, int64(2), capacities[cache1])
}

// TestCapacityMirrorLeavesAnUnknownChargeUncorrected: a claim restored from a
// record written before the driver kept the charged devices. Read as "charged
// nothing" it would count as a squatter on the cache it sits in, while the
// scheduler still subtracts it at the cache it really was charged to, so a
// driver upgrade would withdraw the node's whole allocated capacity.
func TestCapacityMirrorLeavesAnUnknownChargeUncorrected(t *testing.T) {
	cp, plugin, _ := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, false)

	require.NoError(t, cp.cpuAllocationStore.ReserveResourceClaimAllocation(testr.New(t), "claim-legacy",
		relocatableOn(cpuset.New(0, 8)), false))
	cp.PublishResources(context.Background())

	require.Equal(t, int64(4), publishedCapacity(t, plugin)[cacheDevice(0)])
}

// TestCapacityMirrorHoldsAtTheFloorAndTaints: a shareable device's range is
// re-validated against a changed value and an amount below min + step
// invalidates the whole ResourceSlice, which would leave every device of the
// node stale while the controller retried it. The device is published at the
// floor and tainted instead, so the capacity the number over-states is
// withdrawn rather than handed out.
func TestCapacityMirrorHoldsAtTheFloorAndTaints(t *testing.T) {
	cp, plugin, reg := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, true)
	cache0 := cacheDevice(0)

	// Charged on cache 1, sitting on cache 0: three of cache 0's four CPUs are
	// occupied by claims it was never charged for, so the computed capacity is 1
	// and the floor -- min 2 plus a step of 2 -- is 4.
	placeCharged(t, cp, "claim-a", cacheDevice(1), cpuset.New(0, 8))
	placeCharged(t, cp, "claim-b", cacheDevice(1), cpuset.New(1))
	cp.PublishResources(context.Background())

	capacities := publishedCapacity(t, plugin)
	require.Equal(t, int64(7), capacities[cacheDevice(1)], "cache 1 is charged for three CPUs nothing occupies")
	require.Equal(t, int64(4), capacities[cache0], "one computed, four published: the floor, not the size")
	require.Equal(t, []string{devattr.FloorTaintKey}, publishedTaintKeys(t, plugin, cache0))
	require.Empty(t, publishedTaintKeys(t, plugin, cacheDevice(1)), "a device published as computed carries no taint")
	require.InDelta(t, 1, metricValue(t, reg, "dra_cpu_capacity_mirror_floored_devices", nil), 0.01)
}

// TestCapacityMirrorLeavesAPoolAlone: a pool's capacity bounds how much work
// lands on CPUs every claim asking for it holds at once, so the amount charged
// there is not a count of CPUs one claim occupies and their difference would
// mean nothing.
func TestCapacityMirrorLeavesAPoolAlone(t *testing.T) {
	cp := &CPUDriver{topology: deviceTopology{
		deviceNameToCPUs: map[string]cpuset.CPUSet{
			"cpudevcache000": cpuset.New(0, 1),
			"cpudevpool000":  cpuset.New(2, 3),
		},
		deviceNameToRole: map[string]string{
			"cpudevcache000": devattr.PARTITION_ROLE_DEFAULT,
			"cpudevpool000":  devattr.PARTITION_ROLE_SHARED,
		},
	}}

	mirrored := cp.mirroredDevices()
	require.Contains(t, mirrored, "cpudevcache000")
	require.NotContains(t, mirrored, "cpudevpool000")
}

// TestPublishedSlicesAreStaleWhenACorrectionChanges: the published capacities
// are the third input the slices carry, and a claim leaves the device its
// allocation charged under every grouping a claim can be moved under, not only
// the one where a device is a cache.
func TestPublishedSlicesAreStaleWhenACorrectionChanges(t *testing.T) {
	cp, _, _ := newMirrorDriver(t, devattr.GROUP_BY_NUMA_NODE, false)
	numa0, numa1 := devattr.CPUDeviceNUMAGroupedPrefix+"000", devattr.CPUDeviceNUMAGroupedPrefix+"001"
	require.Contains(t, cp.topology.deviceNameToCPUs, numa0)
	require.Contains(t, cp.topology.deviceNameToCPUs, numa1)

	cp.PublishResources(context.Background())
	require.False(t, cp.publishedSlicesAreStale())

	// A claim charged to one NUMA node's device and running on the other's, which
	// is what a partition list edited under a running node leaves behind.
	placeCharged(t, cp, "claim-drifted", numa0, cpuset.New(4, 12))
	require.True(t, cp.publishedSlicesAreStale())

	cp.PublishResources(context.Background())
	require.False(t, cp.publishedSlicesAreStale())
}

// TestCapacityFloorFollowsTheRequestPolicy: both comparisons the API server
// makes against a changed value are against min and against min + step, so the
// floor is their maximum, and a capacity with no range has neither.
func TestCapacityFloorFollowsTheRequestPolicy(t *testing.T) {
	require.Equal(t, int64(0), capacityFloor(resourceapi.DeviceCapacity{}))

	withPolicy := func(min int64, steps ...int64) resourceapi.DeviceCapacity {
		policy := &resourceapi.CapacityRequestPolicy{
			ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: resource.NewQuantity(min, resource.DecimalSI)},
		}
		for _, step := range steps {
			policy.ValidRange.Step = resource.NewQuantity(step, resource.DecimalSI)
		}
		return resourceapi.DeviceCapacity{RequestPolicy: policy}
	}
	require.Equal(t, int64(2), capacityFloor(withPolicy(2)))
	require.Equal(t, int64(4), capacityFloor(withPolicy(2, 2)))
	require.Equal(t, int64(8), capacityFloor(withPolicy(4, 4)))
}
