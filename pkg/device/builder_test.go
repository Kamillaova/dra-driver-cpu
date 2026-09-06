/*
Copyright The Kubernetes Authors.

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

package device_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/cpuset"
)

// fakeTopology returns a 4-CPU topology: CPUs 0,1 on socket0/NUMA0 and CPUs 2,3
// on socket1/NUMA1, SMT disabled.
func fakeTopology() *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for cpu := range 4 {
		socket := cpu / 2
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:          cpu,
			CoreID:         cpu,
			SocketID:       socket,
			NUMANodeID:     socket,
			NumaNodeCPUSet: cpuset.New(socket*2, socket*2+1),
			SiblingCPUID:   -1,
			SiblingCPUSet:  cpuset.New(cpu),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: 4, NumCores: 4, NumSockets: 2, NumNUMANodes: 2,
		SMTEnabled: false, CPUDetails: details,
	}
}

func TestDeviceBuilderNodeAllocatableResourceMapping(t *testing.T) {
	topo := fakeTopology()
	online := cpuset.New(0, 1, 2, 3)
	reserved := cpuset.New(0)
	one := resource.MustParse("1")

	tests := []struct {
		name                          string
		cpuDeviceMode                 string
		groupBy                       string
		publishNodeAllocatableMapping bool
	}{
		{
			name:                          "grouped/enabled",
			cpuDeviceMode:                 device.CPU_DEVICE_MODE_GROUPED,
			groupBy:                       device.GROUP_BY_NUMA_NODE,
			publishNodeAllocatableMapping: true,
		},
		{
			name:                          "grouped/disabled",
			cpuDeviceMode:                 device.CPU_DEVICE_MODE_GROUPED,
			groupBy:                       device.GROUP_BY_NUMA_NODE,
			publishNodeAllocatableMapping: false,
		},
		{
			name:                          "individual/enabled",
			cpuDeviceMode:                 device.CPU_DEVICE_MODE_INDIVIDUAL,
			publishNodeAllocatableMapping: true,
		},
		{
			name:                          "individual/disabled",
			cpuDeviceMode:                 device.CPU_DEVICE_MODE_INDIVIDUAL,
			publishNodeAllocatableMapping: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var devices []resourceapi.Device
			if tc.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
				built := device.BuildGrouped(logr.Discard(), tc.groupBy, topo, online, reserved, store.NewPCIeRootMapper(), tc.publishNodeAllocatableMapping, false, unpartitioned(online))
				devices = onlyPartition(t, built.ByPartition)
			} else {
				devices, _ = device.Build(topo, reserved, store.NewPCIeRootMapper(), tc.publishNodeAllocatableMapping)
			}
			require.NotEmpty(t, devices)

			for _, dev := range devices {
				if !tc.publishNodeAllocatableMapping {
					require.Nil(t, dev.NodeAllocatableResources,
						"device %q must not expose nodeAllocatableResources when publishing is disabled", dev.Name)
					continue
				}

				require.Contains(t, dev.NodeAllocatableResources, v1.ResourceCPU,
					"device %q must expose a node allocatable mapping for cpu", dev.Name)
				nar := dev.NodeAllocatableResources[v1.ResourceCPU]
				require.NotNil(t, nar.Mapping, "device %q: mapping must be set", dev.Name)
				require.Nil(t, nar.Overhead, "device %q: overhead must not be set", dev.Name)

				if tc.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
					// Grouped devices expose consumable capacity: the mapping must reference
					// the dra.cpu/cpu capacity with a 1:1 multiplier. The capacityKey must
					// reference an existing capacity entry or the apiserver rejects the slice.
					require.NotNil(t, nar.Mapping.CapacityKey, "device %q: capacityKey must be set", dev.Name)
					require.Equal(t, resourceapi.QualifiedName(device.CPUResourceQualifiedName), *nar.Mapping.CapacityKey)
					require.NotNil(t, nar.Mapping.CapacityMultiplier, "device %q: capacityMultiplier must be set", dev.Name)
					require.Zero(t, nar.Mapping.CapacityMultiplier.Cmp(one), "device %q: capacityMultiplier must be 1", dev.Name)
					require.Nil(t, nar.Mapping.DeviceMultiplier,
						"device %q: deviceMultiplier is mutually exclusive with capacityKey", dev.Name)
					require.Contains(t, dev.Capacity, resourceapi.QualifiedName(device.CPUResourceQualifiedName),
						"device %q: capacityKey must reference a defined capacity", dev.Name)
				} else {
					// Individual devices are one CPU each: the mapping must use a
					// deviceMultiplier of 1.
					require.NotNil(t, nar.Mapping.DeviceMultiplier, "device %q: deviceMultiplier must be set", dev.Name)
					require.Zero(t, nar.Mapping.DeviceMultiplier.Cmp(one), "device %q: deviceMultiplier must be 1", dev.Name)
					require.Nil(t, nar.Mapping.CapacityKey,
						"device %q: capacityKey is mutually exclusive with deviceMultiplier", dev.Name)
					require.Nil(t, nar.Mapping.CapacityMultiplier,
						"device %q: capacityMultiplier is only valid with capacityKey", dev.Name)
				}
			}
		})
	}
}

// fakeCacheTopology returns an 8-CPU single-socket, single-NUMA topology with
// two uncore caches of unequal allocatable size once CPU 0 is reserved:
// cache 0 keeps CPUs 1-3, cache 1 keeps CPUs 4-7.
func fakeCacheTopology() *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for cpu := range 8 {
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        cpu,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: cpu / 4,
			SiblingCPUID:  -1,
			SiblingCPUSet: cpuset.New(cpu),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: 8, NumCores: 8, NumSockets: 1, NumNUMANodes: 1, NumUncoreCache: 2,
		SMTEnabled: false, CPUDetails: details,
	}
}

// unpartitioned is the one partition a node whose cores nobody described has:
// the implicit one, holding every allocatable CPU.
func unpartitioned(allocatable cpuset.CPUSet) []device.Partition {
	return device.WithImplicitDefault(nil, allocatable)
}

// onlyPartition unwraps the single group of devices such a node publishes.
func onlyPartition(t *testing.T, byPartition [][]resourceapi.Device) []resourceapi.Device {
	t.Helper()
	require.Len(t, byPartition, 1)
	return byPartition[0]
}

func TestGroupedUncoreCacheAttributes(t *testing.T) {
	topo := fakeCacheTopology()
	online := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(0), store.NewPCIeRootMapper(), false, false, unpartitioned(online.Difference(cpuset.New(0))))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)
	attrs := devices[0].Attributes

	// Reserving CPU 0 shrinks cache 0 to three allocatable CPUs, so the largest
	// single-cache claim the group can align is four, the whole of cache 1.
	require.Contains(t, attrs, device.AttributeLargestUncoreCacheCPUs)
	require.EqualValues(t, 4, *attrs[device.AttributeLargestUncoreCacheCPUs].IntValue)
	require.Contains(t, attrs, device.AttributeUncoreCachesInGroup)
	require.EqualValues(t, 2, *attrs[device.AttributeUncoreCachesInGroup].IntValue)
}

func TestGroupedUncoreCacheAttributesOmittedWhenUnknown(t *testing.T) {
	topo := fakeCacheTopology()
	// A single CPU with no cache information must suppress both attributes
	// rather than yield a count derived from the remaining CPUs.
	info := topo.CPUDetails[5]
	info.UncoreCacheID = -1
	topo.CPUDetails[5] = info

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo,
		cpuset.New(0, 1, 2, 3, 4, 5, 6, 7), cpuset.New(), store.NewPCIeRootMapper(), false, false,
		unpartitioned(cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)

	require.NotContains(t, devices[0].Attributes, device.AttributeLargestUncoreCacheCPUs)
	require.NotContains(t, devices[0].Attributes, device.AttributeUncoreCachesInGroup)
}

func TestIndividualDevicesHaveNoGroupCacheAttributes(t *testing.T) {
	devices, _ := device.Build(fakeCacheTopology(), cpuset.New(), store.NewPCIeRootMapper(), false)
	require.NotEmpty(t, devices)
	for _, dev := range devices {
		require.NotContains(t, dev.Attributes, device.AttributeLargestUncoreCacheCPUs,
			"device %q: group geometry is meaningless for a single CPU", dev.Name)
		require.NotContains(t, dev.Attributes, device.AttributeUncoreCachesInGroup, dev.Name)
	}
}

// fakeSMTCacheTopology returns 16 CPUs on one socket and one NUMA node: 8 cores
// of 2 threads across 2 uncore caches, i.e. the smallest shape with more than one
// cache per NUMA node. Cores are 0-7, thread 1 of core c is CPU c+8.
//
//	cache 0: cores 0-3 -> CPUs 0-3, 8-11
//	cache 1: cores 4-7 -> CPUs 4-7, 12-15
func fakeSMTCacheTopology() *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for cpu := range 16 {
		core := cpu % 8
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        core,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: core / 4,
			SiblingCPUID:  (cpu + 8) % 16,
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: 16, NumCores: 8, NumSockets: 1, NumNUMANodes: 1, NumUncoreCache: 2,
		SMTEnabled: true, CPUDetails: details,
	}
}

func TestGroupedFullPhysicalCPUsOnly(t *testing.T) {
	topo := fakeSMTCacheTopology()
	online := topo.CPUDetails.CPUs()

	buildOne := func(t *testing.T, reserved cpuset.CPUSet, fullPhysicalCPUsOnly bool) resourceapi.Device {
		t.Helper()
		built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
			reserved, store.NewPCIeRootMapper(), false, fullPhysicalCPUsOnly, unpartitioned(online.Difference(reserved)))
		devices := onlyPartition(t, built.ByPartition)
		require.Len(t, devices, 1)
		return devices[0]
	}

	cpuCapacity := func(dev resourceapi.Device) resourceapi.DeviceCapacity {
		c, ok := dev.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
		require.True(t, ok, "device must publish CPU capacity")
		return c
	}

	t.Run("publishes a whole-core request policy", func(t *testing.T) {
		capacity := cpuCapacity(buildOne(t, cpuset.New(), true))

		require.Equal(t, int64(16), capacity.Value.Value())
		require.NotNil(t, capacity.RequestPolicy)
		require.NotNil(t, capacity.RequestPolicy.ValidRange)
		require.EqualValues(t, 2, capacity.RequestPolicy.ValidRange.Min.Value(), "min is one whole core")
		require.EqualValues(t, 2, capacity.RequestPolicy.ValidRange.Step.Value(), "step is one whole core")

		// Default must stay the full capacity: a claim that omits a capacity
		// request consumes Default, and upstream gives it the whole device.
		require.NotNil(t, capacity.RequestPolicy.Default)
		require.EqualValues(t, 16, capacity.RequestPolicy.Default.Value())
	})

	t.Run("a core split by the reservation is dropped from capacity", func(t *testing.T) {
		// Reserving CPU 0 alone leaves its sibling CPU 8 unpairable, so both
		// leave the pool: 16 - 2 = 14, not 15.
		dev := buildOne(t, cpuset.New(0), true)
		capacity := cpuCapacity(dev)

		require.Equal(t, int64(14), capacity.Value.Value())
		require.EqualValues(t, 14, *dev.Attributes[device.AttributeNumCPUs].IntValue,
			"numCPUs must agree with the published capacity")
		require.EqualValues(t, 14, capacity.RequestPolicy.Default.Value())
		// Cache 0 lost a whole core and holds 6 allocatable CPUs, so cache 1 at
		// 8 becomes the largest and bounds a single-cache claim.
		require.EqualValues(t, 8, *dev.Attributes[device.AttributeLargestUncoreCacheCPUs].IntValue)
	})

	t.Run("disabled leaves capacity and policy untouched", func(t *testing.T) {
		dev := buildOne(t, cpuset.New(0), false)
		capacity := cpuCapacity(dev)

		require.Equal(t, int64(15), capacity.Value.Value(), "the odd thread stays claimable")
		require.Nil(t, capacity.RequestPolicy)
	})

	t.Run("smtEnabled and threadsPerCore stay true regardless of the option", func(t *testing.T) {
		// The hardware fact is unaffected by whether allocation enforces it.
		dev := buildOne(t, cpuset.New(0), false)
		require.True(t, *dev.Attributes[device.AttributeSMTEnabled].BoolValue)
		require.EqualValues(t, 2, *dev.Attributes[device.AttributeThreadsPerCore].IntValue)
	})
}

func TestGroupedFullPhysicalCPUsOnlyIsANoOpWithoutSMT(t *testing.T) {
	// Every core has one thread: nothing to keep together, so the feature is a
	// no-op even though it was requested -- distinct from the non-uniform case,
	// which is disabled rather than a no-op.
	topo := fakeCacheTopology()
	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo,
		topo.CPUDetails.CPUs(), cpuset.New(), store.NewPCIeRootMapper(), false, true, unpartitioned(topo.CPUDetails.CPUs()))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)

	capacity, ok := devices[0].Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	require.True(t, ok)
	require.Equal(t, int64(8), capacity.Value.Value(), "nothing is dropped: single-thread cores are already complete")
	require.Nil(t, capacity.RequestPolicy, "a step of one CPU would constrain nothing")
	require.EqualValues(t, 1, *devices[0].Attributes[device.AttributeThreadsPerCore].IntValue)
	require.False(t, *devices[0].Attributes[device.AttributeSMTEnabled].BoolValue)
	require.Empty(t, built.ThreadsPerCore, "a step of 1 does not qualify as whole-core allocation in effect")
}

// twoNUMANodesOneNonUniform returns two NUMA nodes on one socket: NUMA 0 has 4
// cores uniformly two-way SMT (CPUs 0-3, 8-11), NUMA 1 has 4 cores where core 7
// lacks its second thread, as a partially offlined SMT core would (CPUs
// 4-7, 12-14). Regression shape for the bug where one non-uniform core
// anywhere on the node disabled whole-core allocation for every device.
func twoNUMANodesOneNonUniform() *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for cpu := range 16 {
		core := cpu % 8
		if core == 7 && cpu >= 8 {
			continue
		}
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        core,
			SocketID:      0,
			NUMANodeID:    core / 4,
			UncoreCacheID: core / 4,
			SiblingCPUID:  (cpu + 8) % 16,
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: len(details), NumCores: 8, NumSockets: 1, NumNUMANodes: 2, NumUncoreCache: 2,
		SMTEnabled: true, CPUDetails: details,
	}
}

func TestGroupedFullPhysicalCPUsOnlyIsPerDevice(t *testing.T) {
	topo := twoNUMANodesOneNonUniform()
	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo,
		topo.CPUDetails.CPUs(), cpuset.New(), store.NewPCIeRootMapper(), false, true, unpartitioned(topo.CPUDetails.CPUs()))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 2)

	byName := make(map[string]resourceapi.Device, len(devices))
	for _, dev := range devices {
		byName[dev.Name] = dev
	}
	numa0 := byName[device.CPUDeviceNUMAGroupedPrefix+"000"]
	numa1 := byName[device.CPUDeviceNUMAGroupedPrefix+"001"]

	require.EqualValues(t, 2, *numa0.Attributes[device.AttributeThreadsPerCore].IntValue, "NUMA 0's own cores are uniform")
	require.True(t, *numa0.Attributes[device.AttributeSMTEnabled].BoolValue)
	require.Equal(t, 2, built.ThreadsPerCore[numa0.Name], "whole-core allocation stays in effect for the uniform device")
	require.NotNil(t, numa0.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)].RequestPolicy)

	require.EqualValues(t, 0, *numa1.Attributes[device.AttributeThreadsPerCore].IntValue, "NUMA 1's cores do not agree")
	require.False(t, *numa1.Attributes[device.AttributeSMTEnabled].BoolValue)
	require.NotContains(t, built.ThreadsPerCore, numa1.Name, "the other device's non-uniform cores must not disable it here")
	require.Nil(t, numa1.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)].RequestPolicy)
	// The odd core is not dropped: nothing here promises whole cores for it.
	require.EqualValues(t, 7, *numa1.Attributes[device.AttributeNumCPUs].IntValue)
}

func TestGroupedThreadsPerCoreIndependentOfFullPhysicalCPUsOnly(t *testing.T) {
	// The hardware fact must not read false just because the operator has not
	// turned on whole-core allocation.
	topo := fakeSMTCacheTopology()
	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo,
		topo.CPUDetails.CPUs(), cpuset.New(), store.NewPCIeRootMapper(), false, false, unpartitioned(topo.CPUDetails.CPUs()))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)

	require.True(t, *devices[0].Attributes[device.AttributeSMTEnabled].BoolValue)
	require.EqualValues(t, 2, *devices[0].Attributes[device.AttributeThreadsPerCore].IntValue)
	require.Nil(t, devices[0].Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)].RequestPolicy,
		"no request policy without fullPhysicalCPUsOnly")
	require.Empty(t, built.ThreadsPerCore, "no device gets an allocation step without fullPhysicalCPUsOnly")
}

func TestGroupedNonUniformDeviceHasNoSMTAttribute(t *testing.T) {
	// hybridTopo is a hybrid part: cores 0 and 1 are SMT, cores 2 and 3 are
	// single-threaded, so there is no single thread count for this device.
	hybridDetails := cpuinfo.CPUDetails{}
	for cpu := range 4 {
		hybridDetails[cpu] = cpuinfo.CPUInfo{CpuID: cpu, CoreID: cpu % 2, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0}
	}
	hybridDetails[4] = cpuinfo.CPUInfo{CpuID: 4, CoreID: 2, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0}
	hybridDetails[5] = cpuinfo.CPUInfo{CpuID: 5, CoreID: 3, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0}
	hybridTopo := &cpuinfo.CPUTopology{
		NumCPUs: 6, NumCores: 4, NumSockets: 1, NumNUMANodes: 1, NumUncoreCache: 1,
		SMTEnabled: true, CPUDetails: hybridDetails,
	}

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, hybridTopo,
		hybridTopo.CPUDetails.CPUs(), cpuset.New(), store.NewPCIeRootMapper(), false, true, unpartitioned(hybridTopo.CPUDetails.CPUs()))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)

	require.EqualValues(t, 0, *devices[0].Attributes[device.AttributeThreadsPerCore].IntValue)
	require.False(t, *devices[0].Attributes[device.AttributeSMTEnabled].BoolValue)
	require.Empty(t, built.ThreadsPerCore)
	require.EqualValues(t, 6, *devices[0].Attributes[device.AttributeNumCPUs].IntValue,
		"nothing is dropped: there is no single core size to enforce")
}

// wideSMTTopology returns a single-socket, single-NUMA topology of coresPerNode
// cores with threadsPerCore threads each, numbered the way the kernel numbers a
// real multi-way SMT part: thread t of core c is CPU c+t*coresPerNode (the AMD
// EPYC n<->n+128 pattern generalised to more than two threads).
func wideSMTTopology(coresPerNode, threadsPerCore int) *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for core := range coresPerNode {
		siblings := make([]int, threadsPerCore)
		for t := range threadsPerCore {
			siblings[t] = core + t*coresPerNode
		}
		siblingSet := cpuset.New(siblings...)
		for _, cpu := range siblings {
			details[cpu] = cpuinfo.CPUInfo{
				CpuID:         cpu,
				CoreID:        core,
				SocketID:      0,
				NUMANodeID:    0,
				SiblingCPUID:  -1,
				SiblingCPUSet: siblingSet,
			}
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: coresPerNode * threadsPerCore, NumCores: coresPerNode, NumSockets: 1, NumNUMANodes: 1,
		SMTEnabled: threadsPerCore > 1, CPUDetails: details,
	}
}

// devicesByCPU maps each published individual device to the CPU ID it names,
// for asserting device-ID adjacency independent of publication order.
func devicesByCPU(t *testing.T, devices []resourceapi.Device, cpuIDByDevice map[string]int) map[int]resourceapi.Device {
	t.Helper()
	byCPU := make(map[int]resourceapi.Device, len(devices))
	for _, dev := range devices {
		cpuID, ok := cpuIDByDevice[dev.Name]
		require.True(t, ok, "device %q has no entry in the name-to-CPU map", dev.Name)
		byCPU[cpuID] = dev
	}
	return byCPU
}

// deviceNum extracts the trailing "%03d" ordinal from a cpudevNNN device name.
func deviceNum(t *testing.T, name string) int {
	t.Helper()
	var n int
	_, err := fmt.Sscanf(name, device.CPUDevicePrefix+"%03d", &n)
	require.NoError(t, err, "device name %q does not match %s%%03d", name, device.CPUDevicePrefix)
	return n
}

func TestIndividualModeGroupsAllSiblingsOfAPhysicalCore(t *testing.T) {
	tests := []struct {
		name           string
		threadsPerCore int
	}{
		{name: "SMT2", threadsPerCore: 2},
		{name: "SMT4", threadsPerCore: 4},
		{name: "SMT8 (POWER)", threadsPerCore: 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topo := wideSMTTopology(2, tc.threadsPerCore)
			devices, cpuIDByDevice := device.Build(topo, cpuset.New(), store.NewPCIeRootMapper(), false)
			require.Len(t, devices, topo.NumCPUs)

			byCPU := devicesByCPU(t, devices, cpuIDByDevice)
			for core := range 2 {
				var nums []int
				for thread := range tc.threadsPerCore {
					cpuID := core + thread*2
					dev, ok := byCPU[cpuID]
					require.True(t, ok, "no device published for CPU %d", cpuID)
					nums = append(nums, deviceNum(t, dev.Name))
				}
				sort.Ints(nums)
				for i := 1; i < len(nums); i++ {
					require.Equal(t, nums[0]+i, nums[i],
						"core %d's %d threads must get %d consecutive device IDs, got %v", core, tc.threadsPerCore, tc.threadsPerCore, nums)
				}
			}
		})
	}
}

func TestIndividualModeReservedSiblingLeavesRestOfCoreGrouped(t *testing.T) {
	// A 4-way core with one thread reserved: the remaining three siblings must
	// still get consecutive device IDs among themselves.
	topo := wideSMTTopology(1, 4)
	reserved := cpuset.New(0)

	devices, cpuIDByDevice := device.Build(topo, reserved, store.NewPCIeRootMapper(), false)
	require.Len(t, devices, 3)

	byCPU := devicesByCPU(t, devices, cpuIDByDevice)
	var nums []int
	for _, cpuID := range []int{1, 2, 3} {
		dev, ok := byCPU[cpuID]
		require.True(t, ok, "no device published for CPU %d", cpuID)
		nums = append(nums, deviceNum(t, dev.Name))
	}
	sort.Ints(nums)
	require.Equal(t, []int{nums[0], nums[0] + 1, nums[0] + 2}, nums,
		"the three unreserved siblings must still get consecutive device IDs, got %v", nums)
}

func TestGroupedDevicesPerPartition(t *testing.T) {
	topo := fakeCacheTopology()
	online := topo.CPUDetails.CPUs()
	// system is kept from every workload and helpers is a pool, so both are out
	// of the allocatable set the way the driver folds them into it.
	allocatable := online.Difference(cpuset.New(0, 1))
	partitions := device.WithImplicitDefault([]device.Partition{
		{Name: "system", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(0)},
		{Name: "helpers", Role: device.PARTITION_ROLE_SHARED, CPUs: cpuset.New(1)},
		{Name: "dataplane", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(2, 3), ThreadsPerCore: 1},
	}, allocatable)

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(0, 1), store.NewPCIeRootMapper(), false, false, partitions)

	require.Len(t, built.ByPartition, 2, "the reserved partition and the pool publish no device")
	dataplane := built.ByPartition[0]
	implicit := built.ByPartition[1]
	require.Len(t, dataplane, 1)
	require.Len(t, implicit, 1)

	require.Equal(t, device.CPUDeviceNUMAGroupedPrefix+"000-dataplane", dataplane[0].Name)
	require.Equal(t, "dataplane", *dataplane[0].Attributes[device.AttributePartition].StringValue)
	require.Equal(t, device.PARTITION_ROLE_EXCLUSIVE, *dataplane[0].Attributes[device.AttributeRole].StringValue)
	dataplaneCapacity := dataplane[0].Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	require.EqualValues(t, 2, dataplaneCapacity.Value.Value())
	require.Equal(t, []resourceapi.DeviceTaint{{
		Key:    device.PartitionTaintKey,
		Value:  "dataplane",
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	}}, dataplane[0].Taints, "a named partition is reached only by a claim that tolerates it")

	require.Equal(t, device.CPUDeviceNUMAGroupedPrefix+"000", implicit[0].Name,
		"the implicit partition keeps the name an undescribed node publishes")
	require.Equal(t, device.DefaultPartitionName, *implicit[0].Attributes[device.AttributePartition].StringValue)
	require.Empty(t, implicit[0].Taints, "a claim naming no partition must be allocatable somewhere")
	implicitCapacity := implicit[0].Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	require.EqualValues(t, 4, implicitCapacity.Value.Value(),
		"CPUs 4-7: what the described partitions and the reservation left")
}

func TestGroupedDevicesOfAnUndescribedNodeNameTheImplicitPartition(t *testing.T) {
	topo := fakeCacheTopology()
	online := topo.CPUDetails.CPUs()

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(), store.NewPCIeRootMapper(), false, false, unpartitioned(online))
	devices := onlyPartition(t, built.ByPartition)
	require.Len(t, devices, 1)

	require.Equal(t, device.CPUDeviceNUMAGroupedPrefix+"000", devices[0].Name)
	require.Equal(t, device.DefaultPartitionName, *devices[0].Attributes[device.AttributePartition].StringValue)
	require.Equal(t, device.PARTITION_ROLE_DEFAULT, *devices[0].Attributes[device.AttributeRole].StringValue)
	require.Empty(t, devices[0].Taints)
}

func TestGroupedPartitionSplitsAGroupIntoTwoDevices(t *testing.T) {
	// One NUMA node split between two partitions must yield two devices with
	// disjoint CPUs, not one device answering for both.
	topo := fakeCacheTopology()
	online := topo.CPUDetails.CPUs()
	partitions := device.WithImplicitDefault([]device.Partition{
		{Name: "vm", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(4, 5, 6, 7)},
	}, online)

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(), store.NewPCIeRootMapper(), false, false, partitions)
	require.Len(t, built.ByPartition, 2)
	require.Len(t, built.ByPartition[0], 1)
	require.Len(t, built.ByPartition[1], 1)

	vm := built.ByPartition[0][0]
	implicit := built.ByPartition[1][0]
	require.Equal(t, device.CPUDeviceNUMAGroupedPrefix+"000-vm", vm.Name)
	vmCapacity := vm.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	implicitCapacity := implicit.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	require.EqualValues(t, 4, vmCapacity.Value.Value())
	require.EqualValues(t, 4, implicitCapacity.Value.Value())
}

func TestGroupedPartitionDeviceThreadArityIsItsOwn(t *testing.T) {
	// The dataplane partition's siblings are offline, so its cores have one
	// thread while the rest of the node has two. Whole-core allocation must stay
	// in effect for the partition that still has SMT.
	topo := fakeSMTCacheTopology()
	for _, cpu := range []int{10, 11} {
		delete(topo.CPUDetails, cpu)
	}
	topo.NumCPUs = len(topo.CPUDetails)
	online := topo.CPUDetails.CPUs()
	partitions := device.WithImplicitDefault([]device.Partition{
		{Name: "dataplane", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(2, 3), ThreadsPerCore: 1},
	}, online)

	built := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(), store.NewPCIeRootMapper(), false, true, partitions)
	require.Len(t, built.ByPartition, 2)
	dataplane := built.ByPartition[0][0]
	implicit := built.ByPartition[1][0]

	require.EqualValues(t, 1, *dataplane.Attributes[device.AttributeThreadsPerCore].IntValue)
	require.False(t, *dataplane.Attributes[device.AttributeSMTEnabled].BoolValue)
	require.NotContains(t, built.ThreadsPerCore, dataplane.Name, "one thread per core needs no allocation step")
	require.Nil(t, dataplane.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)].RequestPolicy)

	require.EqualValues(t, 2, *implicit.Attributes[device.AttributeThreadsPerCore].IntValue)
	require.Equal(t, 2, built.ThreadsPerCore[implicit.Name])
	require.NotNil(t, implicit.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)].RequestPolicy)
}
