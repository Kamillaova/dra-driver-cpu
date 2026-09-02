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
				devices, _ = device.BuildGrouped(logr.Discard(), tc.groupBy, topo, online, reserved, store.NewPCIeRootMapper(), tc.publishNodeAllocatableMapping)
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

func TestGroupedUncoreCacheAttributes(t *testing.T) {
	topo := fakeCacheTopology()
	online := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)

	devices, _ := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo, online,
		cpuset.New(0), store.NewPCIeRootMapper(), false)
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

	devices, _ := device.BuildGrouped(logr.Discard(), device.GROUP_BY_NUMA_NODE, topo,
		cpuset.New(0, 1, 2, 3, 4, 5, 6, 7), cpuset.New(), store.NewPCIeRootMapper(), false)
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
