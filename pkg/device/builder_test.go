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
	"path"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/dynamic-resource-allocation/deviceattribute"
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
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: 4, NumCores: 4, NumSockets: 2, NumNUMANodes: 2,
		SMTEnabled: false, CPUDetails: details,
	}
}

func TestDeviceBuilderNodeAllocatableResourceMapping(t *testing.T) {
	topo := fakeTopology()
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
			res, err := device.Build(device.BuildInput{
				Inventory: device.Inventory{
					CPUTopology:  topo,
					ReservedCPUs: reserved,
				},
				Layout:                                device.FindLayout(tc.cpuDeviceMode, tc.groupBy),
				PCIeRootMapper:                        store.NewPCIeRootMapper(),
				PublishNodeAllocatableResourceMapping: tc.publishNodeAllocatableMapping,
			})
			require.NoError(t, err)
			require.NotEmpty(t, res.Devices)

			for _, dev := range res.Devices {
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

func TestMachineGroupedUsesTopologyValidatedCPUs(t *testing.T) {
	topo := fakeTopology()
	// CPU 4 was omitted from CPUDetails because topology discovery could not
	// validate it.

	res, err := device.Build(
		device.BuildInput{
			Inventory: device.Inventory{
				CPUTopology:  topo,
				ReservedCPUs: cpuset.New(),
			},
			Layout:                                device.LayoutMachine,
			PCIeRootMapper:                        store.NewPCIeRootMapper(),
			PublishNodeAllocatableResourceMapping: false,
		})
	require.NoError(t, err)
	require.Len(t, res.Devices, 1)

	capacity := res.Devices[0].Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
	require.Equal(t, int64(4), capacity.Value.Value())
	numCPUs := res.Devices[0].Attributes[device.AttributeNumCPUs]
	require.NotNil(t, numCPUs.IntValue)
	require.Equal(t, int64(4), *numCPUs.IntValue)
}

// coreTopology builds a one-socket topology from cores, each the CPUs of one
// physical core, which carry it as their sibling list; core c sits on NUMA node
// numaOf(c).
func coreTopology(smtEnabled bool, numaOf func(core int) int, cores ...[]int) *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	numaNodes := map[int]bool{}
	for coreID, cpus := range cores {
		siblings := cpuset.New(cpus...)
		for _, cpu := range cpus {
			sibling := -1
			if len(cpus) == 2 {
				sibling = cpus[0] + cpus[1] - cpu
			}
			details[cpu] = cpuinfo.CPUInfo{
				CpuID:         cpu,
				CoreID:        coreID,
				SocketID:      0,
				NUMANodeID:    numaOf(coreID),
				SiblingCPUID:  sibling,
				SiblingCPUSet: siblings,
			}
		}
		numaNodes[numaOf(coreID)] = true
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: len(details), NumCores: len(cores), NumSockets: 1, NumNUMANodes: len(numaNodes),
		SMTEnabled: smtEnabled, CPUDetails: details,
	}
}

func onNUMANode0(int) int { return 0 }

// twoThreadCores is n cores of two threads, core c holding CPUs c and c+n.
func twoThreadCores(n int) [][]int {
	cores := make([][]int, n)
	for c := range n {
		cores[c] = []int{c, c + n}
	}
	return cores
}

// offlineSiblingCores has four cores of two threads and two whose siblings are
// offline.
var offlineSiblingCores = [][]int{{0, 128}, {1, 129}, {2, 130}, {3, 131}, {4}, {5}}

// hybridCores is shaped like an Intel Core i7-12850HX: CPUs 0-7 are cores of one
// thread, CPUs 8-15 cores of two whose siblings are 16-23.
func hybridCores() [][]int {
	var cores [][]int
	for c := range 8 {
		cores = append(cores, []int{c})
	}
	for c := 8; c < 16; c++ {
		cores = append(cores, []int{c, c + 8})
	}
	return cores
}

func TestBuildWholeCores(t *testing.T) {
	type wantPolicy struct {
		def, min, step int64 // step 0: none
	}
	type wantDevice struct {
		capacity       int64
		threadsPerCore int
		smtEnabled     bool
		policy         *wantPolicy
	}
	twoNUMANodes := func(core int) int { return core / 2 }

	tests := []struct {
		name                 string
		topo                 *cpuinfo.CPUTopology
		layout               device.Layout
		reserved             cpuset.CPUSet
		fullPhysicalCPUsOnly bool
		want                 map[string]wantDevice
	}{
		{
			name:     "option off: a split core keeps its free thread and no policy is published",
			topo:     coreTopology(true, onNUMANode0, twoThreadCores(4)...),
			layout:   device.LayoutNUMANode,
			reserved: cpuset.New(0),
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 7, threadsPerCore: 2, smtEnabled: true},
			},
		},
		{
			name:                 "option on: a split core leaves the device",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(4)...),
			layout:               device.LayoutNUMANode,
			reserved:             cpuset.New(0),
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 6, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 6, min: 2, step: 2}},
			},
		},
		{
			name:                 "option on: one core of two threads gets min without a step",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(1)...),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 2, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 2, min: 2}},
			},
		},
		{
			name:                 "option on: two cores of two threads get a step",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(2)...),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 4, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 4, min: 2, step: 2}},
			},
		},
		{
			name:                 "option on: cores of one thread get no policy",
			topo:                 coreTopology(false, onNUMANode0, []int{0}, []int{1}, []int{2}, []int{3}),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 4, threadsPerCore: 1, smtEnabled: false},
			},
		},
		{
			name:                 "option on: cores of two and one thread make a mixed device",
			topo:                 coreTopology(true, onNUMANode0, offlineSiblingCores...),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 10, threadsPerCore: 0, smtEnabled: true},
			},
		},
		{
			name:                 "option on: a mixed device leaves out a split core too",
			topo:                 coreTopology(true, onNUMANode0, offlineSiblingCores...),
			layout:               device.LayoutNUMANode,
			reserved:             cpuset.New(128),
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 8, threadsPerCore: 0, smtEnabled: true},
			},
		},
		{
			name:                 "option on: a hybrid CPU makes a mixed device",
			topo:                 coreTopology(true, onNUMANode0, hybridCores()...),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 24, threadsPerCore: 0, smtEnabled: true},
			},
		},
		{
			name:                 "option on: threads are counted on the cores left in the device",
			topo:                 coreTopology(true, onNUMANode0, []int{0, 3}, []int{1}, []int{2}),
			layout:               device.LayoutNUMANode,
			reserved:             cpuset.New(3),
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 2, threadsPerCore: 1, smtEnabled: true},
			},
		},
		{
			name:                 "option on: each device decides for itself",
			topo:                 coreTopology(true, twoNUMANodes, []int{0, 4}, []int{1, 5}, []int{2, 6}, []int{3}),
			layout:               device.LayoutNUMANode,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 4, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 4, min: 2, step: 2}},
				"cpudevnuma001": {capacity: 3, threadsPerCore: 0, smtEnabled: true},
			},
		},
		{
			name:   "option off: smtEnabled is the node's on mixed and one-thread devices",
			topo:   coreTopology(true, twoNUMANodes, []int{0, 4}, []int{1}, []int{2}, []int{3}),
			layout: device.LayoutNUMANode,
			want: map[string]wantDevice{
				"cpudevnuma000": {capacity: 3, threadsPerCore: 0, smtEnabled: true},
				"cpudevnuma001": {capacity: 2, threadsPerCore: 1, smtEnabled: true},
			},
		},
		{
			name:                 "option on, socket layout",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(2)...),
			layout:               device.LayoutSocket,
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevsocket000": {capacity: 4, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 4, min: 2, step: 2}},
			},
		},
		{
			name:                 "option on, machine layout",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(2)...),
			layout:               device.LayoutMachine,
			reserved:             cpuset.New(3),
			fullPhysicalCPUsOnly: true,
			want: map[string]wantDevice{
				"cpudevmachine": {capacity: 2, threadsPerCore: 2, smtEnabled: true, policy: &wantPolicy{def: 2, min: 2}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := device.Build(device.BuildInput{
				Inventory: device.Inventory{
					CPUTopology:  tc.topo,
					ReservedCPUs: tc.reserved,
				},
				Layout:               tc.layout,
				PCIeRootMapper:       store.NewPCIeRootMapper(),
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
			})
			require.NoError(t, err)
			require.Len(t, res.Devices, len(tc.want))
			require.Len(t, res.ThreadsPerCore, len(tc.want))

			for _, dev := range res.Devices {
				want, found := tc.want[dev.Name]
				require.Truef(t, found, "unexpected device %q", dev.Name)

				capacity := dev.Capacity[resourceapi.QualifiedName(device.CPUResourceQualifiedName)]
				require.Equalf(t, want.capacity, capacity.Value.Value(), "device %q capacity", dev.Name)
				require.Equalf(t, want.capacity, *dev.Attributes[device.AttributeNumCPUs].IntValue, "device %q numCPUs", dev.Name)
				require.EqualValuesf(t, want.threadsPerCore, *dev.Attributes[device.AttributeThreadsPerCore].IntValue, "device %q threadsPerCore", dev.Name)
				require.Equalf(t, want.threadsPerCore, res.ThreadsPerCore[dev.Name], "device %q threads per core in the result", dev.Name)
				_, mixed := res.MixedCores[dev.Name]
				require.Equalf(t, tc.fullPhysicalCPUsOnly && want.threadsPerCore == 0, mixed, "device %q counted as mixed", dev.Name)
				require.Equalf(t, want.smtEnabled, *dev.Attributes[device.AttributeSMTEnabled].BoolValue, "device %q smtEnabled", dev.Name)

				if want.policy == nil {
					require.Nilf(t, capacity.RequestPolicy, "device %q must publish no request policy", dev.Name)
					continue
				}
				policy := capacity.RequestPolicy
				require.NotNilf(t, policy, "device %q must publish a request policy", dev.Name)
				require.NotNil(t, policy.Default)
				require.Equal(t, want.policy.def, policy.Default.Value())
				require.NotNil(t, policy.ValidRange)
				require.Equal(t, want.policy.min, policy.ValidRange.Min.Value())
				require.Nil(t, policy.ValidRange.Max)
				if want.policy.step == 0 {
					require.Nil(t, policy.ValidRange.Step, "a step past the capacity is refused by the API server")
				} else {
					require.NotNil(t, policy.ValidRange.Step)
					require.Equal(t, want.policy.step, policy.ValidRange.Step.Value())
				}
			}
		})
	}
}

func TestBuildGroupedDeviceAttributeCount(t *testing.T) {
	for _, tc := range []struct {
		layout device.Layout
		want   int
	}{
		{layout: device.LayoutNUMANode, want: 12},
		{layout: device.LayoutSocket, want: 9},
		{layout: device.LayoutMachine, want: 8},
	} {
		res, err := device.Build(device.BuildInput{
			Inventory: device.Inventory{
				CPUTopology:  coreTopology(true, onNUMANode0, twoThreadCores(2)...),
				ReservedCPUs: cpuset.New(),
			},
			Layout:               tc.layout,
			PCIeRootMapper:       store.NewPCIeRootMapper(),
			ExposeExtAttrs:       true,
			FullPhysicalCPUsOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, res.Devices, 1)
		dev := res.Devices[0]
		require.Equalf(t, tc.want, len(dev.Attributes)+len(dev.Capacity), "device %q attributes and capacities", dev.Name)
		require.LessOrEqual(t, len(dev.Attributes)+len(dev.Capacity), resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice)
	}
}

func TestBuildPublishesSettingAttributes(t *testing.T) {
	twoNUMANodes := func(core int) int { return core / 2 }
	for _, tc := range []struct {
		name                 string
		layout               device.Layout
		fullPhysicalCPUsOnly bool
		allocator            string
	}{
		{name: "individual", layout: device.LayoutIndividual, allocator: "cpumanager"},
		{name: "individual, external allocator", layout: device.LayoutIndividual, allocator: "external"},
		{name: "NUMA node", layout: device.LayoutNUMANode, allocator: "cpumanager"},
		{name: "NUMA node, whole cores", layout: device.LayoutNUMANode, fullPhysicalCPUsOnly: true, allocator: "cpumanager"},
		{name: "socket, whole cores, external allocator", layout: device.LayoutSocket, fullPhysicalCPUsOnly: true, allocator: "external"},
		{name: "machine, external allocator", layout: device.LayoutMachine, allocator: "external"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := device.Build(device.BuildInput{
				Inventory: device.Inventory{
					CPUTopology:  coreTopology(true, twoNUMANodes, twoThreadCores(4)...),
					ReservedCPUs: cpuset.New(),
				},
				Layout:               tc.layout,
				PCIeRootMapper:       store.NewPCIeRootMapper(),
				ExposeExtAttrs:       tc.allocator == "external",
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
				Allocator:            tc.allocator,
			})
			require.NoError(t, err)
			require.NotEmpty(t, res.Devices)
			for _, dev := range res.Devices {
				whole := dev.Attributes[device.AttributeFullPhysicalCPUsOnly]
				require.NotNilf(t, whole.BoolValue, "device %q must publish fullPhysicalCPUsOnly as a bool", dev.Name)
				require.Equalf(t, tc.fullPhysicalCPUsOnly, *whole.BoolValue, "device %q fullPhysicalCPUsOnly", dev.Name)
				allocator := dev.Attributes[device.AttributeAllocator]
				require.NotNilf(t, allocator.StringValue, "device %q must publish allocator as a string", dev.Name)
				require.Equalf(t, tc.allocator, *allocator.StringValue, "device %q allocator", dev.Name)
				require.LessOrEqualf(t, len(dev.Attributes)+len(dev.Capacity), resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice,
					"device %q attributes and capacities", dev.Name)
			}
		})
	}
}

// threeThreadCore is one core of CPUs 0-2 by its sibling list, each CPU with a
// core_id of its own: the shape a sysfs overlay can give past GetCPUInfos,
// which refuses more than two CPUs per (socket, cluster, core).
func threeThreadCore() *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	for cpu := range 3 {
		details[cpu] = cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: 0, SiblingCPUID: -1, SiblingCPUSet: cpuset.New(0, 1, 2),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: 3, NumCores: 3, NumSockets: 1, NumNUMANodes: 1, SMTEnabled: true, CPUDetails: details,
	}
}

func TestBuildCountsCoreSizesUnderFullPhysicalCPUsOnly(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		topo                 *cpuinfo.CPUTopology
		reserved             cpuset.CPUSet
		fullPhysicalCPUsOnly bool
		want                 map[string]device.CoreCounts
		wantErr              string
	}{
		{
			name:                 "option on: cores of two and one thread",
			topo:                 coreTopology(true, onNUMANode0, offlineSiblingCores...),
			fullPhysicalCPUsOnly: true,
			want:                 map[string]device.CoreCounts{"cpudevnuma000": {TwoThreads: 4, OneThread: 2}},
		},
		{
			name:                 "option on: the cores left once a split core leaves",
			topo:                 coreTopology(true, onNUMANode0, offlineSiblingCores...),
			reserved:             cpuset.New(128),
			fullPhysicalCPUsOnly: true,
			want:                 map[string]device.CoreCounts{"cpudevnuma000": {TwoThreads: 3, OneThread: 2}},
		},
		{
			name:                 "a device whose cores are alike is not counted",
			topo:                 coreTopology(true, onNUMANode0, twoThreadCores(2)...),
			fullPhysicalCPUsOnly: true,
			want:                 map[string]device.CoreCounts{},
		},
		{
			name: "option off: nothing is counted",
			topo: coreTopology(true, onNUMANode0, offlineSiblingCores...),
			want: map[string]device.CoreCounts{},
		},
		{
			name:                 "option on: a core of three threads fails the build",
			topo:                 threeThreadCore(),
			fullPhysicalCPUsOnly: true,
			wantErr:              `device "cpudevnuma000" holds a core of 3 threads (CPUs 0-2), where fullPhysicalCPUsOnly supports whole cores of 1 or 2 threads`,
		},
		{
			name: "option off: a core of three threads builds",
			topo: threeThreadCore(),
			want: map[string]device.CoreCounts{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := device.Build(device.BuildInput{
				Inventory: device.Inventory{
					CPUTopology:  tc.topo,
					ReservedCPUs: tc.reserved,
				},
				Layout:               device.LayoutNUMANode,
				PCIeRootMapper:       store.NewPCIeRootMapper(),
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
			})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, res.MixedCores)
		})
	}
}

// TestBuildPublishesScalarNUMANodeAttribute pins the shape of the standard
// numaNode attribute published for a device bound to a single NUMA node: the
// standard name, the scalar int type and the value. The value is built by the
// upstream deviceattribute helpers, but the published attribute must remain
// exactly what the driver published before the switch.
func TestBuildPublishesScalarNUMANodeAttribute(t *testing.T) {
	tests := []struct {
		name     string
		layout   device.Layout
		wantNUMA map[string]int64
	}{
		{
			name:   "individual",
			layout: device.LayoutIndividual,
			wantNUMA: map[string]int64{
				"cpudev000": 0,
				"cpudev001": 0,
				"cpudev002": 1,
				"cpudev003": 1,
			},
		},
		{
			name:   "grouped by numanode",
			layout: device.LayoutNUMANode,
			wantNUMA: map[string]int64{
				"cpudevnuma000": 0,
				"cpudevnuma001": 1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := device.Build(device.BuildInput{
				Inventory: device.Inventory{
					CPUTopology:  fakeTopology(),
					ReservedCPUs: cpuset.New(),
				},
				Layout:         tc.layout,
				PCIeRootMapper: store.NewPCIeRootMapper(),
			})
			require.NoError(t, err)
			require.Len(t, res.Devices, len(tc.wantNUMA))

			for _, dev := range res.Devices {
				want, found := tc.wantNUMA[dev.Name]
				require.Truef(t, found, "unexpected device %q", dev.Name)

				attr, found := dev.Attributes[deviceattribute.StandardDeviceAttributeNUMANode]
				require.Truef(t, found, "device %q must publish the standard numaNode attribute", dev.Name)
				// This release publishes the scalar form only; the list form is a
				// separate follow-up (see #320).
				require.Nilf(t, attr.IntValues, "device %q must publish numaNode in scalar form", dev.Name)
				require.NotNilf(t, attr.IntValue, "device %q must publish a numaNode int value", dev.Name)
				require.Equalf(t, want, *attr.IntValue, "device %q numaNode", dev.Name)
			}
		})
	}
}

func TestBuildValidatesDeviceAttributeValueCount(t *testing.T) {
	topo := &cpuinfo.CPUTopology{
		NumCPUs: 1,
		CPUDetails: cpuinfo.CPUDetails{
			0: {CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, SiblingCPUID: -1},
		},
	}
	build := func(mapper *store.PCIeRootMapper) (device.BuildResult, error) {
		return device.Build(device.BuildInput{
			Inventory: device.Inventory{
				CPUTopology:  topo,
				ReservedCPUs: cpuset.New(),
			},
			Layout:         device.LayoutNUMANode,
			PCIeRootMapper: mapper,
			ExposeExtAttrs: true,
		})
	}

	withoutPCIeRoots, err := build(store.NewPCIeRootMapper())
	require.NoError(t, err)
	maxPCIeRoots := resourceapi.ResourceSliceMaxAttributeValuesPerDevice - countAttributeValues(withoutPCIeRoots.Devices[0].Attributes)

	withPCIeRoots, err := build(newPCIeRootMapper(t, maxPCIeRoots))
	require.NoError(t, err)
	device := withPCIeRoots.Devices[0]
	require.Len(t, device.Attributes[deviceattribute.StandardDeviceAttributePCIeRoot].StringValues, maxPCIeRoots)
	require.LessOrEqual(t, len(device.Attributes)+len(device.Capacity), resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice)
	require.LessOrEqual(t, countAttributeValues(device.Attributes), resourceapi.ResourceSliceMaxAttributeValuesPerDevice)

	_, err = build(newPCIeRootMapper(t, maxPCIeRoots+1))
	require.ErrorContains(t, err, "DRA max attribute value limit")
}

func newPCIeRootMapper(t *testing.T, rootCount int) *store.PCIeRootMapper {
	t.Helper()

	sysfs := fstest.MapFS{}
	for i := range rootCount {
		rootName := fmt.Sprintf("pci%04x:%02x", i/256, i%256)
		busID := strings.TrimPrefix(rootName, "pci")
		sysfs[path.Join("devices", rootName, "pci_bus", busID, "cpulistaffinity")] = &fstest.MapFile{Data: []byte("0\n")}
	}

	mapper := store.NewPCIeRootMapper()
	require.NoError(t, mapper.Probe(testr.New(t), sysfs, cpuset.New(0)))
	return mapper
}

func countAttributeValues(attrs map[resourceapi.QualifiedName]resourceapi.DeviceAttribute) int {
	count := 0
	for _, attr := range attrs {
		if len(attr.BoolValues) == 0 && len(attr.IntValues) == 0 && len(attr.StringValues) == 0 && len(attr.VersionValues) == 0 {
			count++
			continue
		}
		count += len(attr.BoolValues) + len(attr.IntValues) + len(attr.StringValues) + len(attr.VersionValues)
	}
	return count
}

func TestLookupConsumedCapacity(t *testing.T) {
	qty1 := *resource.NewQuantity(1, resource.DecimalSI)
	qty2 := *resource.NewQuantity(2, resource.DecimalSI)

	tests := []struct {
		name             string
		consumedCapacity map[resourceapi.QualifiedName]resource.Quantity
		driverName       string
		wantQuantity     resource.Quantity
		wantOK           bool
	}{
		{
			name:             "fully-qualified with driver name",
			consumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": qty1},
			driverName:       "dra.cpu",
			wantQuantity:     qty1,
			wantOK:           true,
		},
		{
			name:             "unqualified cpu key",
			consumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{"cpu": qty2},
			driverName:       "dra.cpu",
			wantQuantity:     qty2,
			wantOK:           true,
		},
		{
			name:             "fallback to dra.cpu/cpu for quantity when driverName differs",
			consumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": qty1},
			driverName:       "dra-driver-cpu.k8s.io",
			wantQuantity:     qty1,
			wantOK:           true,
		},
		{
			name:             "missing cpu capacity",
			consumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{"memory": qty1},
			driverName:       "dra.cpu",
			wantOK:           false,
		},
		{
			name:             "empty map",
			consumedCapacity: nil,
			driverName:       "dra.cpu",
			wantOK:           false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := device.LookupConsumedCapacity(tc.consumedCapacity, tc.driverName)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.wantQuantity, got)
			}
		})
	}
}

func TestConsumedCPUCount(t *testing.T) {
	tests := []struct {
		name      string
		capacity  map[resourceapi.QualifiedName]resource.Quantity
		wantCount int
		wantErr   string
	}{
		{name: "whole number", capacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": resource.MustParse("4")}, wantCount: 4},
		{name: "unqualified key", capacity: map[resourceapi.QualifiedName]resource.Quantity{"cpu": resource.MustParse("2")}, wantCount: 2},
		{name: "whole number in milli units", capacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": resource.MustParse("3000m")}, wantCount: 3},
		{name: "missing", capacity: map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("1")}, wantErr: `CPU capacity "dra.cpu/cpu" for device "dev0" is missing`},
		{name: "zero", capacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": resource.MustParse("0")}, wantErr: "must be positive, got 0"},
		{name: "negative", capacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": resource.MustParse("-1")}, wantErr: "must be positive, got -1"},
		{name: "fractional", capacity: map[resourceapi.QualifiedName]resource.Quantity{"dra.cpu/cpu": resource.MustParse("1500m")}, wantErr: "must be a whole number, got 1500m"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := resourceapi.DeviceRequestAllocationResult{Device: "dev0", ConsumedCapacity: tc.capacity}
			got, err := device.ConsumedCPUCount(result, "dra.cpu")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantCount, got)
		})
	}
}
