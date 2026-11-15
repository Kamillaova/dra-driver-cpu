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

package driver

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

// partitionSysFS is mixedCoreCPUInfos' node: CPUs 0-7 present, 6 and 7 offline,
// and CPU 8 online but missing from the topology.
func partitionSysFS() fstest.MapFS {
	return fstest.MapFS{
		filepath.Join("devices", "system", "cpu", "present"): &fstest.MapFile{Data: []byte("0-8\n")},
		filepath.Join("devices", "system", "cpu", "online"):  &fstest.MapFile{Data: []byte("0-5,8\n")},
	}
}

func TestValidatePartitions(t *testing.T) {
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)
	present, online := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8), cpuset.New(0, 1, 2, 3, 4, 5, 8)
	partition := func(name string, threads int, cpus ...int) device.Partition {
		return device.Partition{Name: name, Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(cpus...), ThreadsPerCore: threads}
	}
	defaultEntry := device.Partition{Name: device.DefaultPartitionName}

	for _, tc := range []struct {
		name     string
		declared []device.Partition
		wantErr  string
	}{
		{
			name:     "whole cores, one of them with its sibling offline",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 4), partition("spdk", 1, 2)},
		},
		{
			name:     "a CPU the node does not have",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 4, 9)},
			wantErr:  `partition "vms" names CPUs 9, which this node does not have: remove them from its cpuset`,
		},
		{
			name:     "an offline CPU",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 2, 6)},
			wantErr:  `partition "spdk" names offline CPUs 6: bring them online, or remove them from its cpuset`,
		},
		{
			name:     "an online CPU outside the topology",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 3, 8)},
			wantErr:  `partition "vms" names CPUs 8, whose topology the kernel reports incompletely so the driver does not manage them: remove them from its cpuset`,
		},
		{
			name:     "two smt false partitions naming the two threads of one core",
			declared: []device.Partition{partition("a", 1, 0), defaultEntry, partition("b", 1, 4)},
			wantErr:  `partitions "a" and "b" name threads of one physical core, CPUs 0,4: list all of a core's threads in one partition`,
		},
		{
			name:     "a core split with the default partition",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 1, 4)},
			wantErr:  `partition "vms" splits the physical cores of CPUs 1: a partition holds whole cores, so list their sibling threads 5 in it too`,
		},
		{
			name:     "an smt false partition whose sibling is online",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 1)},
			wantErr:  `partition "spdk" splits the physical cores of CPUs 1: a partition holds whole cores, so list their sibling threads 5 in it too`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitions(topo.CPUDetails, present, online, tc.declared)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewWithPartitions(t *testing.T) {
	declared := []device.Partition{
		{Name: device.DefaultPartitionName},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 1, 4, 5)},
		{Name: "system", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(3)},
	}
	for _, tc := range []struct {
		name         string
		sysfs        fstest.MapFS
		declared     []device.Partition
		wantErr      string
		wantReserved cpuset.CPUSet
		wantSlices   [][]string
	}{
		{
			name:         "the exclusive partition publishes, the reserved one holds its CPUs back",
			sysfs:        partitionSysFS(),
			declared:     declared,
			wantReserved: cpuset.New(3),
			wantSlices:   [][]string{{"cpudevnuma000-vms"}},
		},
		{
			name:     "a partition naming an offline CPU stops the driver",
			sysfs:    partitionSysFS(),
			declared: []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(7)}},
			wantErr:  `partition "vms" names offline CPUs 7: bring them online, or remove them from its cpuset`,
		},
		{
			name:     "the present CPUs unreadable",
			sysfs:    testSysFS(mixedCoreCPUInfos),
			declared: declared,
			wantErr:  "failed to read the present CPUs: open devices/system/cpu/present: file does not exist",
		},
		{
			name:         "without partitions nothing is read and the node is upstream's",
			sysfs:        fstest.MapFS{},
			wantReserved: cpuset.New(),
			wantSlices:   [][]string{{"cpudevnuma000"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := New(testr.New(t), Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   tc.sysfs,
			}, &Config{
				DriverName:       testDriverName,
				NodeName:         testNodeName,
				ReservedCPUs:     cpuset.New(),
				CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
				CPUPartitions:    tc.declared,
			})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Truef(t, tc.wantReserved.Equals(d.topology.ReservedCPUs), "reserved CPUs: got %s, want %s", d.topology.ReservedCPUs, tc.wantReserved)
			require.Truef(t, d.allocationState.GetSharedCPUs().Intersection(tc.wantReserved).IsEmpty(), "shared CPUs %s hold reserved ones", d.allocationState.GetSharedCPUs())
			var gotSlices [][]string
			for _, slice := range d.topology.deviceSlices {
				var names []string
				for _, dev := range slice {
					names = append(names, dev.Name)
				}
				gotSlices = append(gotSlices, names)
			}
			require.Equal(t, tc.wantSlices, gotSlices)
		})
	}
}
