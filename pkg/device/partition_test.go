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
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

func TestResolvePartitions(t *testing.T) {
	// Cores {0,4}, {1,5}, {2,6} and {3,7}.
	details := coreTopology(true, onNUMANode0, twoThreadCores(4)...).CPUDetails
	defaultEntry := func(threads int) device.Partition {
		return device.Partition{Name: device.DefaultPartitionName, ThreadsPerCore: threads}
	}
	vms := func(cpus ...int) device.Partition {
		return device.Partition{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(cpus...), ThreadsPerCore: 1}
	}
	resolved := func(name, role string, cpus ...int) device.Partition {
		return device.Partition{Name: name, Role: role, CPUs: cpuset.New(cpus...)}
	}

	tests := []struct {
		name     string
		declared []device.Partition
		reserved cpuset.CPUSet
		want     []device.Partition
	}{
		{
			name: "the empty list is the whole node as the partition default",
			want: []device.Partition{resolved("default", "default", 0, 1, 2, 3, 4, 5, 6, 7)},
		},
		{
			name:     "the empty list leaves out the reserved CPUs",
			reserved: cpuset.New(0, 4),
			want:     []device.Partition{resolved("default", "default", 1, 2, 3, 5, 6, 7)},
		},
		{
			name:     "the entry alone holds every CPU",
			declared: []device.Partition{defaultEntry(0)},
			want:     []device.Partition{resolved("default", "default", 0, 1, 2, 3, 4, 5, 6, 7)},
		},
		{
			name:     "the entry first holds the cores no other partition names",
			declared: []device.Partition{defaultEntry(0), vms(1, 5)},
			want: []device.Partition{
				resolved("default", "default", 0, 2, 3, 4, 6, 7),
				{Name: "vms", Role: "exclusive", CPUs: cpuset.New(1, 5), ThreadsPerCore: 1},
			},
		},
		{
			name:     "the entry last under smt false keeps the lowest thread of each core",
			declared: []device.Partition{vms(1, 5), defaultEntry(1)},
			want: []device.Partition{
				{Name: "vms", Role: "exclusive", CPUs: cpuset.New(1, 5), ThreadsPerCore: 1},
				{Name: "default", Role: "default", CPUs: cpuset.New(0, 2, 3), ThreadsPerCore: 1},
			},
		},
		{
			name:     "an smt false partition whose sibling is online leaves the sibling out of default",
			declared: []device.Partition{defaultEntry(0), vms(1)},
			want: []device.Partition{
				resolved("default", "default", 0, 2, 3, 4, 6, 7),
				{Name: "vms", Role: "exclusive", CPUs: cpuset.New(1), ThreadsPerCore: 1},
			},
		},
		{
			name: "a reserved partition's cores are no default CPUs",
			declared: []device.Partition{
				{Name: "system", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(0, 4)},
				defaultEntry(0),
			},
			want: []device.Partition{
				resolved("system", "reserved", 0, 4),
				resolved("default", "default", 1, 2, 3, 5, 6, 7),
			},
		},
		{
			name:     "other partitions naming every core leave the entry empty",
			declared: []device.Partition{defaultEntry(0), vms(0, 1, 2, 3, 4, 5, 6, 7)},
			want: []device.Partition{
				resolved("default", "default"),
				{Name: "vms", Role: "exclusive", CPUs: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7), ThreadsPerCore: 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := device.ResolvePartitions(tc.declared, details, tc.reserved)
			require.Len(t, got, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, want.Name, got[i].Name, "partition %d name", i)
				require.Equal(t, want.Role, got[i].Role, "partition %q role", want.Name)
				require.Equal(t, want.ThreadsPerCore, got[i].ThreadsPerCore, "partition %q threads per core", want.Name)
				require.Truef(t, want.CPUs.Equals(got[i].CPUs), "partition %q CPUs: got %s, want %s", want.Name, got[i].CPUs, want.CPUs)
			}
		})
	}
}

func TestPartitionPublishesOwnedDevices(t *testing.T) {
	for _, tc := range []struct {
		role           string
		partitionsUsed bool
		want           bool
	}{
		{role: device.PARTITION_ROLE_DEFAULT, partitionsUsed: false, want: true},
		{role: device.PARTITION_ROLE_DEFAULT, partitionsUsed: true, want: false},
		{role: device.PARTITION_ROLE_EXCLUSIVE, partitionsUsed: true, want: true},
		{role: device.PARTITION_ROLE_RESERVED, partitionsUsed: true, want: false},
	} {
		got := device.Partition{Name: "p", Role: tc.role}.PublishesOwnedDevices(tc.partitionsUsed)
		require.Equalf(t, tc.want, got, "role %q, partitions used %v", tc.role, tc.partitionsUsed)
	}
}
