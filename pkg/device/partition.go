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

package device

import (
	"slices"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/utils/cpuset"
)

const (
	// PARTITION_ROLE_DEFAULT holds CPUs that claims share.
	PARTITION_ROLE_DEFAULT = "default"
	// PARTITION_ROLE_EXCLUSIVE holds CPUs a claim takes for itself.
	PARTITION_ROLE_EXCLUSIVE = "exclusive"
	// PARTITION_ROLE_RESERVED holds CPUs kept from every workload.
	PARTITION_ROLE_RESERVED = "reserved"
)

// DefaultPartitionName names the partition entry whose presence switches a
// node into partitions. Its role is always PARTITION_ROLE_DEFAULT.
const DefaultPartitionName = "default"

// PartitionRoles returns the roles a partition entry may name.
func PartitionRoles() []string {
	return []string{PARTITION_ROLE_DEFAULT, PARTITION_ROLE_EXCLUSIVE, PARTITION_ROLE_RESERVED}
}

// IsPartitionRole reports whether role is one a partition entry may name.
func IsPartitionRole(role string) bool {
	switch role {
	case PARTITION_ROLE_DEFAULT, PARTITION_ROLE_EXCLUSIVE, PARTITION_ROLE_RESERVED:
		return true
	}
	return false
}

// Partition is a cpuPartitions entry resolved against the node.
type Partition struct {
	Name string
	Role string
	CPUs cpuset.CPUSet
	// ThreadsPerCore is the online threads per core the entry's smt asks for,
	// and 0 when it accepts whatever the platform provides.
	ThreadsPerCore int
}

// PublishesOwnedDevices reports whether claims take CPUs of p for themselves.
// Without partitions the whole node is the default partition and does, as
// upstream's devices do.
func (p Partition) PublishesOwnedDevices(partitionsUsed bool) bool {
	switch p.Role {
	case PARTITION_ROLE_EXCLUSIVE:
		return true
	case PARTITION_ROLE_DEFAULT:
		return !partitionsUsed
	}
	return false
}

// IsReserved reports whether p keeps its CPUs from every workload.
func (p Partition) IsReserved() bool {
	return p.Role == PARTITION_ROLE_RESERVED
}

// ResolvePartitions fills in the entry named DefaultPartitionName: its role is
// PARTITION_ROLE_DEFAULT and its CPUs are the CPUs of details on the cores no
// other partition names, a core being named when any of its CPUs is listed,
// and under ThreadsPerCore N the lowest N of each core. The other entries are
// returned as declared, in their order. Without declared partitions the whole
// node less reservedCPUs is the one partition DefaultPartitionName.
func ResolvePartitions(declared []Partition, details cpuinfo.CPUDetails, reservedCPUs cpuset.CPUSet) []Partition {
	if len(declared) == 0 {
		return []Partition{{
			Name: DefaultPartitionName,
			Role: PARTITION_ROLE_DEFAULT,
			CPUs: details.CPUs().Difference(reservedCPUs),
		}}
	}
	named := make(map[cpuinfo.CoreLocation]bool)
	for _, partition := range declared {
		if partition.Name == DefaultPartitionName {
			continue
		}
		for _, cpuID := range partition.CPUs.UnsortedList() {
			if loc, ok := details.CoreOf(cpuID); ok {
				named[loc] = true
			}
		}
	}
	resolved := slices.Clone(declared)
	for i := range resolved {
		if resolved[i].Name != DefaultPartitionName {
			continue
		}
		resolved[i].Role = PARTITION_ROLE_DEFAULT
		resolved[i].CPUs = remainderCPUs(details, named, resolved[i].ThreadsPerCore).Difference(reservedCPUs)
	}
	return resolved
}

func remainderCPUs(details cpuinfo.CPUDetails, named map[cpuinfo.CoreLocation]bool, threadsPerCore int) cpuset.CPUSet {
	threads := make(map[cpuinfo.CoreLocation][]int)
	for _, cpuID := range details.CPUs().List() {
		loc, ok := details.CoreOf(cpuID)
		if !ok || named[loc] {
			continue
		}
		if threadsPerCore > 0 && len(threads[loc]) == threadsPerCore {
			continue
		}
		threads[loc] = append(threads[loc], cpuID)
	}
	var cpuIDs []int
	for _, cpus := range threads {
		cpuIDs = append(cpuIDs, cpus...)
	}
	return cpuset.New(cpuIDs...)
}
