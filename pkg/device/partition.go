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

	"k8s.io/utils/cpuset"
)

const (
	// PARTITION_ROLE_RESERVED excludes a partition's CPUs from devices and from
	// every pool, so no container ever runs there.
	PARTITION_ROLE_RESERVED = "reserved"
	// PARTITION_ROLE_DEFAULT publishes devices and hosts the containers that
	// hold no claim.
	PARTITION_ROLE_DEFAULT = "default"
	// PARTITION_ROLE_SHARED is a pool claimed by the workloads that want CPUs
	// near their exclusive ones without owning them.
	PARTITION_ROLE_SHARED = "shared"
	// PARTITION_ROLE_EXCLUSIVE publishes devices and never hosts a container
	// without a claim.
	PARTITION_ROLE_EXCLUSIVE = "exclusive"
)

// DefaultPartitionName names the implicit partition holding every allocatable
// CPU no declared partition claims. It carries no taint, so a claim that knows
// nothing about partitions lands there.
const DefaultPartitionName = "default"

// PartitionTaintKey is the key of the NoSchedule taint every named partition's
// devices carry, so that reaching one takes a toleration naming it and no
// permissive device class can hand those CPUs to a claim that never asked for
// that partition.
const PartitionTaintKey = "dra.cpu/partition"

// Partition is a named set of whole cores on this node, resolved against the
// node's own topology.
type Partition struct {
	Name string
	Role string
	CPUs cpuset.CPUSet
	// ThreadsPerCore is how many online threads per core the operator declared
	// the partition has, and zero when they declared no expectation.
	ThreadsPerCore int
}

// Named reports whether p was declared by an operator, as opposed to being the
// implicit partition of the CPUs nothing else claims.
func (p Partition) Named() bool {
	return p.Name != DefaultPartitionName
}

// PublishesDevices reports whether claims take CPUs from p. A reserved
// partition runs nothing, and a shared one is a pool rather than a set of
// exclusive devices.
func (p Partition) PublishesDevices() bool {
	return p.Role == PARTITION_ROLE_DEFAULT || p.Role == PARTITION_ROLE_EXCLUSIVE
}

// WithImplicitDefault returns partitions followed by the implicit partition
// holding every allocatable CPU none of them claims. It is where a claim naming
// no partition lands, so it exists even when it is empty of CPUs and even when
// nothing was declared at all.
func WithImplicitDefault(partitions []Partition, allocatableCPUs cpuset.CPUSet) []Partition {
	remainder := allocatableCPUs
	for _, partition := range partitions {
		remainder = remainder.Difference(partition.CPUs)
	}
	return append(slices.Clone(partitions), Partition{
		Name: DefaultPartitionName,
		Role: PARTITION_ROLE_DEFAULT,
		CPUs: remainder,
	})
}
