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
