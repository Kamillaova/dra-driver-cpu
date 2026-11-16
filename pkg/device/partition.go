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
