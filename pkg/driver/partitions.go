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
	"fmt"
	"io/fs"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"k8s.io/utils/cpuset"
)

// resolvePartitions checks the declared partitions against the node and
// resolves them. Without declared partitions the node is the one partition
// device.DefaultPartitionName, as upstream's node is.
func resolvePartitions(logger logr.Logger, sysfs fs.ReadLinkFS, topo *cpuinfo.CPUTopology, declared []device.Partition, reservedCPUs cpuset.CPUSet) ([]device.Partition, error) {
	if len(declared) == 0 {
		return device.ResolvePartitions(nil, topo.CPUDetails, reservedCPUs), nil
	}
	presentCPUs, err := cpuinfo.PresentCPUs(sysfs)
	if err != nil {
		return nil, fmt.Errorf("failed to read the present CPUs: %w", err)
	}
	onlineCPUs, err := cpuinfo.OnlineCPUs(sysfs)
	if err != nil {
		return nil, fmt.Errorf("failed to read the online CPUs: %w", err)
	}
	if err := validatePartitions(topo.CPUDetails, presentCPUs, onlineCPUs, declared); err != nil {
		return nil, err
	}
	partitions := device.ResolvePartitions(declared, topo.CPUDetails, reservedCPUs)
	for _, partition := range partitions {
		logger.Info("resolved CPU partition", "partition", partition.Name, "role", partition.Role, "cpus", partition.CPUs.String())
	}
	return partitions, nil
}

// validatePartitions refuses partitions this node cannot hold. The checks that
// need no topology ran when the configuration loaded.
func validatePartitions(details cpuinfo.CPUDetails, presentCPUs, onlineCPUs cpuset.CPUSet, declared []device.Partition) error {
	owner := make(map[cpuinfo.CoreLocation]device.Partition)
	for _, partition := range declared {
		if missing := partition.CPUs.Difference(presentCPUs); !missing.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s, which this node does not have: remove them from its cpuset",
				partition.Name, missing.String())
		}
		if offline := partition.CPUs.Difference(onlineCPUs); !offline.IsEmpty() {
			return fmt.Errorf("partition %q lists offline CPUs %s: bring them online, or remove them from its cpuset",
				partition.Name, offline.String())
		}
		if unmanaged := partition.CPUs.Difference(details.CPUs()); !unmanaged.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s with incomplete topology (the kernel reports no socket, core or NUMA node for them)",
				partition.Name, unmanaged.String())
		}
		for _, cpuID := range partition.CPUs.List() {
			loc, _ := details.CoreOf(cpuID)
			other, named := owner[loc]
			if named && other.Name != partition.Name {
				core := details.SiblingsOf(cpuID)
				return fmt.Errorf("partitions %q and %q split one core between them (%s in %q, %s in %q): put both threads in one partition",
					other.Name, partition.Name, cpuNoun(core.Intersection(other.CPUs)), other.Name, cpuNoun(core.Intersection(partition.CPUs)), partition.Name)
			}
			owner[loc] = partition
		}
	}
	for _, partition := range declared {
		if split := partition.CPUs.Difference(details.CompleteCores(partition.CPUs)); !split.IsEmpty() {
			return fmt.Errorf("partition %q lists CPUs %s without their sibling threads %s: list every thread of each core",
				partition.Name, split.String(), siblingsOutside(details, split, partition.CPUs).String())
		}
	}
	return nil
}

func cpuNoun(cpus cpuset.CPUSet) string {
	if cpus.Size() == 1 {
		return "CPU " + cpus.String()
	}
	return "CPUs " + cpus.String()
}

func siblingsOutside(details cpuinfo.CPUDetails, cpus, inside cpuset.CPUSet) cpuset.CPUSet {
	siblings := cpuset.New()
	for _, cpuID := range cpus.List() {
		siblings = siblings.Union(details.SiblingsOf(cpuID))
	}
	return siblings.Difference(inside)
}

// defaultPartitionCPUs is the CPUs of the entry device.DefaultPartitionName,
// where containers without claims run; a named partition of role default
// takes claims on its devices only.
func defaultPartitionCPUs(partitions []device.Partition) cpuset.CPUSet {
	for _, partition := range partitions {
		if partition.Name == device.DefaultPartitionName {
			return partition.CPUs
		}
	}
	return cpuset.New()
}

// unclaimablePartitionCPUs is every CPU of a partition that keeps its CPUs from
// every workload.
func unclaimablePartitionCPUs(partitions []device.Partition) cpuset.CPUSet {
	unclaimable := cpuset.New()
	for _, partition := range partitions {
		if partition.IsReserved() {
			unclaimable = unclaimable.Union(partition.CPUs)
		}
	}
	return unclaimable
}
