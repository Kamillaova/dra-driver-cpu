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

package driverconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuallocator"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/cpuset"
)

// CPUPartition is one named set of whole cores on a node: which CPUs it holds,
// what it is for, and how many threads per core it expects to find online. The
// entry named device.DefaultPartitionName takes neither a role nor a cpuset.
type CPUPartition struct {
	Name   string `json:"name"`
	Role   string `json:"role,omitempty"`
	CPUSet string `json:"cpuset,omitempty"`
	SMT    *SMT   `json:"smt,omitempty"`
	// AllowExclusive lets a request on a partition of role default take
	// exclusive CPUs out of it. Defaults to false.
	AllowExclusive bool `json:"allowExclusive,omitempty"`
}

// SMT is how many threads per core a partition expects the platform to leave
// online: true for as many as the hardware has, false for one, or an explicit
// upper bound. The driver verifies the expectation and never changes hotplug
// state itself.
type SMT struct {
	// Threads is the highest number of online threads per core the partition
	// accepts, and zero when it accepts whatever the platform provides.
	Threads int `json:"-"`
}

func (s SMT) MarshalJSON() ([]byte, error) {
	switch s.Threads {
	case 0:
		return []byte("true"), nil
	case 1:
		return []byte("false"), nil
	default:
		return []byte(strconv.Itoa(s.Threads)), nil
	}
}

func (s *SMT) UnmarshalJSON(data []byte) error {
	var native bool
	if err := json.Unmarshal(data, &native); err == nil {
		if native {
			s.Threads = 0
		} else {
			s.Threads = 1
		}
		return nil
	}
	var threads int
	if err := json.Unmarshal(data, &threads); err != nil {
		return fmt.Errorf("invalid smt %s: must be true, false or a thread count", data)
	}
	if threads < 1 {
		return fmt.Errorf("invalid smt %d: a core has at least one online thread", threads)
	}
	s.Threads = threads
	return nil
}

// PartitionsUsed reports whether the configuration uses CPU partitions, which
// it does exactly when cpuPartitions holds the entry named
// device.DefaultPartitionName.
func (c Config) PartitionsUsed() bool {
	return slices.ContainsFunc(c.CPUPartitions, func(p CPUPartition) bool {
		return p.Name == device.DefaultPartitionName
	})
}

// Partitions returns the declared partitions in the form the driver resolves
// against the node, and nil where the configuration uses no partitions.
func (c Config) Partitions() ([]device.Partition, error) {
	if !c.PartitionsUsed() {
		return nil, nil
	}
	partitions := make([]device.Partition, 0, len(c.CPUPartitions))
	for _, partition := range c.CPUPartitions {
		cpus, err := cpuset.Parse(partition.CPUSet)
		if err != nil {
			return nil, fmt.Errorf("invalid cpuset %q of partition %q: %w", partition.CPUSet, partition.Name, err)
		}
		threads := 0
		if partition.SMT != nil {
			threads = partition.SMT.Threads
		}
		partitions = append(partitions, device.Partition{
			Name:           partition.Name,
			Role:           partition.Role,
			CPUs:           cpus,
			ThreadsPerCore: threads,
			AllowExclusive: partition.AllowExclusive,
		})
	}
	return partitions, nil
}

// partitionNameMaxLength keeps every device name the driver builds from a
// partition a DNS label: a device name may be 63 characters and the longest
// prefix the driver prepends is "cpudevsocket000-".
const partitionNameMaxLength = 46

// validateCPUPartitions runs the checks that need no knowledge of the node the
// driver stands on. Whether the CPUs a partition names exist, are online, or
// form whole cores is decided against the topology in the driver.
func (c Config) validateCPUPartitions() error {
	if len(c.CPUPartitions) == 0 {
		return nil
	}
	if !c.PartitionsUsed() {
		return fmt.Errorf("cpuPartitions names partitions but no partition %q: add `- name: %s` to cpuPartitions to use partitions",
			device.DefaultPartitionName, device.DefaultPartitionName)
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && c.GroupBy == device.GROUP_BY_MACHINE {
		return fmt.Errorf("invalid groupBy %q with cpuPartitions: needs allocator %q, which partitions do not use",
			device.GROUP_BY_MACHINE, cpuallocator.ModeExternal)
	}
	if c.Allocator == cpuallocator.ModeExternal {
		return fmt.Errorf("invalid allocator %q with cpuPartitions: must be %q", c.Allocator, cpuallocator.ModeCPUManager)
	}
	// The partitions describe every allocatable CPU of the node, which is the
	// grouped device's unit; individual mode publishes one device per CPU and
	// the scheduler names them directly.
	if c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED {
		return fmt.Errorf("invalid cpuPartitions: requires cpuDeviceMode %q, got %q",
			device.CPU_DEVICE_MODE_GROUPED, c.CPUDeviceMode)
	}
	// Two descriptions of the same CPUs in one scope, with no rule saying which
	// wins.
	if c.ReservedCPUs != "" {
		return reservedCPUsReplaced(c.ReservedCPUs)
	}

	byName := make(map[string]cpuset.CPUSet, len(c.CPUPartitions))
	var order []string
	for _, partition := range c.CPUPartitions {
		if err := validatePartitionName(partition.Name); err != nil {
			return err
		}
		if _, duplicate := byName[partition.Name]; duplicate {
			return fmt.Errorf("invalid cpuPartitions: partition %q is declared twice", partition.Name)
		}
		if partition.Name == device.DefaultPartitionName {
			if partition.CPUSet != "" {
				return fmt.Errorf("partition %q: cpuset is filled in automatically; remove it", partition.Name)
			}
			if partition.Role != "" {
				return fmt.Errorf("partition %q: role is always %s; remove it", partition.Name, device.PARTITION_ROLE_DEFAULT)
			}
			byName[partition.Name] = cpuset.New()
			continue
		}
		if !device.IsPartitionRole(partition.Role) {
			return fmt.Errorf("invalid role %q of partition %q: must be one of %q", partition.Role, partition.Name, device.PartitionRoles())
		}
		// Only a shared device leaves a request a choice between a share and
		// CPUs of its own.
		if shared := (device.Partition{Role: partition.Role}).PublishesSharedDevices(true); partition.AllowExclusive && !shared {
			return fmt.Errorf("partition %q has role %q, where allowExclusive does not apply; remove it", partition.Name, partition.Role)
		}
		cpus, err := cpuset.Parse(partition.CPUSet)
		if err != nil {
			return fmt.Errorf("invalid cpuset %q of partition %q: %w", partition.CPUSet, partition.Name, err)
		}
		if cpus.IsEmpty() {
			return fmt.Errorf("invalid cpuset %q of partition %q: names no CPUs", partition.CPUSet, partition.Name)
		}
		for _, other := range order {
			if overlap := cpus.Intersection(byName[other]); !overlap.IsEmpty() {
				return fmt.Errorf("partitions %q and %q both claim %s: a CPU belongs to one partition",
					other, partition.Name, overlap.String())
			}
		}
		byName[partition.Name] = cpus
		order = append(order, partition.Name)
	}
	return nil
}

func reservedCPUsReplaced(reservedCPUs string) error {
	return fmt.Errorf("invalid reservedCPUs %q: partitions replace it; declare a partition with role %q instead",
		reservedCPUs, device.PARTITION_ROLE_RESERVED)
}

func validatePartitionName(name string) error {
	if name == "" {
		return errors.New("invalid cpuPartitions: a partition has no name")
	}
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid partition name %q: %s", name, strings.Join(errs, "; "))
	}
	if len(name) > partitionNameMaxLength {
		return fmt.Errorf("invalid partition name %q: at most %d characters, so that the device names built from it stay DNS labels", name, partitionNameMaxLength)
	}
	return nil
}
