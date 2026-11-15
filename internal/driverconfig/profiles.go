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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuallocator"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"k8s.io/apimachinery/pkg/api/validate/content"
)

// ProfileLabel is the node label whose value names the profile the node runs.
const ProfileLabel = "dra.cpu/profile"

// Profile is the part of the configuration that differs between kinds of
// node: how the node's CPUs are partitioned.
type Profile struct {
	CPUPartitions []CPUPartition `json:"cpuPartitions,omitempty"`
}

// WithProfile returns the configuration a node runs when its ProfileLabel
// label holds name: the named profile's partitions in place of the fleet-wide
// list. Without profiles the label selects nothing and c is returned as it is.
// With profiles, a node without the label or naming a profile the
// configuration does not declare is an error, never a fallback onto another
// kind of node's partitions.
func (c Config) WithProfile(name string) (Config, error) {
	if len(c.Profiles) == 0 {
		return c, nil
	}
	declared := slices.Sorted(maps.Keys(c.Profiles))
	if name == "" {
		return Config{}, fmt.Errorf("node has no %s label; label it with one of the declared profiles %q", ProfileLabel, declared)
	}
	profile, ok := c.Profiles[name]
	if !ok {
		return Config{}, fmt.Errorf("node label %s=%s names no declared profile; declared: %q", ProfileLabel, name, declared)
	}
	return c.asProfile(name, profile)
}

// asProfile returns c with p's partitions in place of its own, revalidated.
// Selecting a profile and checking every profile share it, so the two cannot
// disagree on what a profile means.
func (c Config) asProfile(name string, p Profile) (Config, error) {
	c.CPUPartitions = p.CPUPartitions
	c.Profiles = nil
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("profile %q: %w", name, err)
	}
	return c, nil
}

// validateProfiles checks every profile, not only the one a node selects, so a
// broken profile stops every node at rollout rather than one kind of node at
// its next restart.
func (c Config) validateProfiles() error {
	if len(c.Profiles) == 0 {
		return nil
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && c.GroupBy == device.GROUP_BY_MACHINE {
		return fmt.Errorf("invalid groupBy %q with profiles: needs allocator %q, which partitions do not use",
			device.GROUP_BY_MACHINE, cpuallocator.ModeExternal)
	}
	if c.Allocator == cpuallocator.ModeExternal {
		return fmt.Errorf("invalid allocator %q with profiles: must be %q", c.Allocator, cpuallocator.ModeCPUManager)
	}
	if c.ReservedCPUs != "" {
		return fmt.Errorf("invalid reservedCPUs %q: with profiles declared, the CPUs a node keeps from workloads are a partition with role %q inside that node's own profile",
			c.ReservedCPUs, device.PARTITION_ROLE_RESERVED)
	}
	if len(c.CPUPartitions) > 0 {
		return errors.New("invalid cpuPartitions: with profiles declared, the partitions belong to the profiles, one complete description per node type")
	}

	type declaration struct{ profile, role string }
	// A partition's name is its device class on every node, so a role that
	// changed with the profile would make one class owned on some nodes and
	// shared on others.
	roles := map[string]declaration{}
	for _, name := range slices.Sorted(maps.Keys(c.Profiles)) {
		profile := c.Profiles[name]
		if err := validateProfileName(name); err != nil {
			return err
		}
		if len(profile.CPUPartitions) == 0 {
			return fmt.Errorf("profile %q has no partitions; add \"- name: %s\" to its cpuPartitions", name, device.DefaultPartitionName)
		}
		if _, err := c.asProfile(name, profile); err != nil {
			return err
		}
		for _, partition := range profile.CPUPartitions {
			if partition.Name == device.DefaultPartitionName {
				continue
			}
			first, seen := roles[partition.Name]
			if !seen {
				roles[partition.Name] = declaration{profile: name, role: partition.Role}
				continue
			}
			if first.role != partition.Role {
				return fmt.Errorf("partition %q has role %q in profile %q and %q in profile %q; a partition keeps one role in every profile",
					partition.Name, first.role, first.profile, partition.Role, name)
			}
		}
	}
	return nil
}

// validateProfileName refuses a name no node can select. The empty name is a
// label value, but an empty label reads as no label.
func validateProfileName(name string) error {
	if name == "" {
		return fmt.Errorf("invalid profile name %q: a node selects its profile by the value of its %s label, and an empty label selects no profile",
			name, ProfileLabel)
	}
	if reasons := content.IsLabelValue(name); len(reasons) > 0 {
		return fmt.Errorf("invalid profile name %q: a node selects its profile by the value of its %s label, and this name is not a label value: %s",
			name, ProfileLabel, strings.Join(reasons, "; "))
	}
	return nil
}
