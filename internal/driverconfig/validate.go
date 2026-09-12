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
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
)

// Validate checks that enum fields hold recognised values and that
// kubeletRootDir is non-empty and absolute. The root is required rather than
// optional because the hostPath mounts render from the same value.
// Config files bypass the flag.Value validators, so Resolve calls this after merging.
func (c Config) Validate() error {
	if c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED && c.CPUDeviceMode != device.CPU_DEVICE_MODE_INDIVIDUAL {
		return fmt.Errorf("invalid cpuDeviceMode %q: must be %q or %q",
			c.CPUDeviceMode, device.CPU_DEVICE_MODE_GROUPED, device.CPU_DEVICE_MODE_INDIVIDUAL)
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && !slices.Contains(groupings, c.GroupBy) {
		return fmt.Errorf("invalid groupBy %q: must be one of %q", c.GroupBy, groupings)
	}
	if c.Allocator != AllocatorExternal && c.Allocator != AllocatorCPUManager {
		return fmt.Errorf("invalid allocator %q: must be %q or %q",
			c.Allocator, AllocatorExternal, AllocatorCPUManager)
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && c.GroupBy == device.GROUP_BY_MACHINE && c.Allocator != AllocatorExternal {
		return fmt.Errorf("invalid allocator %q with groupBy %q: must be %q", c.Allocator, device.GROUP_BY_MACHINE, AllocatorExternal)
	}
	// Whole-core allocation is only the driver's to enforce when the driver
	// chooses which CPUs back a claim. In individual mode the scheduler picks
	// exact per-CPU devices, so the driver cannot keep a core's siblings
	// together and silently ignoring the option would be worse than refusing it.
	if c.FullPhysicalCPUsOnly && c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED {
		return fmt.Errorf("invalid fullPhysicalCPUsOnly: requires cpuDeviceMode %q, got %q",
			device.CPU_DEVICE_MODE_GROUPED, c.CPUDeviceMode)
	}
	// The kubelet root becomes socket and mount locations, so a relative path
	// would resolve against the working directory and silently break
	// registration.
	//
	// Empty is refused rather than defaulted. A tunable that takes a value
	// should be given one, and the way this one arrives empty in practice is a
	// chart that rendered nothing into it -- in which case the hostPath mounts
	// rendered from the same value are wrong too, and quietly falling back to
	// the standard root is how the driver would end up registering somewhere
	// the kubelet is not watching. That is the failure this flag exists for.
	if c.KubeletRootDir == "" {
		return fmt.Errorf("invalid kubeletRootDir: must not be empty")
	}
	if !filepath.IsAbs(c.KubeletRootDir) {
		return fmt.Errorf("invalid kubeletRootDir %q: must be an absolute path", c.KubeletRootDir)
	}
	if err := c.validateDefrag(); err != nil {
		return err
	}
	if err := c.validateCachePlacementStrategy(); err != nil {
		return err
	}
	if err := c.validateCPUPartitions(); err != nil {
		return err
	}
	if err := c.validateProfiles(); err != nil {
		return err
	}
	return nil
}

func (c Config) validateCachePlacementStrategy() error {
	switch coreselect.Policy(c.CachePlacementStrategy) {
	case coreselect.Pack:
	case coreselect.Spread:
		// The policy governs which CPUs the driver picks, so it means nothing
		// where the driver does not pick: in individual mode the scheduler
		// names exact CPU devices.
		if c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED {
			return fmt.Errorf("invalid cachePlacementStrategy %q: requires cpuDeviceMode %q, got %q",
				c.CachePlacementStrategy, device.CPU_DEVICE_MODE_GROUPED, c.CPUDeviceMode)
		}
	default:
		return fmt.Errorf("invalid cachePlacementStrategy %q: must be %q or %q", c.CachePlacementStrategy, coreselect.Pack, coreselect.Spread)
	}
	return nil
}

func (c Config) validateDefrag() error {
	if !c.DefragEnabled {
		return nil
	}
	// Defragmentation moves claims the driver placed itself. In individual mode
	// the scheduler picks the exact CPU devices, and with groupBy "machine" the
	// cpuset comes from the claim's own opaque config, so in both cases the
	// placement is not the driver's to change.
	if c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED {
		return fmt.Errorf("invalid defragEnabled: requires cpuDeviceMode %q, got %q",
			device.CPU_DEVICE_MODE_GROUPED, c.CPUDeviceMode)
	}
	// Grouping by uncore cache is the case where the driver does choose the CPUs
	// and still may not move them: a device is one cache there, so a claim moved
	// to another cache no longer sits on the device its allocation names, and the
	// scheduler's per-device accounting stops describing the node.
	if c.GroupBy == device.GROUP_BY_UNCORE_CACHE {
		return fmt.Errorf("invalid defragEnabled: with groupBy %q a device is one uncore cache, so a move between caches would leave the device the claim was allocated on",
			device.GROUP_BY_UNCORE_CACHE)
	}
	if c.GroupBy != device.GROUP_BY_NUMA_NODE && c.GroupBy != device.GROUP_BY_SOCKET {
		return fmt.Errorf("invalid defragEnabled: requires groupBy %q or %q, got %q",
			device.GROUP_BY_NUMA_NODE, device.GROUP_BY_SOCKET, c.GroupBy)
	}
	// A move is a container update the runtime did not ask for, which is exactly
	// what that option asserts is safe here.
	if !c.AssumeUnsolicitedUpdatesSafe {
		return fmt.Errorf("invalid defragEnabled: requires assumeUnsolicitedUpdatesSafe")
	}
	return nil
}

// validateProfiles checks the profiles themselves and the scope rule around
// them: once one node type describes its cores, every node type describes its
// own, and a description outside a profile has no node it belongs to.
func (c Config) validateProfiles() error {
	if len(c.Profiles) == 0 {
		return nil
	}
	if c.ReservedCPUs != "" {
		return fmt.Errorf("invalid reservedCPUs %q: with profiles declared, the CPUs a node keeps from workloads are a partition with role %q inside that node's own profile",
			c.ReservedCPUs, device.PARTITION_ROLE_RESERVED)
	}
	if len(c.CPUPartitions) > 0 {
		return fmt.Errorf("invalid cpuPartitions: with profiles declared, the partitions belong to the profiles, one complete description per node type")
	}
	for _, name := range slices.Sorted(maps.Keys(c.Profiles)) {
		if name == DefaultProfileName {
			return fmt.Errorf("invalid profile %q: it names the implicit profile of a node whose cores are all interchangeable, which is never declared and is selected with the label %s=%s",
				name, ProfileLabel, DefaultProfileName)
		}
		if len(c.Profiles[name].CPUPartitions) == 0 {
			return fmt.Errorf("invalid profile %q: it describes no cores; a node type that carves out none is labelled %s=%s instead",
				name, ProfileLabel, DefaultProfileName)
		}
		if _, err := c.asProfile(name, c.Profiles[name]); err != nil {
			return err
		}
	}
	return nil
}
