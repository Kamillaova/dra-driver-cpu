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
	"path/filepath"
	"slices"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuallocator"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
)

// groupings are the values groupBy takes. One list, since the validator and
// the generated schema must accept the same set: a value only one of them
// knows is a config file either the driver or the chart refuses.
var groupings = []string{
	device.GROUP_BY_NUMA_NODE,
	device.GROUP_BY_SOCKET,
	device.GROUP_BY_MACHINE,
	device.GROUP_BY_UNCORE_CACHE,
}

// Validate checks that enum fields hold recognised values, that cpuPartitions
// and every profile are well formed, that kubeletRootDir is non-empty and absolute, and that qps
// and burst are positive. The root is required rather than optional because the hostPath
// mounts render from the same value.
// Config files bypass the flag.Value validators, so Resolve calls this after merging.
func (c Config) Validate() error {
	if c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED && c.CPUDeviceMode != device.CPU_DEVICE_MODE_INDIVIDUAL {
		return fmt.Errorf("invalid cpuDeviceMode %q: must be %q or %q",
			c.CPUDeviceMode, device.CPU_DEVICE_MODE_GROUPED, device.CPU_DEVICE_MODE_INDIVIDUAL)
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && !slices.Contains(groupings, c.GroupBy) {
		return fmt.Errorf("invalid groupBy %q: must be one of %q", c.GroupBy, groupings)
	}
	if c.Allocator != cpuallocator.ModeExternal && c.Allocator != cpuallocator.ModeCPUManager {
		return fmt.Errorf("invalid allocator %q: must be %q or %q",
			c.Allocator, cpuallocator.ModeExternal, cpuallocator.ModeCPUManager)
	}
	if err := c.validateCPUPartitions(); err != nil {
		return err
	}
	if err := c.validateProfiles(); err != nil {
		return err
	}
	if c.CPUDeviceMode == device.CPU_DEVICE_MODE_GROUPED && c.GroupBy == device.GROUP_BY_MACHINE && c.Allocator != cpuallocator.ModeExternal {
		return fmt.Errorf("invalid allocator %q with groupBy %q: must be %q", c.Allocator, device.GROUP_BY_MACHINE, cpuallocator.ModeExternal)
	}
	// In individual mode the scheduler, not the driver, picks every CPU a claim gets.
	if c.FullPhysicalCPUsOnly && c.CPUDeviceMode != device.CPU_DEVICE_MODE_GROUPED {
		return fmt.Errorf("invalid fullPhysicalCPUsOnly with cpuDeviceMode %q: requires cpuDeviceMode %q",
			c.CPUDeviceMode, device.CPU_DEVICE_MODE_GROUPED)
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
	// client-go would read a zero qps as its default, 5, and a negative one as
	// no rate limit, and it refuses a burst below 1 only when it builds the
	// client.
	if c.QPS <= 0 {
		return fmt.Errorf("invalid qps %v: must be greater than 0", c.QPS)
	}
	if c.Burst < 1 {
		return fmt.Errorf("invalid burst %d: must be at least 1", c.Burst)
	}
	return nil
}
