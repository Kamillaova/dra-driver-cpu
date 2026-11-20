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

package v1alpha1

import (
	"encoding/json"
	"fmt"
)

const (
	APIVersion = "v1alpha1"
)

// OpaqueConfig is the versioned configuration schema for v1alpha1.
//
// This struct defines the schema of the custom allocation configuration that goes into
// the ResourceClaim status at: claim.Status.Allocation.Devices.Config[*].Opaque.Parameters.Raw.
//
// This configuration is opaque to Kubernetes, which passes it directly as raw JSON bytes to be
// parsed by this driver.
type OpaqueConfig struct {
	// APIVersion specifies the schema version. Should be "v1alpha1".
	APIVersion string `json:"apiVersion"`
	// CPUConfig holds CPU-specific configuration settings.
	CPUConfig CPUConfig `json:"cpuConfig"`
	// CgroupConfig holds the cgroup layout of the container using the claim.
	CgroupConfig CgroupConfig `json:"cgroupConfig"`
}

// CgroupConfig holds the cgroup layout of the container using the claim. It
// applies to the whole claim.
type CgroupConfig struct {
	// Threaded splits the container's cgroup into threaded groups.
	Threaded ThreadedConfig `json:"threaded"`
}

// ThreadedConfig splits the cgroup of the container using the claim into
// threaded groups, one cgroup each, whose threads the workload places itself.
// Its zero value is the block left out.
type ThreadedConfig struct {
	// Enabled switches the split on. Left out, the block changes nothing.
	Enabled bool `json:"enabled,omitempty"`
	// Place is the group the container's process starts in, on a node whose
	// cgroupThreadedMode is place.
	Place string `json:"place,omitempty"`
	// Groups is the complete list of groups. Left out, each request gets a
	// group named after it; an empty list is refused.
	Groups []ThreadedGroup `json:"groups,omitempty"`
}

// IsZero reports whether the block is left out or written with zero values.
func (c ThreadedConfig) IsZero() bool {
	return !c.Enabled && c.Place == "" && c.Groups == nil
}

// ThreadedGroup is one threaded group and the requests, or alternatives of a
// firstAvailable request, whose CPUs it holds.
type ThreadedGroup struct {
	Name     string   `json:"name"`
	Requests []string `json:"requests"`
}

// CPUConfig holds CPU-specific configuration settings.
type CPUConfig struct {
	// CPUSet specifies the cpus to allocate in standard cpuset syntax (e.g., "0-3,5").
	CPUSet string `json:"cpuset,omitempty"`
	// Limit caps the CPU time of a shared request: a quantity above zero, or
	// LimitMax for no cap. Left out, a shared request is capped at its amount.
	Limit *Limit `json:"limit,omitempty"`
	// Exclusive asks, on a partition of role default that allows it, for CPUs
	// of the request's own rather than a share. Left out, a request is shared
	// on a node with partitions and exclusive on one without.
	Exclusive *bool `json:"exclusive,omitempty"`
}

// ClaimPlacementStatusKind is the kind of ClaimPlacementStatus.
const ClaimPlacementStatusKind = "ClaimPlacementStatus"

// ClaimPlacementStatus is the data of the entry the driver writes in a
// ResourceClaim's status for each of its allocation results:
// claim.Status.Devices[*].Data.
type ClaimPlacementStatus struct {
	// APIVersion specifies the schema version. Should be "v1alpha1".
	APIVersion string `json:"apiVersion"`
	// Kind is ClaimPlacementStatusKind.
	Kind string `json:"kind"`
	// Partition is the CPU partition of the result's device, "default" on a
	// node without partitions.
	Partition string `json:"partition"`
	// CPUSet is the CPUs an exclusive request holds on the device. A share's
	// entry has none, since the CPUs it runs on change after it is prepared.
	CPUSet string `json:"cpuset,omitempty"`
	// Group is the threaded group the result's CPUs land in, in a claim that
	// sets cgroupConfig.threaded.enabled.
	Group string `json:"group,omitempty"`
}

// LimitMax is the one way to write that a request has no CPU limit.
const LimitMax Limit = "max"

// Limit is a CPU limit as written: a Kubernetes quantity, given as a JSON
// number or string, or LimitMax.
type Limit string

// UnmarshalJSON keeps a string's content or a number's text as written and
// parses neither, so that a value that is no quantity is refused by the
// caller, which knows the request it belongs to.
func (l *Limit) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		*l = Limit(text)
		return nil
	}
	var number json.Number
	if err := json.Unmarshal(data, &number); err != nil {
		return fmt.Errorf("limit %s is neither a quantity nor %q", data, LimitMax)
	}
	*l = Limit(number)
	return nil
}
