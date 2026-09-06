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

package api

import (
	"encoding/json"
	"fmt"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"k8s.io/utils/cpuset"
)

// ClaimPlacement is what a claim says about its own placement: facts about the
// claim, not about any one of its requests, so a driver folding several of a
// claim's configurations expects them to agree on these.
type ClaimPlacement struct {
	// Relocatable is whether the driver may change the claim's CPUs while its
	// containers run.
	Relocatable bool
	// Alignment is what the claim asks about landing split, and AlignmentSet
	// whether the claim asked at all. The two differ where the claim's requests
	// leave the allocator no choice of shape, since asking there is an error
	// and the default is not.
	Alignment    v1alpha1.Alignment
	AlignmentSet bool
}

// ClaimConfig is one opaque configuration as a driver reads it, with the
// defaults of its version applied.
type ClaimConfig struct {
	ClaimPlacement
	// CPUs are the CPUs the configuration named, and HasCPUs whether it named
	// any at all: an absent cpuset is not a request for none.
	CPUs    cpuset.CPUSet
	HasCPUs bool
}

// ParseOpaqueConfig decodes the raw opaque parameters into the driver's internal format
// based on the apiVersion.
//
// CCX-FORK: upstream returns the cpuset alone, and refuses a configuration that
// carries none. A claim now states two more things about its own placement, so
// the whole configuration is returned and whether it named a cpuset is left to
// the caller that needs one.
func ParseOpaqueConfig(raw []byte) (ClaimConfig, error) {
	var config v1alpha1.OpaqueConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		return ClaimConfig{}, fmt.Errorf("failed to unmarshal opaque config: %w", err)
	}

	switch config.APIVersion {
	case v1alpha1.APIVersion:
		return parseV1Alpha1(config.CPUConfig)

	default:
		return ClaimConfig{}, fmt.Errorf("unsupported opaque config apiVersion: %q", config.APIVersion)
	}
}

func parseV1Alpha1(cpuConfig v1alpha1.CPUConfig) (ClaimConfig, error) {
	parsed := ClaimConfig{ClaimPlacement: ClaimPlacement{
		Relocatable:  cpuConfig.Relocatable,
		Alignment:    cpuConfig.Alignment,
		AlignmentSet: cpuConfig.Alignment != "",
	}}

	switch parsed.Alignment {
	case "":
		parsed.Alignment = v1alpha1.AlignmentBestEffort
	case v1alpha1.AlignmentBestEffort:
	case v1alpha1.AlignmentRepairable:
		// Making a split claim whole means moving its own CPUs, so a claim
		// asking for that has to accept being moved.
		if !parsed.Relocatable {
			return ClaimConfig{}, fmt.Errorf("opaque config: cpuConfig.alignment %q requires cpuConfig.relocatable", parsed.Alignment)
		}
	default:
		return ClaimConfig{}, fmt.Errorf("opaque config: unsupported cpuConfig.alignment %q, must be %q or %q",
			parsed.Alignment, v1alpha1.AlignmentBestEffort, v1alpha1.AlignmentRepairable)
	}

	if cpuConfig.CPUSet == "" {
		return parsed, nil
	}
	// Naming the CPUs and permitting them to change are contradictory: the
	// named set is what the claim is for, and the driver would be free to leave
	// it. Refused whatever the node's grouping, so that a template saying both
	// fails where it is written rather than only where a cpuset is honoured.
	if parsed.Relocatable {
		return ClaimConfig{}, fmt.Errorf("opaque config: cpuConfig.cpuset %q cannot be combined with cpuConfig.relocatable", cpuConfig.CPUSet)
	}
	cpus, err := cpuset.Parse(cpuConfig.CPUSet)
	if err != nil {
		return ClaimConfig{}, fmt.Errorf("opaque config: failed to parse cpuset %q: %w", cpuConfig.CPUSet, err)
	}
	parsed.CPUs, parsed.HasCPUs = cpus, true
	return parsed, nil
}
