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
}

// CPUConfig holds CPU-specific configuration settings.
type CPUConfig struct {
	// CPUSet specifies the cpus to allocate in standard cpuset syntax (e.g., "0-3,5").
	CPUSet string `json:"cpuset,omitempty"`
	// Limit caps the CPU time of a shared request: a quantity above zero, or
	// LimitMax for no cap. Left out, a shared request is capped at its amount.
	Limit *Limit `json:"limit,omitempty"`
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
