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

package allocation

import (
	"fmt"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/cpuset"
	"sigs.k8s.io/json"
)

// Config is one opaque configuration as the driver reads it.
type Config struct {
	// CPUs are the CPUs the configuration names, and HasCPUs whether it names
	// any: a cpuset left out or empty is not a request for none.
	CPUs    cpuset.CPUSet
	HasCPUs bool
	// Limit is the CPU limit as written, nil where the configuration sets none.
	Limit *v1alpha1.Limit
}

// ParseOpaqueConfig decodes the raw opaque parameters into the driver's internal format
// based on the apiVersion. Whether the configuration must name CPUs is left to
// the caller that needs them.
func ParseOpaqueConfig(raw []byte) (Config, error) {
	var config v1alpha1.OpaqueConfig
	strictErrs, err := json.UnmarshalStrict(raw, &config)
	if err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal opaque config: %w", err)
	}
	if len(strictErrs) > 0 {
		return Config{}, fmt.Errorf("opaque config: %w", runtime.NewStrictDecodingError(strictErrs))
	}

	switch config.APIVersion {
	case v1alpha1.APIVersion:
		parsed := Config{Limit: config.CPUConfig.Limit}
		if config.CPUConfig.CPUSet == "" {
			return parsed, nil
		}
		parsedSet, err := cpuset.Parse(config.CPUConfig.CPUSet)
		if err != nil {
			return Config{}, fmt.Errorf("opaque config: failed to parse cpuset %q: %w", config.CPUConfig.CPUSet, err)
		}
		parsed.CPUs, parsed.HasCPUs = parsedSet, true
		return parsed, nil

	default:
		return Config{}, fmt.Errorf("unsupported opaque config apiVersion: %q", config.APIVersion)
	}
}
