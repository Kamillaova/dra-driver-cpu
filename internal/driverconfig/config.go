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

	"sigs.k8s.io/yaml"
)

// ConfigAPIVersion is the version validated in config files.
const ConfigAPIVersion = "v1alpha1"

// Config holds the driver runtime configuration.
type Config struct {
	Kubeconfig       string `json:"kubeconfig,omitempty"`
	HostnameOverride string `json:"hostnameOverride,omitempty"`
	BindAddress      string `json:"bindAddress,omitempty"`
	ReservedCPUs     string `json:"reservedCPUs,omitempty"`
	CPUDeviceMode    string `json:"cpuDeviceMode,omitempty"`
	GroupBy          string `json:"groupBy,omitempty"`
	ExposePCIeRoots  bool   `json:"exposePCIeRoots,omitempty"`
	SysFSOverlay     string `json:"sysfsOverlay,omitempty"`
	// KubeletRootDir is the kubelet root directory. The plugin registration and
	// plugins directories are derived from it as <root>/plugins_registry and
	// <root>/plugins. Set it when the kubelet --root-dir is not the default
	// /var/lib/kubelet.
	KubeletRootDir string `json:"kubeletRootDir,omitempty"`
	// PublishNodeAllocatableResourceMapping publishes KEP-5517 nodeAllocatableResources
	// mappings in ResourceSlice devices. Requires the DRANodeAllocatableResources
	// feature gate in the cluster. Defaults to false.
	PublishNodeAllocatableResourceMapping bool `json:"publishNodeAllocatableResourceMapping,omitempty"`
	// FullPhysicalCPUsOnly allocates whole physical cores, so a core's SMT
	// siblings are never split between two claims or between a claim and the
	// shared pool. This is the equivalent of the kubelet CPU Manager's
	// FullPCPUsOnly policy option. Defaults to false, leaving single-thread
	// allocations possible; it is a no-op where SMT is disabled.
	FullPhysicalCPUsOnly bool `json:"fullPhysicalCPUsOnly,omitempty"`
	// AssumeUnsolicitedUpdatesSafe permits the driver to push container updates
	// the runtime did not ask for. Every such feature requires it.
	//
	// It is an operator assertion rather than something the driver can detect:
	// unsolicited updates deadlock runtimes whose vendored NRI predates
	// containerd/nri#301, and the NRI Configure handshake reports the runtime
	// version but not its NRI version, so no reliable check exists. containerd
	// carries the fix from NRI v0.12.1, released in containerd v2.4.0-beta.0;
	// containerd v2.3.4 and CRI-O v1.36 do not. Defaults to false.
	AssumeUnsolicitedUpdatesSafe bool `json:"assumeUnsolicitedUpdatesSafe,omitempty"`
	// ReconcileSharedOnUnprepare widens shared containers onto the CPUs a claim
	// released as soon as it is unprepared, instead of leaving them on the
	// narrower cpuset until their next lifecycle event. Requires
	// AssumeUnsolicitedUpdatesSafe, and is inert without it. Defaults to true.
	ReconcileSharedOnUnprepare bool `json:"reconcileSharedOnUnprepare,omitempty"`
	// DefragEnabled lets the driver move a running claim onto different CPUs to
	// recover uncore cache alignment lost to claim churn, without restarting its
	// container. The claim keeps its CPU count; only which CPUs back it change.
	//
	// Requires cpuDeviceMode "grouped" with groupBy "numanode" or "socket", since
	// the driver only chooses a claim's CPUs in those modes, and
	// AssumeUnsolicitedUpdatesSafe, since a move is pushed to the runtime
	// unprompted. Defaults to false.
	DefragEnabled bool `json:"defragEnabled,omitempty"`
}

// LogValues returns key-value pairs for structured logging of the config.
func (c Config) LogValues() []any {
	return []any{
		"kubeconfig", c.Kubeconfig,
		"bindAddress", c.BindAddress,
		"cpuDeviceMode", c.CPUDeviceMode,
		"groupBy", c.GroupBy,
		"reservedCPUs", c.ReservedCPUs,
		"hostnameOverride", c.HostnameOverride,
		"exposePCIeRoots", c.ExposePCIeRoots,
		"sysfsOverlay", c.SysFSOverlay,
		"kubeletRootDir", c.KubeletRootDir,
		"publishNodeAllocatableResourceMapping", c.PublishNodeAllocatableResourceMapping,
		"fullPhysicalCPUsOnly", c.FullPhysicalCPUsOnly,
		"assumeUnsolicitedUpdatesSafe", c.AssumeUnsolicitedUpdatesSafe,
		"reconcileSharedOnUnprepare", c.ReconcileSharedOnUnprepare,
		"defragEnabled", c.DefragEnabled,
	}
}

// dumpConfig mirrors Config field-for-field but drops the omitempty json
// tags, so Dump also prints zero values (e.g. exposePCIeRoots=false).
type dumpConfig struct {
	Kubeconfig                            string `json:"kubeconfig"`
	HostnameOverride                      string `json:"hostnameOverride"`
	BindAddress                           string `json:"bindAddress"`
	ReservedCPUs                          string `json:"reservedCPUs"`
	CPUDeviceMode                         string `json:"cpuDeviceMode"`
	GroupBy                               string `json:"groupBy"`
	ExposePCIeRoots                       bool   `json:"exposePCIeRoots"`
	SysFSOverlay                          string `json:"sysfsOverlay"`
	KubeletRootDir                        string `json:"kubeletRootDir"`
	PublishNodeAllocatableResourceMapping bool   `json:"publishNodeAllocatableResourceMapping"`
	FullPhysicalCPUsOnly                  bool   `json:"fullPhysicalCPUsOnly"`
	AssumeUnsolicitedUpdatesSafe          bool   `json:"assumeUnsolicitedUpdatesSafe"`
	ReconcileSharedOnUnprepare            bool   `json:"reconcileSharedOnUnprepare"`
	DefragEnabled                         bool   `json:"defragEnabled"`
}

// Dump renders the Config as YAML, for logging a human-readable snapshot of
// the fully loaded configuration. Zero values are included, unlike
// marshalling Config directly, since they reflect real runtime state.
func (c Config) Dump() string {
	out, err := yaml.Marshal(dumpConfig(c))
	if err != nil {
		return fmt.Sprintf("<!!! FAILED TO MARSHAL Config: %v !!!>", err)
	}
	return string(out)
}
