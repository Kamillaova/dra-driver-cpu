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
	"slices"

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
	// CachePlacementStrategy is how a claim that fits inside one uncore cache
	// chooses among the caches that can hold it. "pack" (the default) fills
	// the fullest cache that fits, keeping clean caches whole for larger
	// claims; "spread" fills the emptiest, giving each claim a cache of its
	// own while there is slack and relying on defragmentation to consolidate
	// the small tenants when a whole-cache claim actually arrives. Without
	// fullPhysicalCPUsOnly, spread still avoids splitting a physical core
	// where it can, but two claims may end up on one core's siblings once a
	// cache offers nothing better -- whole-core allocation is what actually
	// forbids that.
	CachePlacementStrategy string `json:"cachePlacementStrategy,omitempty"`
	// CPUPartitions describes the node's cores as named partitions: which CPUs
	// each holds, whether workloads may run there and how, and how many threads
	// per core it expects to find online. The CPUs no partition names form the
	// implicit "default" partition, where a claim that names no partition lands.
	//
	// It replaces ReservedCPUs rather than joining it: two descriptions of the
	// same CPUs in one scope have no precedence rule worth writing, so setting
	// both is an error and a reserved partition is how CPUs are kept from
	// workloads once the list is used.
	CPUPartitions []CPUPartition `json:"cpuPartitions,omitempty"`
	// Profiles are per-node-type descriptions of a node's own cores, one
	// cpuPartitions list each. CPU numbering is a property of the hardware, so
	// a fleet mixing node types has no single description that is right
	// everywhere; every other field stays fleet-wide policy. A node selects its
	// profile with the ProfileLabel label.
	//
	// Declaring any profile makes the fleet-wide CPU-naming fields errors and
	// an unlabelled node refuse to start, so a node of one type can never take
	// a description meant for another. Every profile is validated on every
	// node, so a typo in any of them fails the whole fleet fast instead of one
	// node quietly.
	Profiles map[string]Profile `json:"profiles,omitempty"`
}

// Profile is one node type's description of its own cores.
type Profile struct {
	CPUPartitions []CPUPartition `json:"cpuPartitions,omitempty"`
}

// ProfileLabel is the node label whose value names the config profile the
// node's driver applies at startup. Changing it takes effect on the next
// driver restart, deliberately: the partitions it selects are the ground truth
// under every current placement, not a value to swap live.
const ProfileLabel = "dra.cpu/profile"

// DefaultProfileName selects the implicit profile, which declares no partition
// and so leaves the whole node in the implicit default one. It always exists
// and is never declared, so a node whose cores are all interchangeable asks
// for it by name rather than by carrying no label.
const DefaultProfileName = "default"

func (p Profile) String() string {
	return fmt.Sprintf("{cpuPartitions: %v}", p.CPUPartitions)
}

// asProfile returns c with p's cores in place of its own, revalidated. It is
// the step selecting a profile on a node and checking every profile from every
// node have in common, so the two cannot drift on what a profile means or on
// what they say when one does not hold.
func (c Config) asProfile(name string, p Profile) (Config, error) {
	c.CPUPartitions = p.CPUPartitions
	c.Profiles = nil
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("config profile %q does not validate: %w", name, err)
	}
	return c, nil
}

// WithProfile returns the config the node runs on once the profile its label
// names is applied, revalidated. Naming a profile the config does not define
// is an error, never a fallback: a typo must not run a node on another node
// type's description of its cores.
func (c Config) WithProfile(name string) (Config, error) {
	if len(c.Profiles) == 0 {
		if name != "" && name != DefaultProfileName {
			return Config{}, fmt.Errorf("the node's %s label selects config profile %q, but the config declares no profiles",
				ProfileLabel, name)
		}
		return c, nil
	}
	if name == "" {
		return Config{}, fmt.Errorf("the node carries no %s label and the config declares profiles %v: label the node with the profile matching its hardware, or with %q for a node whose cores are all interchangeable",
			ProfileLabel, slices.Sorted(maps.Keys(c.Profiles)), DefaultProfileName)
	}
	profile, declared := c.Profiles[name]
	if !declared && name != DefaultProfileName {
		return Config{}, fmt.Errorf("the node's %s label selects config profile %q, but the config declares only %v",
			ProfileLabel, name, slices.Sorted(maps.Keys(c.Profiles)))
	}
	return c.asProfile(name, profile)
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
		"cachePlacementStrategy", c.CachePlacementStrategy,
		"cpuPartitions", c.CPUPartitions,
		"profiles", c.Profiles,
	}
}

// dumpConfig mirrors Config field-for-field but drops the omitempty json
// tags, so Dump also prints zero values (e.g. exposePCIeRoots=false).
type dumpConfig struct {
	Kubeconfig                            string             `json:"kubeconfig"`
	HostnameOverride                      string             `json:"hostnameOverride"`
	BindAddress                           string             `json:"bindAddress"`
	ReservedCPUs                          string             `json:"reservedCPUs"`
	CPUDeviceMode                         string             `json:"cpuDeviceMode"`
	GroupBy                               string             `json:"groupBy"`
	ExposePCIeRoots                       bool               `json:"exposePCIeRoots"`
	SysFSOverlay                          string             `json:"sysfsOverlay"`
	KubeletRootDir                        string             `json:"kubeletRootDir"`
	PublishNodeAllocatableResourceMapping bool               `json:"publishNodeAllocatableResourceMapping"`
	FullPhysicalCPUsOnly                  bool               `json:"fullPhysicalCPUsOnly"`
	AssumeUnsolicitedUpdatesSafe          bool               `json:"assumeUnsolicitedUpdatesSafe"`
	ReconcileSharedOnUnprepare            bool               `json:"reconcileSharedOnUnprepare"`
	DefragEnabled                         bool               `json:"defragEnabled"`
	CachePlacementStrategy                string             `json:"cachePlacementStrategy"`
	CPUPartitions                         []CPUPartition     `json:"cpuPartitions"`
	Profiles                              map[string]Profile `json:"profiles"`
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
