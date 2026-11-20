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
	"strings"
	"testing"
)

const noDefaultPartition = "cpuPartitions names partitions but no partition \"default\": add `- name: default` to cpuPartitions to use partitions"

// partitioned is a valid grouped configuration carrying partitions.
func partitioned(partitions ...CPUPartition) Config {
	return Config{
		KubeletRootDir: "/var/lib/kubelet",
		QPS:            20,
		Burst:          50,
		CPUDeviceMode:  "grouped",
		GroupBy:        "numanode",
		Allocator:      "cpumanager",
		CPUPartitions:  partitions,
	}
}

// profiled is a valid grouped configuration declaring profiles.
func profiled(profiles map[string]Profile) Config {
	c := partitioned()
	c.Profiles = profiles
	return c
}

func profile(partitions ...CPUPartition) Profile {
	return Profile{CPUPartitions: partitions}
}

func withConfig(c Config, change func(*Config)) Config {
	change(&c)
	return c
}

var (
	defaultEntry = CPUPartition{Name: "default"}
	vms          = CPUPartition{Name: "vms", Role: "exclusive", CPUSet: "2-3"}
)

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		conf        Config
		expectedErr bool
		// wantErr, when set, is a part of the error the configuration must give.
		wantErr string
	}{{
		name: "error: unknown allocator",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "other-allocator",
		},
		expectedErr: true,
	}, {
		name: "error: non-external allocator in machine mode",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "machine",
			Allocator:      "cpumanager",
		},
		expectedErr: true,
	}, {
		name: "valid: external allocator in machine mode",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "machine",
			Allocator:      "external",
		},
		expectedErr: false,
	}, {
		name: "valid: allocator: cpumanager",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
		},
		expectedErr: false,
	}, {
		name: "valid: groupBy uncorecache",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "uncorecache",
			Allocator:      "cpumanager",
		},
		expectedErr: false,
	}, {
		name: "error: unknown groupBy",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "core",
			Allocator:      "cpumanager",
		},
		expectedErr: true,
	}, {
		name: "valid: allocator: external",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			QPS:            20,
			Burst:          50,
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "external",
		},
		expectedErr: false,
	}, {
		name: "error: zero qps",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			Burst:          50,
		},
		expectedErr: true,
	}, {
		name: "error: negative qps",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            -1,
			Burst:          50,
		},
		expectedErr: true,
	}, {
		name: "error: zero burst",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            20,
		},
		expectedErr: true,
	}, {
		name: "valid: fractional qps, burst of one",
		conf: Config{
			KubeletRootDir: "/var/lib/kubelet",
			CPUDeviceMode:  "grouped",
			GroupBy:        "numanode",
			Allocator:      "cpumanager",
			QPS:            0.5,
			Burst:          1,
		},
		expectedErr: false,
	}, {
		name: "valid: fullPhysicalCPUsOnly in grouped mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "grouped",
			GroupBy:              "numanode",
			Allocator:            "cpumanager",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: false,
	}, {
		name: "valid: fullPhysicalCPUsOnly with the external allocator in machine mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "grouped",
			GroupBy:              "machine",
			Allocator:            "external",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: false,
	}, {
		name: "error: fullPhysicalCPUsOnly in individual mode",
		conf: Config{
			KubeletRootDir:       "/var/lib/kubelet",
			QPS:                  20,
			Burst:                50,
			CPUDeviceMode:        "individual",
			Allocator:            "cpumanager",
			FullPhysicalCPUsOnly: true,
		},
		expectedErr: true,
	}, {
		name:        "error: a reserved partition without the default entry",
		conf:        partitioned(CPUPartition{Name: "system", Role: "reserved", CPUSet: "0-1"}),
		expectedErr: true,
		wantErr:     noDefaultPartition,
	}, {
		name:        "error: vms without the default entry",
		conf:        partitioned(vms),
		expectedErr: true,
		wantErr:     noDefaultPartition,
	}, {
		name:        "error: the missing default entry comes before any other partition check",
		conf:        withConfig(partitioned(CPUPartition{Name: "Bad Name"}), func(c *Config) { c.CPUDeviceMode = "individual" }),
		expectedErr: true,
		wantErr:     noDefaultPartition,
	}, {
		name: "valid: the default entry alone",
		conf: partitioned(defaultEntry),
	}, {
		name: "valid: the default entry, a reserved and an exclusive partition",
		conf: partitioned(defaultEntry, CPUPartition{Name: "system", Role: "reserved", CPUSet: "0-1"}, vms),
	}, {
		name: "valid: smt on the default entry",
		conf: partitioned(CPUPartition{Name: "default", SMT: &SMT{Threads: 1}}, vms),
	}, {
		name:        "error: cpuset on the default entry",
		conf:        partitioned(CPUPartition{Name: "default", CPUSet: "0-1"}, vms),
		expectedErr: true,
		wantErr:     `partition "default": cpuset is filled in automatically; remove it`,
	}, {
		name:        "error: role on the default entry",
		conf:        partitioned(CPUPartition{Name: "default", Role: "default"}, vms),
		expectedErr: true,
		wantErr:     `partition "default": role is always default; remove it`,
	}, {
		name: "valid: allowExclusive on the default entry",
		conf: partitioned(CPUPartition{Name: "default", AllowExclusive: true}, vms),
	}, {
		name: "valid: allowExclusive on a named partition of role default",
		conf: partitioned(defaultEntry, CPUPartition{Name: "infra", Role: "default", CPUSet: "4-5", AllowExclusive: true}),
	}, {
		name:        "error: allowExclusive on an exclusive partition",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "vms", Role: "exclusive", CPUSet: "2-3", AllowExclusive: true}),
		expectedErr: true,
		wantErr:     `partition "vms" has role "exclusive", where allowExclusive does not apply; remove it`,
	}, {
		name:        "error: allowExclusive on a reserved partition",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "system", Role: "reserved", CPUSet: "0-1", AllowExclusive: true}),
		expectedErr: true,
		wantErr:     `partition "system" has role "reserved", where allowExclusive does not apply; remove it`,
	}, {
		name:        "error: a partition other than the default entry without a role",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "vms", CPUSet: "2-3"}),
		expectedErr: true,
		wantErr:     `invalid role "" of partition "vms": must be one of ["default" "exclusive" "reserved"]`,
	}, {
		name:        "error: a partition other than the default entry without a cpuset",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "vms", Role: "exclusive"}),
		expectedErr: true,
		wantErr:     `invalid cpuset "" of partition "vms": names no CPUs`,
	}, {
		name:        "error: role shared, like any unknown role",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "ctrl", Role: "shared", CPUSet: "2-5"}),
		expectedErr: true,
		wantErr:     `invalid role "shared" of partition "ctrl": must be one of ["default" "exclusive" "reserved"]`,
	}, {
		name:        "error: a cpuset that does not parse",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "vms", Role: "exclusive", CPUSet: "2-"}),
		expectedErr: true,
		wantErr:     `invalid cpuset "2-" of partition "vms": strconv.Atoi: parsing "": invalid syntax`,
	}, {
		name:        "error: two partitions naming one CPU",
		conf:        partitioned(defaultEntry, vms, CPUPartition{Name: "spdk", Role: "exclusive", CPUSet: "3-4"}),
		expectedErr: true,
		wantErr:     `partitions "vms" and "spdk" both claim 3: a CPU belongs to one partition`,
	}, {
		name:        "error: a partition declared twice",
		conf:        partitioned(defaultEntry, vms, CPUPartition{Name: "vms", Role: "exclusive", CPUSet: "6-7"}),
		expectedErr: true,
		wantErr:     `invalid cpuPartitions: partition "vms" is declared twice`,
	}, {
		name:        "error: the default entry declared twice",
		conf:        partitioned(defaultEntry, defaultEntry),
		expectedErr: true,
		wantErr:     `invalid cpuPartitions: partition "default" is declared twice`,
	}, {
		name:        "error: a partition without a name",
		conf:        partitioned(defaultEntry, CPUPartition{Role: "exclusive", CPUSet: "2-3"}),
		expectedErr: true,
		wantErr:     "invalid cpuPartitions: a partition has no name",
	}, {
		name:        "error: a partition name that is not a DNS label",
		conf:        partitioned(defaultEntry, CPUPartition{Name: "VMs", Role: "exclusive", CPUSet: "2-3"}),
		expectedErr: true,
		wantErr:     `invalid partition name "VMs": a lowercase RFC 1123 label must consist of`,
	}, {
		name: "valid: a partition name of 46 characters",
		conf: partitioned(defaultEntry, CPUPartition{Name: strings.Repeat("a", 46), Role: "exclusive", CPUSet: "2-3"}),
	}, {
		name:        "error: a partition name of 47 characters",
		conf:        partitioned(defaultEntry, CPUPartition{Name: strings.Repeat("a", 47), Role: "exclusive", CPUSet: "2-3"}),
		expectedErr: true,
		wantErr:     `invalid partition name "` + strings.Repeat("a", 47) + `": at most 46 characters, so that the device names built from it stay DNS labels`,
	}, {
		name:        "error: partitions beside reservedCPUs",
		conf:        withConfig(partitioned(defaultEntry), func(c *Config) { c.ReservedCPUs = "0-1" }),
		expectedErr: true,
		wantErr:     `invalid reservedCPUs "0-1": partitions replace it; declare a partition with role "reserved" instead`,
	}, {
		name: "valid: an empty list beside reservedCPUs",
		conf: withConfig(partitioned(), func(c *Config) { c.ReservedCPUs = "0-1"; c.CPUPartitions = []CPUPartition{} }),
	}, {
		name:        "error: partitions in individual mode",
		conf:        withConfig(partitioned(defaultEntry), func(c *Config) { c.CPUDeviceMode = "individual" }),
		expectedErr: true,
		wantErr:     `invalid cpuPartitions: requires cpuDeviceMode "grouped", got "individual"`,
	}, {
		name: "valid: an empty list in individual mode",
		conf: withConfig(partitioned(), func(c *Config) { c.CPUDeviceMode = "individual"; c.CPUPartitions = []CPUPartition{} }),
	}, {
		name:        "error: partitions with the external allocator",
		conf:        withConfig(partitioned(defaultEntry), func(c *Config) { c.Allocator = "external" }),
		expectedErr: true,
		wantErr:     `invalid allocator "external" with cpuPartitions: must be "cpumanager"`,
	}, {
		name:        "error: partitions with groupBy machine",
		conf:        withConfig(partitioned(defaultEntry), func(c *Config) { c.GroupBy = "machine"; c.Allocator = "external" }),
		expectedErr: true,
		wantErr:     `invalid groupBy "machine" with cpuPartitions: needs allocator "external", which partitions do not use`,
	}, {
		name:        "error: partitions with groupBy machine and the default allocator",
		conf:        withConfig(partitioned(defaultEntry), func(c *Config) { c.GroupBy = "machine" }),
		expectedErr: true,
		wantErr:     `invalid groupBy "machine" with cpuPartitions: needs allocator "external", which partitions do not use`,
	}, {
		name: "valid: two profiles, each holding the default entry",
		conf: profiled(map[string]Profile{"r7625": profile(defaultEntry, vms), "x3d": profile(defaultEntry)}),
	}, {
		name: "valid: a profile named default",
		conf: profiled(map[string]Profile{"default": profile(defaultEntry)}),
	}, {
		name:        "error: a profile without partitions",
		conf:        profiled(map[string]Profile{"r7625": {}}),
		expectedErr: true,
		wantErr:     "profile \"r7625\" has no partitions; add `- name: default` to its cpuPartitions",
	}, {
		name:        "error: a profile naming vms without the default entry",
		conf:        profiled(map[string]Profile{"r7625": profile(vms)}),
		expectedErr: true,
		wantErr:     `profile "r7625": ` + noDefaultPartition,
	}, {
		name: "error: a profile no node under test selects is checked too",
		conf: profiled(map[string]Profile{
			"r7625": profile(defaultEntry, vms),
			"x3d":   profile(defaultEntry, CPUPartition{Name: "ctrl", Role: "shared", CPUSet: "2-5"}),
		}),
		expectedErr: true,
		wantErr:     `profile "x3d": invalid role "shared" of partition "ctrl": must be one of ["default" "exclusive" "reserved"]`,
	}, {
		name:        "error: a fleet-wide list beside profiles",
		conf:        withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.CPUPartitions = []CPUPartition{defaultEntry} }),
		expectedErr: true,
		wantErr:     "invalid cpuPartitions: with profiles declared, the partitions belong to the profiles, one complete description per node type",
	}, {
		name: "valid: an empty fleet-wide list beside profiles",
		conf: withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.CPUPartitions = []CPUPartition{} }),
	}, {
		name:        "error: reservedCPUs beside profiles",
		conf:        withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.ReservedCPUs = "0-1" }),
		expectedErr: true,
		wantErr:     `invalid reservedCPUs "0-1": partitions replace it; declare a partition with role "reserved" instead`,
	}, {
		name:        "error: profiles with the external allocator",
		conf:        withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.Allocator = "external" }),
		expectedErr: true,
		wantErr:     `invalid allocator "external" with profiles: must be "cpumanager"`,
	}, {
		name:        "error: profiles with groupBy machine",
		conf:        withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.GroupBy = "machine"; c.Allocator = "external" }),
		expectedErr: true,
		wantErr:     `invalid groupBy "machine" with profiles: needs allocator "external", which partitions do not use`,
	}, {
		name:        "error: profiles with groupBy machine and the default allocator",
		conf:        withConfig(profiled(map[string]Profile{"r7625": profile(defaultEntry)}), func(c *Config) { c.GroupBy = "machine" }),
		expectedErr: true,
		wantErr:     `invalid groupBy "machine" with profiles: needs allocator "external", which partitions do not use`,
	}, {
		name: "error: a partition name with two roles in different profiles",
		conf: profiled(map[string]Profile{
			"r7625": profile(defaultEntry, vms),
			"x3d":   profile(defaultEntry, CPUPartition{Name: "vms", Role: "reserved", CPUSet: "0-1"}),
		}),
		expectedErr: true,
		wantErr:     `partition "vms" has role "exclusive" in profile "r7625" and "reserved" in profile "x3d"; a partition keeps one role in every profile`,
	}, {
		name: "valid: a profile name of 63 characters",
		conf: profiled(map[string]Profile{strings.Repeat("a", 63): profile(defaultEntry)}),
	}, {
		name:        "error: a profile name of 64 characters",
		conf:        profiled(map[string]Profile{strings.Repeat("a", 64): profile(defaultEntry)}),
		expectedErr: true,
		wantErr:     `invalid profile name "` + strings.Repeat("a", 64) + `": a node selects its profile by the value of its dra.cpu/profile label, and this name is not a label value: must be no more than 63 bytes`,
	}, {
		name:        "error: a profile name with a character outside the label alphabet",
		conf:        profiled(map[string]Profile{"r7625/a": profile(defaultEntry)}),
		expectedErr: true,
		wantErr:     `invalid profile name "r7625/a": a node selects its profile by the value of its dra.cpu/profile label, and this name is not a label value: a valid label must be an empty string or consist of alphanumeric characters`,
	}, {
		name:        "error: a profile name starting with a dash",
		conf:        profiled(map[string]Profile{"-r7625": profile(defaultEntry)}),
		expectedErr: true,
		wantErr:     `invalid profile name "-r7625": a node selects its profile by the value of its dra.cpu/profile label, and this name is not a label value: a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character`,
	}, {
		name:        "error: a profile name ending with a dash",
		conf:        profiled(map[string]Profile{"r7625-": profile(defaultEntry)}),
		expectedErr: true,
		wantErr:     `invalid profile name "r7625-": a node selects its profile by the value of its dra.cpu/profile label, and this name is not a label value: a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character`,
	}, {
		name:        "error: an empty profile name",
		conf:        profiled(map[string]Profile{"": profile(defaultEntry), "r7625": profile(defaultEntry)}),
		expectedErr: true,
		wantErr:     `invalid profile name "": a node selects its profile by the value of its dra.cpu/profile label, and an empty label selects no profile`,
	}, {
		name: "valid: a partition name with one role in every profile",
		conf: profiled(map[string]Profile{
			"r7625": profile(defaultEntry, vms),
			"x3d":   profile(defaultEntry, CPUPartition{Name: "vms", Role: "exclusive", CPUSet: "4-7"}),
		}),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.conf.Validate()
			gotErr := (err != nil)
			if gotErr != tc.expectedErr {
				t.Errorf("error got=%v expected=%v (err=%v)", gotErr, tc.expectedErr, err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}
