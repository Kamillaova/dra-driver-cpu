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

// The test file lives in package driverconfig_test (external test package) to
// verify the exported API without access to internal helpers.
package driverconfig_test

import (
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/sets"
)

// newFlagSet creates a FlagSet with cfg registered and args parsed.
func newFlagSet(t *testing.T, cfg *driverconfig.Config, args []string) *flag.FlagSet {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.AddFlags(fs)
	require.NoError(t, fs.Parse(args))
	return fs
}

// writeFile creates name with content inside dir and returns the full path.
func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

// TestResolve_NoSources: no sources returns Default() unchanged.
func TestResolve_NoSources(t *testing.T) {
	result, err := driverconfig.Resolve(testr.New(t), nil)

	require.NoError(t, err)
	assert.Equal(t, driverconfig.Default(), result)
}

// TestResolve_FileOverridesDefaults: file values are applied when no CLI flags are set.
func TestResolve_FileOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
cpuDeviceMode: individual
groupBy: socket
reservedCPUs: "0-3"
sysfsOverlay: /custom/sysfs
publishNodeAllocatableResourceMapping: true
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.NoError(t, err)
	assert.Equal(t, device.CPU_DEVICE_MODE_INDIVIDUAL, result.CPUDeviceMode)
	assert.Equal(t, device.GROUP_BY_SOCKET, result.GroupBy)
	assert.Equal(t, "0-3", result.ReservedCPUs)
	assert.Equal(t, "/custom/sysfs", result.SysFSOverlay)
	assert.True(t, result.PublishNodeAllocatableResourceMapping)
}

// TestResolve_CLIFlagWinsOverFile: an explicitly-passed CLI flag beats the file value.
func TestResolve_CLIFlagWinsOverFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
reservedCPUs: "0-3"
cpuDeviceMode: individual
`)

	var cfg driverconfig.Config
	fs := newFlagSet(t, &cfg, []string{
		"--reserved-cpus=4-7",
		"--cpu-device-mode=grouped",
	})

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
		driverconfig.FromFlags(fs),
	})

	require.NoError(t, err)
	assert.Equal(t, "4-7", result.ReservedCPUs)
	assert.Equal(t, device.CPU_DEVICE_MODE_GROUPED, result.CPUDeviceMode)
}

// TestResolve_RejectsAmbiguousKeys covers the ways a config file can reach a field
// without naming it the way the schema does. Each has to fail, and the message
// has to say what to write instead: for an excluded field that is the
// alternative setting rather than the canonical spelling, which the next check
// refuses anyway, and a name matching no field at all stays the decoder's to
// report.
func TestResolve_RejectsAmbiguousKeys(t *testing.T) {
	for _, tc := range []struct {
		name            string
		file            string
		flags           []string
		wantContains    []string
		wantNotContains []string
	}{{
		name:         "folded spelling",
		file:         "apiVersion: v1alpha1\nreservedcpus: \"0-3\"\n",
		wantContains: []string{"reservedcpus", "reservedCPUs"},
	}, {
		name:         "folded spelling, mixed case",
		file:         "apiVersion: v1alpha1\nReservedCPUs: \"0-3\"\n",
		wantContains: []string{"ReservedCPUs", "reservedCPUs"},
	}, {
		name:         "folded spelling, upper case",
		file:         "apiVersion: v1alpha1\nRESERVEDCPUS: \"0-3\"\n",
		wantContains: []string{"RESERVEDCPUS", "reservedCPUs"},
	}, {
		name:         "folded spelling, one letter",
		file:         "apiVersion: v1alpha1\nreservedCpus: \"0-3\"\n",
		wantContains: []string{"reservedCpus", "reservedCPUs"},
	}, {
		// Without this the file reached the precedence pass, where the delete is
		// by exact name, and overwrote the flag.
		name:         "folded spelling against an explicit flag",
		file:         "apiVersion: v1alpha1\nreservedcpus: \"2-3\"\n",
		flags:        []string{"--reserved-cpus=0-1"},
		wantContains: []string{"reservedcpus", "reservedCPUs"},
	}, {
		name:         "the same key twice",
		file:         "apiVersion: v1alpha1\nreservedCPUs: \"0-1\"\nreservedCPUs: \"2-3\"\n",
		wantContains: []string{"reservedCPUs"},
	}, {
		name:         "the same key twice, folded",
		file:         "apiVersion: v1alpha1\nreservedCPUs: \"0-1\"\nreservedcpus: \"2-3\"\n",
		wantContains: []string{"reservedcpus"},
	}, {
		name:            "folded apiVersion",
		file:            "ApiVersion: v1alpha1\nreservedCPUs: \"0-1\"\n",
		wantContains:    []string{`use "apiVersion"`},
		wantNotContains: []string{"unknown field"},
	}, {
		name:            "folded excluded field",
		file:            "apiVersion: v1alpha1\nbindaddress: \":9999\"\n",
		wantContains:    []string{"not configurable via the config file", "healthzPort"},
		wantNotContains: []string{"is spelled differently"},
	}, {
		name:            "folded excluded field, upper case",
		file:            "apiVersion: v1alpha1\nBINDADDRESS: \":9999\"\n",
		wantContains:    []string{"not configurable via the config file", "healthzPort"},
		wantNotContains: []string{"is spelled differently"},
	}, {
		name:            "folded excluded field with a flag alternative",
		file:            "apiVersion: v1alpha1\nexposepcieroots: true\n",
		wantContains:    []string{"not configurable via the config file", "args.exposePCIeRoots"},
		wantNotContains: []string{"is spelled differently"},
	}, {
		name:            "a name that matches nothing",
		file:            "apiVersion: v1alpha1\ntotallyUnknownField: 1\n",
		wantContains:    []string{"totallyUnknownField"},
		wantNotContains: []string{"spelled differently"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgFile := writeFile(t, dir, "config.yaml", tc.file)

			var cfg driverconfig.Config
			sources := []driverconfig.Source{driverconfig.FromFile(cfgFile)}
			if len(tc.flags) > 0 {
				fs := newFlagSet(t, &cfg, tc.flags)
				sources = append(sources, driverconfig.FromFlags(fs))
			}

			_, err := driverconfig.Resolve(testr.New(t), sources)

			require.Error(t, err)
			for _, want := range tc.wantContains {
				assert.Contains(t, err.Error(), want)
			}
			for _, notWant := range tc.wantNotContains {
				assert.NotContains(t, err.Error(), notWant)
			}
		})
	}
}

// TestResolve_ReportsEveryMiscasedKey: a hand-edited file usually has more than
// one, and fixing them one restart at a time is the difference between one
// CrashLoopBackOff and three. A key misspelled rather than miscased is not this
// check's to batch, and the decoder reports those one at a time.
//
// Asserted as the whole string rather than key by key. Ranging a map named a
// different key on each node for the same ConfigMap, so the keys are sorted, and
// an exact message is what pins that ordering along with each canonical
// spelling, the separator, and the shape of the whole thing.
func TestResolve_ReportsEveryMiscasedKey(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
reservedcpus: "0-3"
groupby: socket
cpudevicemode: individual
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.Error(t, err)
	// we care about the inner error that the source reported, not about the
	// wrapped error Resolve() produced.
	err2 := errors.Unwrap(err)
	if err2 == nil {
		err2 = err
	}
	assert.EqualError(t, err2,
		`field "cpudevicemode" is spelled differently from the schema; use "cpuDeviceMode"; `+
			`field "groupby" is spelled differently from the schema; use "groupBy"; `+
			`field "reservedcpus" is spelled differently from the schema; use "reservedCPUs"`)
}

// TestResolve_PartialFile: fields absent from the file retain their default values.
func TestResolve_PartialFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
reservedCPUs: "4-7"
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.NoError(t, err)
	assert.Equal(t, "4-7", result.ReservedCPUs)
	assert.Equal(t, ":8080", result.BindAddress)
	assert.Equal(t, device.CPU_DEVICE_MODE_GROUPED, result.CPUDeviceMode)
	assert.Equal(t, device.GROUP_BY_NUMA_NODE, result.GroupBy)
}

// TestResolve_FileWithoutAPIVersion: omitting apiVersion is accepted.
func TestResolve_FileWithoutAPIVersion(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
reservedCPUs: "5-6"
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.NoError(t, err)
	assert.Equal(t, "5-6", result.ReservedCPUs)
}

// TestResolve_UnknownAPIVersionIsError: an unrecognised apiVersion is rejected.
func TestResolve_UnknownAPIVersionIsError(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v99
reservedCPUs: "0-3"
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported apiVersion")
	assert.Contains(t, err.Error(), "v99")
}

// TestResolve_MissingFileIsError: a non-existent file path returns an error.
func TestResolve_MissingFileIsError(t *testing.T) {
	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile("/does/not/exist/config.yaml"),
	})

	require.Error(t, err)
}

// TestResolve_IgnoresUnrelatedFlags: flags that are not Config fields (such as
// --config and the klog --v flag that share the process FlagSet) must not
// produce any error log, while a mapped flag set on the command line still
// overrides the file. Regression guard for the spurious "flag not found in
// flagToJSONKey" errors that used to fire on every startup with a config file.
func TestResolve_IgnoresUnrelatedFlags(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
reservedCPUs: "0-3"
hostnameOverride: from-file
`)

	cfg := driverconfig.Default()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	// Mirror the real command line: --config and klog's --v live on the same
	// FlagSet but are not Config fields.
	fs.String("config", "", "path to the config file")
	fs.Int("v", 0, "log verbosity")
	cfg.AddFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"--config=" + cfgFile,
		"--v=4",
		"--reserved-cpus=1-2",
	}))

	var logs strings.Builder
	logger := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix + " " + args + "\n")
	}, funcr.Options{Verbosity: 10})

	got, err := driverconfig.Resolve(logger, []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
		driverconfig.FromFlags(fs),
	})

	require.NoError(t, err)
	assert.NotContains(t, logs.String(), "flag not found",
		"unrelated flags must not produce an error log")
	// The mapped flag set on the command line still wins over the file value.
	assert.Equal(t, "1-2", got.ReservedCPUs)
	// A file-only field is still applied, so the assertions above cannot pass
	// without the file actually being read.
	assert.Equal(t, "from-file", got.HostnameOverride)
}

// TestWarnDeprecatedFlags_LogsWarning: a deprecated flag logs a warning naming its driverConfig replacement.
func TestWarnDeprecatedFlags_LogsWarning(t *testing.T) {
	for _, tc := range []struct {
		flag              string
		driverConfigField string
	}{
		{flag: "cpu-device-mode=individual", driverConfigField: "cpuDeviceMode"},
		{flag: "group-by=socket", driverConfigField: "groupBy"},
		{flag: "reserved-cpus=0-1", driverConfigField: "reservedCPUs"},
		{flag: "hostname-override=node1", driverConfigField: "hostnameOverride"},
		{flag: "sysfs-overlay=/tmp/overlay.yaml", driverConfigField: "sysfsOverlay"},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			cfg := driverconfig.Default()
			fs := newFlagSet(t, &cfg, []string{"--" + tc.flag})

			var logs strings.Builder
			logger := funcr.New(func(prefix, args string) {
				logs.WriteString(prefix + " " + args + "\n")
			}, funcr.Options{})

			driverconfig.WarnDeprecatedFlags(fs, logger)

			assert.Contains(t, logs.String(), "deprecated")
			assert.Contains(t, logs.String(), tc.driverConfigField)
		})
	}
}

// TestWarnDeprecatedFlags_NonDeprecatedFlagNoWarning: non-deprecated flags don't trigger the deprecation warning.
func TestWarnDeprecatedFlags_NonDeprecatedFlagNoWarning(t *testing.T) {
	cfg := driverconfig.Default()
	fs := newFlagSet(t, &cfg, []string{"--bind-address=:9090"})

	var logs strings.Builder
	logger := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix + " " + args + "\n")
	}, funcr.Options{})

	driverconfig.WarnDeprecatedFlags(fs, logger)

	assert.NotContains(t, logs.String(), "deprecated")
}

// deprecatedFlagNames is the hardcoded set of flags expected to be marked
// as deprecated in --help. Update it whenever a flag's deprecation status changes.
var deprecatedFlagNames = sets.New(
	"cpu-device-mode",
	"group-by",
	"reserved-cpus",
	"hostname-override",
	"sysfs-overlay",
)

// TestDeprecatedFlags_HelpTextSuffix: exactly the flags in deprecatedFlagNames
// carry a "(DEPRECATED: ...)" suffix in --help - none missing, none extra.
func TestDeprecatedFlags_HelpTextSuffix(t *testing.T) {
	cfg := driverconfig.Config{}
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.AddFlags(fs)

	fs.VisitAll(func(f *flag.Flag) {
		wantDeprecated := deprecatedFlagNames.Has(f.Name)
		isMarkedDeprecated := strings.Contains(f.Usage, "(DEPRECATED:")
		switch {
		case wantDeprecated && !isMarkedDeprecated:
			t.Errorf("flag %q is expected to be deprecated but its --help text has no DEPRECATED suffix: %q", f.Name, f.Usage)
		case !wantDeprecated && isMarkedDeprecated:
			t.Errorf("flag %q has a DEPRECATED --help suffix but isn't expected to be deprecated: %q", f.Name, f.Usage)
		}
	})
}

// TestResolve_EmptyFilePath: an empty file path is a no-op.
func TestResolve_EmptyFilePath(t *testing.T) {
	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(""),
	})

	require.NoError(t, err)
	assert.Equal(t, driverconfig.Default(), result)
}

// TestDefault pins the built-in default values.
func TestDefault(t *testing.T) {
	d := driverconfig.Default()

	assert.Equal(t, ":8080", d.BindAddress)
	assert.Equal(t, device.CPU_DEVICE_MODE_GROUPED, d.CPUDeviceMode)
	assert.Equal(t, device.GROUP_BY_NUMA_NODE, d.GroupBy)
	// The kubelet root defaults to the standard location, so behavior is
	// unchanged unless the kubelet --root-dir is relocated.
	assert.Equal(t, driverconfig.DefaultKubeletRootDir, d.KubeletRootDir)
	// Fields with no built-in default must be zero/empty.
	assert.Empty(t, d.Kubeconfig)
	assert.Empty(t, d.HostnameOverride)
	assert.Empty(t, d.ReservedCPUs)
	assert.False(t, d.ExposePCIeRoots)
	assert.False(t, d.PublishNodeAllocatableResourceMapping)
}

// TestResolve_InvalidCPUDeviceModeIsError: an invalid cpuDeviceMode in the file is rejected.
func TestResolve_InvalidCPUDeviceModeIsError(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
cpuDeviceMode: garbage
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid cpuDeviceMode")
	assert.Contains(t, err.Error(), "garbage")
}

// TestResolve_InvalidGroupByIsError: an invalid groupBy in the file is rejected.
func TestResolve_InvalidGroupByIsError(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
groupBy: garbage
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid groupBy")
	assert.Contains(t, err.Error(), "garbage")
}

// TestResolve_GroupByUncoreCache: a config file may ask for one device per
// uncore cache, and the generated schema accepts exactly what the validator
// does.
func TestResolve_GroupByUncoreCache(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
groupBy: uncorecache
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})

	require.NoError(t, err)
	assert.Equal(t, device.GROUP_BY_UNCORE_CACHE, result.GroupBy)

	raw, err := driverconfig.GenerateDriverConfigSchema()
	require.NoError(t, err)
	var schema struct {
		Properties struct {
			GroupBy struct {
				Enum []string `json:"enum"`
			} `json:"groupBy"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	assert.Contains(t, schema.Properties.GroupBy.Enum, device.GROUP_BY_UNCORE_CACHE,
		"a value the driver accepts that the schema does not is a config file the chart rejects")
}

// TestResolve_ExcludedFieldInFileIsError: excluded and removed fields aren't
// configurable via the config file.
func TestResolve_ExcludedFieldInFileIsError(t *testing.T) {
	for _, tc := range []struct {
		field    string
		content  string
		wantErrs []string
	}{
		{field: "bindAddress", content: "bindAddress: \":9090\"", wantErrs: []string{"not configurable via the config file"}},
		{field: "exposePCIeRoots", content: "exposePCIeRoots: true", wantErrs: []string{"not configurable via the config file"}},
		{field: "showMetrics", content: "showMetrics: true", wantErrs: []string{"unknown field"}},
		{
			field:   "kubeletRootDir",
			content: "kubeletRootDir: /mnt/fast/k8s/kubelet",
			// The error must name both routes or a Helm user will not know
			// kubeletRootDir is what the chart actually maps the flag to.
			wantErrs: []string{
				"not configurable via the config file",
				"use the chart's top-level kubeletRootDir value",
				"--kubelet-root-dir when running the binary directly",
			},
		},
	} {
		t.Run(tc.field, func(t *testing.T) {
			dir := t.TempDir()
			cfgFile := writeFile(t, dir, "config.yaml", "apiVersion: v1alpha1\n"+tc.content+"\n")

			_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
				driverconfig.FromFile(cfgFile),
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.field)
			for _, want := range tc.wantErrs {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}

// The chart passes the kubelet root as a flag and renders its hostPath mounts
// from the same value, so a root that cannot be used has to fail here.
func TestResolve_KubeletRootDirFromFlag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		wantRoot string
		wantErr  string
	}{
		{
			name:     "absolute",
			args:     []string{"--kubelet-root-dir=/mnt/fast/k8s/kubelet"},
			wantRoot: "/mnt/fast/k8s/kubelet",
		},
		{
			// Cleaned in the config layer so the logged value matches the paths
			// the driver and the chart derive from it.
			name:     "non-canonical is cleaned",
			args:     []string{"--kubelet-root-dir=/mnt/a/../kubelet//"},
			wantRoot: "/mnt/kubelet",
		},
		{
			name:    "relative",
			args:    []string{"--kubelet-root-dir=relative/kubelet"},
			wantErr: "must be an absolute path",
		},
		{
			name:    "empty",
			args:    []string{"--kubelet-root-dir="},
			wantErr: "must not be empty",
		},
		{
			// flag takes the last value, so a chart appending an empty override
			// would otherwise undo an earlier root.
			name:    "emptied after being set",
			args:    []string{"--kubelet-root-dir=/mnt/fast/k8s/kubelet", "--kubelet-root-dir="},
			wantErr: "must not be empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg driverconfig.Config
			fs := newFlagSet(t, &cfg, tc.args)

			result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
				driverconfig.FromFlags(fs),
			})

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantRoot, result.KubeletRootDir)
		})
	}
}

// TestFromFlags_BoolAndStringValues: FromFlags must produce typed values —
// bool flags as bool (not the string "true"), string flags as string, and
// custom flag.Value types (which don't implement flag.Getter) as string.
// Without this, applyMap would fail to decode a JSON string into a bool field.
func TestFromFlags_BoolAndStringValues(t *testing.T) {
	for _, tc := range []struct {
		name           string
		args           []string
		wantPCIeRoots  bool
		wantReserved   string
		wantDeviceMode string
	}{
		{
			name:           "bool flag set to true",
			args:           []string{"--expose-pcie-roots=true", "--reserved-cpus=0-3", "--cpu-device-mode=individual"},
			wantPCIeRoots:  true,
			wantReserved:   "0-3",
			wantDeviceMode: device.CPU_DEVICE_MODE_INDIVIDUAL,
		},
		{
			name:           "bool flag set to false",
			args:           []string{"--expose-pcie-roots=false", "--reserved-cpus=4-7"},
			wantPCIeRoots:  false,
			wantReserved:   "4-7",
			wantDeviceMode: device.CPU_DEVICE_MODE_GROUPED,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driverconfig.Default()
			fs := newFlagSet(t, &cfg, tc.args)

			src := driverconfig.FromFlags(fs)
			result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{src})

			require.NoError(t, err)
			assert.Equal(t, tc.wantPCIeRoots, result.ExposePCIeRoots)
			assert.Equal(t, tc.wantReserved, result.ReservedCPUs)
			assert.Equal(t, tc.wantDeviceMode, result.CPUDeviceMode)
		})
	}
}

func TestResolve_KubeletRootDirWithAConfigFile(t *testing.T) {
	for _, tc := range []struct {
		name             string
		content          string
		args             []string
		wantErrs         []string
		wantRoot         string
		wantReservedCPUs string
	}{
		{
			// encoding/json matches a field without regard to case, so before the
			// canonical-key pass a differently spelled key walked past the
			// exclusion and replaced a root given on the command line, which is
			// issue #231 reached through a config file.
			name:     "a differently spelled key cannot override the flag",
			content:  "kubeletrootdir: /wrong/root",
			args:     []string{"--kubelet-root-dir=/correct/root"},
			wantErrs: []string{"kubeletrootdir", "not configurable via the config file"},
		},
		{
			// The control. Refusing every file that mentions anything would close
			// the case above without the pass that closes it.
			name:             "an unrelated file leaves the flag alone",
			content:          `reservedCPUs: "0-3"`,
			args:             []string{"--kubelet-root-dir=/correct/root"},
			wantRoot:         "/correct/root",
			wantReservedCPUs: "0-3",
		},
		{
			// The file is an input, not the source: it is valid and is not allowed
			// to carry this field, so blaming its contents would send the reader
			// looking for a key that cannot be there.
			name:     "a bad flag names the file as an input",
			content:  `reservedCPUs: "0-1"`,
			args:     []string{"--kubelet-root-dir=relative/kubelet"},
			wantErrs: []string{"must be an absolute path"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfgFile := writeFile(t, dir, "config.yaml", "apiVersion: v1alpha1\n"+tc.content+"\n")

			var cfg driverconfig.Config
			fs := newFlagSet(t, &cfg, tc.args)

			result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
				driverconfig.FromFile(cfgFile),
				driverconfig.FromFlags(fs),
			})

			if len(tc.wantErrs) > 0 {
				require.Error(t, err)
				for _, want := range tc.wantErrs {
					assert.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantRoot, result.KubeletRootDir)
			assert.Equal(t, tc.wantReservedCPUs, result.ReservedCPUs)
		})
	}
}

// TestResolve_BoolFlagWinsOverFile: a bool CLI flag correctly overrides via the JSON round-trip.
func TestResolve_BoolFlagWinsOverFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
reservedCPUs: "0-3"
`)

	var cfg driverconfig.Config
	fs := newFlagSet(t, &cfg, []string{
		"--expose-pcie-roots=true",
	})

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
		driverconfig.FromFlags(fs),
	})

	require.NoError(t, err)
	assert.True(t, result.ExposePCIeRoots)
	assert.Equal(t, "0-3", result.ReservedCPUs)
}

// TestResolve_FullPhysicalCPUsOnlyDefaultsOff: the option is opt-in, so an
// untouched config keeps upstream's single-thread allocation behaviour.
func TestResolve_FullPhysicalCPUsOnlyDefaultsOff(t *testing.T) {
	assert.False(t, driverconfig.Default().FullPhysicalCPUsOnly)
}

// TestValidate_FullPhysicalCPUsOnlyRequiresGroupedMode: in individual mode the
// scheduler picks exact per-CPU devices, so the driver cannot hold a core's
// siblings together and the option must be refused rather than ignored.
func TestValidate_FullPhysicalCPUsOnlyRequiresGroupedMode(t *testing.T) {
	cfg := driverconfig.Default()
	cfg.FullPhysicalCPUsOnly = true

	cfg.CPUDeviceMode = device.CPU_DEVICE_MODE_GROUPED
	assert.NoError(t, cfg.Validate())

	cfg.CPUDeviceMode = device.CPU_DEVICE_MODE_INDIVIDUAL
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fullPhysicalCPUsOnly")
	assert.Contains(t, err.Error(), "requires cpuDeviceMode")
}

// TestResolve_UnsolicitedUpdateDefaults: pushing updates the runtime did not ask
// for is opt-in, because it deadlocks runtimes with a pre-#301 NRI. The reconcile
// that depends on it defaults on, so one option is enough to enable it.
func TestResolve_UnsolicitedUpdateDefaults(t *testing.T) {
	d := driverconfig.Default()
	assert.False(t, d.AssumeUnsolicitedUpdatesSafe, "must be an explicit operator assertion")
	assert.True(t, d.ReconcileSharedOnUnprepare, "inert until the assertion is made")
}

// TestResolve_UnsolicitedUpdateOptionsFromFile: both options are settable from a
// config file, including turning the reconcile off against its default.
func TestResolve_UnsolicitedUpdateOptionsFromFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
assumeUnsolicitedUpdatesSafe: true
reconcileSharedOnUnprepare: false
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.NoError(t, err)
	assert.True(t, result.AssumeUnsolicitedUpdatesSafe)
	assert.False(t, result.ReconcileSharedOnUnprepare)
}

// TestResolve_DefragDefaults: defragmentation is off.
func TestResolve_DefragDefaults(t *testing.T) {
	d := driverconfig.Default()
	assert.False(t, d.DefragEnabled)
	assert.NoError(t, d.Validate())
}

// TestValidate_DefragRequirements: the modes where the driver does not choose a
// claim's CPUs, and the runtime assertion a move depends on.
func TestValidate_DefragRequirements(t *testing.T) {
	testCases := []struct {
		name          string
		mutate        func(*driverconfig.Config)
		expectedError string
	}{
		{
			name:   "grouped by NUMA node with unsolicited updates permitted",
			mutate: func(*driverconfig.Config) {},
		},
		{
			name:   "grouped by socket",
			mutate: func(c *driverconfig.Config) { c.GroupBy = device.GROUP_BY_SOCKET },
		},
		{
			// The scheduler picked the exact CPU devices, so their placement is
			// not the driver's to change.
			name:          "individual mode",
			mutate:        func(c *driverconfig.Config) { c.CPUDeviceMode = device.CPU_DEVICE_MODE_INDIVIDUAL },
			expectedError: "requires cpuDeviceMode",
		},
		{
			// The cpuset came from the claim's own opaque config.
			name:          "grouped by machine",
			mutate:        func(c *driverconfig.Config) { c.GroupBy = device.GROUP_BY_MACHINE },
			expectedError: "requires groupBy",
		},
		{
			// Here the driver does choose the CPUs, but a device is one cache, so
			// a move would take the claim off the device it was allocated on.
			name:          "grouped by uncore cache",
			mutate:        func(c *driverconfig.Config) { c.GroupBy = device.GROUP_BY_UNCORE_CACHE },
			expectedError: "a device is one uncore cache",
		},
		{
			name:          "without the unsolicited update assertion",
			mutate:        func(c *driverconfig.Config) { c.AssumeUnsolicitedUpdatesSafe = false },
			expectedError: "requires assumeUnsolicitedUpdatesSafe",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driverconfig.Default()
			cfg.DefragEnabled = true
			cfg.AssumeUnsolicitedUpdatesSafe = true
			tc.mutate(&cfg)

			err := cfg.Validate()
			if tc.expectedError == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), "defragEnabled")
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

// TestValidate_DefragRequirementsAreInertWhenDisabled: an unusable combination
// must not stop a driver that is not going to defragment anything.
func TestValidate_DefragRequirementsAreInertWhenDisabled(t *testing.T) {
	cfg := driverconfig.Default()
	cfg.CPUDeviceMode = device.CPU_DEVICE_MODE_INDIVIDUAL
	assert.NoError(t, cfg.Validate())
}

// TestResolve_DefragOptionsFromFile: the option is settable from a config file.
func TestResolve_DefragOptionsFromFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
assumeUnsolicitedUpdatesSafe: true
defragEnabled: true
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.NoError(t, err)
	assert.True(t, result.DefragEnabled)
}

// TestValidate_CachePlacementStrategy: only the two named policies exist, and
// the policy governs which CPUs the driver picks, so it is rejected where the
// driver does not pick.
func TestValidate_CachePlacementStrategy(t *testing.T) {
	cfg := driverconfig.Default()
	assert.Equal(t, "pack", cfg.CachePlacementStrategy, "the default must be upstream's behaviour")
	assert.NoError(t, cfg.Validate())

	cfg.CachePlacementStrategy = "spread"
	assert.NoError(t, cfg.Validate(), "spread must not demand whole-core allocation")

	cfg.CPUDeviceMode = device.CPU_DEVICE_MODE_INDIVIDUAL
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires cpuDeviceMode")
	cfg.CPUDeviceMode = device.CPU_DEVICE_MODE_GROUPED

	cfg.CachePlacementStrategy = "sprinkle"
	err = cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"sprinkle"`)
}

// TestValidate_CPUPartitions covers the checks that need no topology, which are
// the ones every node runs for every profile.
func TestValidate_CPUPartitions(t *testing.T) {
	valid := []driverconfig.CPUPartition{
		{Name: "system", Role: "reserved", CPUs: "0,16"},
		{Name: "dataplane", Role: "exclusive", CPUs: "8-11", SMT: &driverconfig.SMT{Threads: 1}},
	}
	testCases := []struct {
		name          string
		mutate        func(*driverconfig.Config)
		expectedError string
	}{
		{
			name:   "a valid list",
			mutate: func(c *driverconfig.Config) { c.CPUPartitions = valid },
		},
		{
			name: "individual mode",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = valid
				c.CPUDeviceMode = device.CPU_DEVICE_MODE_INDIVIDUAL
			},
			expectedError: "requires cpuDeviceMode",
		},
		{
			name: "beside reservedCPUs",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = valid
				c.ReservedCPUs = "0"
			},
			expectedError: "names CPUs in the same scope",
		},
		{
			name: "an unknown role",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: "vm", Role: "burst", CPUs: "1"}}
			},
			expectedError: `invalid role "burst" of partition "vm"`,
		},
		{
			name: "the implicit partition's name",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: "default", Role: "exclusive", CPUs: "1"}}
			},
			expectedError: "never declared",
		},
		{
			name: "a name that is not a DNS label",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: "Data Plane", Role: "exclusive", CPUs: "1"}}
			},
			expectedError: `invalid partition name "Data Plane"`,
		},
		{
			name: "a name too long for a device name",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: strings.Repeat("a", 47), Role: "exclusive", CPUs: "1"}}
			},
			expectedError: "at most 46 characters",
		},
		{
			name: "a duplicate name",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{
					{Name: "vm", Role: "exclusive", CPUs: "1"},
					{Name: "vm", Role: "exclusive", CPUs: "2"},
				}
			},
			expectedError: `partition "vm" is declared twice`,
		},
		{
			name: "unparseable cpus",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: "a-b"}}
			},
			expectedError: `invalid cpus "a-b" of partition "vm"`,
		},
		{
			name: "no cpus",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: ""}}
			},
			expectedError: "names no CPUs",
		},
		{
			name: "overlapping partitions",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = []driverconfig.CPUPartition{
					{Name: "vm", Role: "exclusive", CPUs: "4-7"},
					{Name: "dataplane", Role: "exclusive", CPUs: "6-9"},
				}
			},
			expectedError: `partitions "vm" and "dataplane" both claim 6-7`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driverconfig.Default()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.expectedError == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

// TestResolve_CPUPartitionsFromFile: the three spellings of the thread-arity
// expectation all parse, and the dump reads back as the operator wrote it.
func TestResolve_CPUPartitionsFromFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
cpuPartitions:
  - name: system
    role: reserved
    cpus: "0,16"
  - name: helpers
    role: shared
    cpus: "1-3"
    smt: true
  - name: dataplane
    role: exclusive
    cpus: "8-11"
    smt: false
  - name: vm
    role: exclusive
    cpus: "4-7"
    smt: 2
`)

	result, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.NoError(t, err)
	require.Len(t, result.CPUPartitions, 4)
	assert.Equal(t, 0, result.CPUPartitions[0].ThreadsPerCore(), "an absent smt means the platform's own arity")
	assert.Equal(t, 0, result.CPUPartitions[1].ThreadsPerCore())
	assert.Equal(t, 1, result.CPUPartitions[2].ThreadsPerCore())
	assert.Equal(t, 2, result.CPUPartitions[3].ThreadsPerCore())

	dump := result.Dump()
	assert.Contains(t, dump, "smt: true")
	assert.Contains(t, dump, "smt: false")
	assert.Contains(t, dump, "smt: 2")
}

// TestResolve_CPUPartitionsRejectUnknownFields: a misspelled partition field is
// a typo the operator must see, not a silently ignored key.
func TestResolve_CPUPartitionsRejectUnknownFields(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
cpuPartitions:
  - name: vm
    role: exclusive
    cpuset: "4-7"
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cpuset")
}

// TestResolve_CPUPartitionsRejectBadSMT: smt takes true, false or a thread
// count, and anything else fails the file rather than defaulting.
func TestResolve_CPUPartitionsRejectBadSMT(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
cpuPartitions:
  - name: vm
    role: exclusive
    cpus: "4-7"
    smt: "off"
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be true, false or a thread count")
}

// TestWithProfile: the label value picks the node type's description of its
// own cores; the implicit profile leaves the whole node in one default
// partition; a name the config does not declare is an error, never a fallback
// onto another node type's cores, and an unlabelled node does not start at all.
func TestWithProfile(t *testing.T) {
	r7625 := []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: "8-15"}}
	base := driverconfig.Default()
	base.Profiles = map[string]driverconfig.Profile{
		"r7625": {CPUPartitions: r7625},
		"x3d":   {CPUPartitions: []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: "4-7"}}},
	}

	t.Run("a profile becomes the node's partitions", func(t *testing.T) {
		cfg, err := base.WithProfile("r7625")
		require.NoError(t, err)
		assert.Equal(t, r7625, cfg.CPUPartitions)
		assert.Empty(t, cfg.Profiles, "the profile a node did not select is not its business")
	})

	t.Run("the implicit profile declares nothing", func(t *testing.T) {
		cfg, err := base.WithProfile(driverconfig.DefaultProfileName)
		require.NoError(t, err)
		assert.Empty(t, cfg.CPUPartitions)
	})

	t.Run("an unlabelled node does not start", func(t *testing.T) {
		_, err := base.WithProfile("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), driverconfig.ProfileLabel)
		assert.Contains(t, err.Error(), "r7625")
		assert.Contains(t, err.Error(), "x3d")
	})

	t.Run("an unknown profile is an error", func(t *testing.T) {
		_, err := base.WithProfile("tpyo")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"tpyo"`)
		assert.Contains(t, err.Error(), driverconfig.ProfileLabel)
		assert.Contains(t, err.Error(), "r7625")
	})

	t.Run("without profiles the fleet-wide fields stand", func(t *testing.T) {
		fleet := driverconfig.Default()
		fleet.ReservedCPUs = "0-1"
		for _, name := range []string{"", driverconfig.DefaultProfileName} {
			cfg, err := fleet.WithProfile(name)
			require.NoError(t, err)
			assert.Equal(t, "0-1", cfg.ReservedCPUs)
		}
		_, err := fleet.WithProfile("r7625")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "declares no profiles")
	})
}

// TestValidate_Profiles covers the scope rule around profiles and the checks
// on the profiles themselves, all of which every node runs.
func TestValidate_Profiles(t *testing.T) {
	vm := []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: "8-15"}}
	testCases := []struct {
		name          string
		mutate        func(*driverconfig.Config)
		expectedError string
	}{
		{
			name: "a valid profile",
			mutate: func(c *driverconfig.Config) {
				c.Profiles = map[string]driverconfig.Profile{"r7625": {CPUPartitions: vm}}
			},
		},
		{
			name: "reservedCPUs beside profiles",
			mutate: func(c *driverconfig.Config) {
				c.ReservedCPUs = "0"
				c.Profiles = map[string]driverconfig.Profile{"r7625": {CPUPartitions: vm}}
			},
			expectedError: "with profiles declared",
		},
		{
			name: "cpuPartitions beside profiles",
			mutate: func(c *driverconfig.Config) {
				c.CPUPartitions = vm
				c.Profiles = map[string]driverconfig.Profile{"r7625": {CPUPartitions: vm}}
			},
			expectedError: "the partitions belong to the profiles",
		},
		{
			name: "the implicit profile's name",
			mutate: func(c *driverconfig.Config) {
				c.Profiles = map[string]driverconfig.Profile{"default": {CPUPartitions: vm}}
			},
			expectedError: "never declared",
		},
		{
			name: "a profile describing no cores",
			mutate: func(c *driverconfig.Config) {
				c.Profiles = map[string]driverconfig.Profile{"r7625": {}}
			},
			expectedError: "describes no cores",
		},
		{
			name: "a profile the node under test does not select",
			mutate: func(c *driverconfig.Config) {
				c.Profiles = map[string]driverconfig.Profile{
					"r7625": {CPUPartitions: vm},
					"x3d":   {CPUPartitions: []driverconfig.CPUPartition{{Name: "vm", Role: "burst", CPUs: "1"}}},
				}
			},
			expectedError: `config profile "x3d" does not validate`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driverconfig.Default()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.expectedError == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

// TestResolve_ProfileRejectsReservedCPUs: a profile carries partitions and
// nothing else, so upstream's fleet-wide cpuset inside one is a typo the
// operator has to see rather than a key that decodes into nothing.
func TestResolve_ProfileRejectsReservedCPUs(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
apiVersion: v1alpha1
profiles:
  r7625:
    reservedCPUs: "0,128"
`)

	_, err := driverconfig.Resolve(testr.New(t), []driverconfig.Source{
		driverconfig.FromFile(cfgFile),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reservedCPUs")
}

// TestWarnDeprecatedCPUFields: a node's cores described outside a profile
// still work and say so; described inside one, there is nothing to warn about.
func TestWarnDeprecatedCPUFields(t *testing.T) {
	partitions := []driverconfig.CPUPartition{{Name: "vm", Role: "exclusive", CPUs: "8-15"}}
	for _, tc := range []struct {
		name   string
		mutate func(*driverconfig.Config)
		want   []string
		absent bool
	}{
		{
			name:   "a fleet-wide reservation",
			mutate: func(c *driverconfig.Config) { c.ReservedCPUs = "0-1" },
			want:   []string{"deprecated", "reservedCPUs", driverconfig.ProfileLabel},
		},
		{
			name:   "a fleet-wide partition list",
			mutate: func(c *driverconfig.Config) { c.CPUPartitions = partitions },
			want:   []string{"deprecated", "cpuPartitions"},
		},
		{
			name: "the same list inside a profile",
			mutate: func(c *driverconfig.Config) {
				c.Profiles = map[string]driverconfig.Profile{"r7625": {CPUPartitions: partitions}}
			},
			absent: true,
		},
		{
			name:   "nothing named at all",
			mutate: func(c *driverconfig.Config) {},
			absent: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := driverconfig.Default()
			tc.mutate(&cfg)

			var logs strings.Builder
			logger := funcr.New(func(prefix, args string) {
				logs.WriteString(prefix + " " + args + "\n")
			}, funcr.Options{})

			cfg.WarnDeprecatedCPUFields(logger)

			if tc.absent {
				assert.NotContains(t, logs.String(), "deprecated")
				return
			}
			for _, want := range tc.want {
				assert.Contains(t, logs.String(), want)
			}
		})
	}
}
