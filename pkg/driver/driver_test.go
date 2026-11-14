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

package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/containerd/nri/pkg/stub"
	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

type mockNRIRunner struct {
	runFunc func(ctx context.Context) error
	calls   atomic.Int32
}

func (m *mockNRIRunner) Run(ctx context.Context) error {
	m.calls.Add(1)
	return m.runFunc(ctx)
}

const (
	testRetryInitialBackoff = time.Millisecond
	testRetryMaxBackoff     = 4 * time.Millisecond
	testRetryHealthyRun     = time.Hour
)

func TestRunNRIPluginWithRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			cancel()
			return context.Canceled
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.ErrorIs(t, err, context.Canceled, "should return context.Canceled when context is cancelled")
	require.Equal(t, int32(1), runner.calls.Load(), "Run should be called exactly once before context cancel")
}

func TestRunNRIPluginWithRetry_ContextCancelledAfterSeveralRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var calls atomic.Int32
	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			n := calls.Add(1)
			if n >= 3 {
				cancel()
				return context.Canceled
			}
			return fmt.Errorf("transient error")
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.ErrorIs(t, err, context.Canceled, "should return context.Canceled when context is cancelled")
	require.Equal(t, int32(3), calls.Load(), "Run should be called 3 times before context cancel")
}

func TestRunNRIPluginWithRetry_ExhaustsAttempts(t *testing.T) {
	ctx := context.Background()

	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			return fmt.Errorf("persistent error")
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, 3, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.Error(t, err, "should return error after exhausting attempts")
	require.Equal(t, int32(3), runner.calls.Load(), "Run should be called exactly maxAttempts times")
}

func TestRunNRIPluginWithRetry_SuccessfulRunNoRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			cancel()
			return nil
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), runner.calls.Load())
}

func gapsBetween(times []time.Time) []time.Duration {
	gaps := make([]time.Duration, 0, len(times))
	for i := 1; i < len(times); i++ {
		gaps = append(gaps, times[i].Sub(times[i-1]))
	}
	return gaps
}

func TestRunNRIPluginWithRetry_BacksOffBetweenAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs []time.Time
		runner := &mockNRIRunner{
			runFunc: func(context.Context) error {
				runs = append(runs, time.Now())
				return fmt.Errorf("connection refused")
			},
		}

		err := runNRIPluginWithRetry(t.Context(), runner, maxAttempts, nriRetryInitialBackoff, nriRetryMaxBackoff, nriRetryHealthyRunDuration)
		require.Error(t, err)
		require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}, gapsBetween(runs))
	})
}

func TestRunNRIPluginWithRetry_BackoffDoublesUpToTheCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var runs []time.Time
		runner := &mockNRIRunner{
			runFunc: func(context.Context) error {
				runs = append(runs, time.Now())
				return fmt.Errorf("connection refused")
			},
		}

		err := runNRIPluginWithRetry(t.Context(), runner, 6, time.Second, 4*time.Second, time.Hour)
		require.Error(t, err)
		require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 4 * time.Second}, gapsBetween(runs))
	})
}

func TestRunNRIPluginWithRetry_HealthyRunResetsTheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		var runs []time.Time
		runner := &mockNRIRunner{
			runFunc: func(context.Context) error {
				runs = append(runs, time.Now())
				switch len(runs) {
				case 1:
					return fmt.Errorf("crash loop")
				case 2:
					time.Sleep(nriRetryHealthyRunDuration)
					return fmt.Errorf("fresh problem")
				default:
					cancel()
					return context.Canceled
				}
			},
		}

		// With two attempts, only the reset leaves one for a third run.
		err := runNRIPluginWithRetry(ctx, runner, 2, nriRetryInitialBackoff, nriRetryMaxBackoff, nriRetryHealthyRunDuration)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, runs, 3)
		require.Equal(t, nriRetryHealthyRunDuration+nriRetryInitialBackoff, runs[2].Sub(runs[1]), "the backoff restarts from its initial value")
	})
}

// exposePCIeRoots lowers devicesPerResourceSlice from 128 to 64, which the
// configuration does not show and which can double the ResourceSlices a node
// publishes.
func TestNewReportsResourceSliceCountUnderExposePCIeRoots(t *testing.T) {
	var logs strings.Builder
	logger := funcr.New(func(prefix, args string) {
		logs.WriteString(prefix + " " + args + "\n")
	}, funcr.Options{})

	prov := Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mockCPUInfos_DualSocket_120CPUsPerSocket_HT},
		SysFS:   testSysFS(mockCPUInfos_DualSocket_120CPUsPerSocket_HT),
	}
	conf := Config{
		DriverName:      testDriverName,
		NodeName:        testNodeName,
		ReservedCPUs:    cpuset.New(),
		ExposePCIeRoots: true,
	}
	_, err := New(logger, prov, &conf)
	require.NoError(t, err)

	logged := logs.String()
	require.Contains(t, logged, `"msg"="chunked devices into ResourceSlices"`)
	require.Contains(t, logged, `"numDevices"=240`)
	require.Contains(t, logged, `"devicesPerResourceSlice"=64`)
	require.Contains(t, logged, `"numResourceSlices"=4`)
	require.Contains(t, logged, `"exposePCIeRoots"=true`)
}

// mixedCoreCPUInfos is one NUMA node with cores {0,4} and {1,5} of two threads
// and cores {2} and {3} of one, their siblings offline.
var mixedCoreCPUInfos = []cpuinfo.CPUInfo{
	{CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 4, SiblingCPUSet: cpuset.New(0, 4), CoreType: cpuinfo.CoreTypeStandard},
	{CpuID: 1, CoreID: 1, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 5, SiblingCPUSet: cpuset.New(1, 5), CoreType: cpuinfo.CoreTypeStandard},
	{CpuID: 2, CoreID: 2, SocketID: 0, NUMANodeID: 0, SiblingCPUID: -1, SiblingCPUSet: cpuset.New(2), CoreType: cpuinfo.CoreTypeStandard},
	{CpuID: 3, CoreID: 3, SocketID: 0, NUMANodeID: 0, SiblingCPUID: -1, SiblingCPUSet: cpuset.New(3), CoreType: cpuinfo.CoreTypeStandard},
	{CpuID: 4, CoreID: 0, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 0, SiblingCPUSet: cpuset.New(0, 4), CoreType: cpuinfo.CoreTypeStandard},
	{CpuID: 5, CoreID: 1, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 1, SiblingCPUSet: cpuset.New(1, 5), CoreType: cpuinfo.CoreTypeStandard},
}

func TestNewReportsAMixedDeviceUnderFullPhysicalCPUsOnly(t *testing.T) {
	const msg = `"msg"="fullPhysicalCPUsOnly publishes no request policy on a device whose cores differ in threads"`
	for _, tc := range []struct {
		name                 string
		fullPhysicalCPUsOnly bool
		want                 string // the line's keys, "" for no line
	}{
		{
			name:                 "option on",
			fullPhysicalCPUsOnly: true,
			want:                 `"device"="cpudevnuma000" "coresWithTwoThreads"=2 "coresWithOneThread"=2`,
		},
		{name: "option off"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs strings.Builder
			logger := funcr.New(func(prefix, args string) {
				logs.WriteString(prefix + " " + args + "\n")
			}, funcr.Options{})
			prov := Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   testSysFS(mixedCoreCPUInfos),
			}
			_, err := New(logger, prov, &Config{
				DriverName:           testDriverName,
				NodeName:             testNodeName,
				ReservedCPUs:         cpuset.New(),
				CPUDeviceMode:        device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy:     device.GROUP_BY_NUMA_NODE,
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
			})
			require.NoError(t, err)

			var lines []string
			for line := range strings.Lines(logs.String()) {
				if strings.Contains(line, msg) {
					lines = append(lines, line)
				}
			}
			if tc.want == "" {
				require.Empty(t, lines)
				return
			}
			require.Len(t, lines, 1)
			require.Contains(t, lines[0], tc.want)
		})
	}
}

func TestNewPublishesWholeCorePoliciesUnderFullPhysicalCPUsOnly(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		fullPhysicalCPUsOnly bool
		wantPolicy           bool
	}{
		{name: "option off"},
		{name: "option on", fullPhysicalCPUsOnly: true, wantPolicy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prov := Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mockCPUInfos_DualSocket_4CPUsPerSocket_HT},
				SysFS:   testSysFS(mockCPUInfos_DualSocket_4CPUsPerSocket_HT),
			}
			d, err := New(testr.New(t), prov, &Config{
				DriverName:           testDriverName,
				NodeName:             testNodeName,
				ReservedCPUs:         cpuset.New(),
				CPUDeviceMode:        device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy:     device.GROUP_BY_NUMA_NODE,
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
			})
			require.NoError(t, err)
			require.Len(t, d.topology.deviceSlices, 1)
			require.Len(t, d.topology.deviceSlices[0], 2)
			for _, dev := range d.topology.deviceSlices[0] {
				policy := dev.Capacity[device.CPUResourceQualifiedName].RequestPolicy
				require.Equalf(t, tc.wantPolicy, policy != nil, "device %q request policy", dev.Name)
			}
		})
	}
}

func TestNewRefusesAReservationThatSplitsACoreUnderFullPhysicalCPUsOnly(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		reserved             cpuset.CPUSet
		fullPhysicalCPUsOnly bool
		wantErr              string
	}{
		{
			name:                 "one thread of a core",
			reserved:             cpuset.New(0, 1, 9),
			fullPhysicalCPUsOnly: true,
			wantErr:              `reservedCPUs "0-1,9" splits physical cores, which fullPhysicalCPUsOnly does not allow: reserve the sibling threads of CPUs 0 too, or leave those CPUs out`,
		},
		{name: "whole cores", reserved: cpuset.New(0, 8), fullPhysicalCPUsOnly: true},
		{name: "a CPU outside the topology splits nothing", reserved: cpuset.New(0, 8, 99), fullPhysicalCPUsOnly: true},
		{name: "one thread of a core without the option", reserved: cpuset.New(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prov := Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: wholeCoreCPUInfos},
				SysFS:   testSysFS(wholeCoreCPUInfos),
			}
			_, err := New(testr.New(t), prov, &Config{
				DriverName:           testDriverName,
				NodeName:             testNodeName,
				ReservedCPUs:         tc.reserved,
				CPUDeviceMode:        device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy:     device.GROUP_BY_NUMA_NODE,
				FullPhysicalCPUsOnly: tc.fullPhysicalCPUsOnly,
			})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestWaitForRegistration covers an unexported function, which we would normally
// reach through its caller instead. CPUDriver.Start does a lot and takes no
// injectable dependencies, so there is no seam to reach this behaviour from
// outside the package today. Testing it directly is the exception rather than the
// pattern, and it can move behind Start once Start is separable.
func TestWaitForRegistration(t *testing.T) {
	const registrarPath = "/var/lib/kubelet/plugins_registry"
	rejection := func(reason string) *registerapi.RegistrationStatus {
		return &registerapi.RegistrationStatus{Error: reason}
	}

	for _, tc := range []struct {
		name string
		// status answers one poll. Calling cancel ends the wait the way a shutdown does.
		status       func(cancel context.CancelFunc, call int32) *registerapi.RegistrationStatus
		wantErr      bool
		wantErrIs    error
		wantInErr    []string
		wantNotInErr []string
		minCalls     int32
	}{
		{
			name: "registered on the first poll",
			status: func(context.CancelFunc, int32) *registerapi.RegistrationStatus {
				return &registerapi.RegistrationStatus{PluginRegistered: true}
			},
		},
		{
			name: "the newest rejection is the one reported",
			status: func(_ context.CancelFunc, call int32) *registerapi.RegistrationStatus {
				if call == 1 {
					return rejection("unsupported plugin API version")
				}
				return rejection("driver name already registered")
			},
			wantErr:      true,
			wantInErr:    []string{"driver name already registered"},
			wantNotInErr: []string{"unsupported plugin API version"},
			minCalls:     2,
		},
		{
			name: "unregistered with no reason given",
			status: func(context.CancelFunc, int32) *registerapi.RegistrationStatus {
				return &registerapi.RegistrationStatus{}
			},
			wantErr:   true,
			wantInErr: []string{"reported no reason"},
		},
		{
			name: "kubelet never reported a status",
			status: func(context.CancelFunc, int32) *registerapi.RegistrationStatus {
				return nil
			},
			wantErr:   true,
			wantInErr: []string{registrarPath},
		},
		{
			name: "a shutdown is not diagnosed",
			status: func(cancel context.CancelFunc, _ int32) *registerapi.RegistrationStatus {
				cancel()
				return nil
			},
			wantErr:      true,
			wantErrIs:    context.Canceled,
			wantNotInErr: []string{registrarPath},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			registrar := &mockKubeletPlugin{
				statusFunc: func(call int32) *registerapi.RegistrationStatus {
					return tc.status(cancel, call)
				},
			}

			err := waitForRegistration(ctx, registrar, registrarPath, time.Millisecond, 200*time.Millisecond)
			if tc.minCalls > 0 {
				require.GreaterOrEqual(t, registrar.statusCalls.Load(), tc.minCalls, "too few polls for this case to prove anything")
			}
			if !tc.wantErr {
				require.NoError(t, err)
				require.Equal(t, int32(1), registrar.statusCalls.Load(), "a registered plugin should end the wait right away")
				return
			}
			require.Error(t, err)
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
			}
			for _, want := range tc.wantInErr {
				require.ErrorContains(t, err, want)
			}
			for _, unwanted := range tc.wantNotInErr {
				require.NotContains(t, err.Error(), unwanted)
			}
		})
	}
}

func TestGenerateShortID(t *testing.T) {
	testCases := []struct {
		name   string
		length int
	}{
		{name: "zero length", length: 0},
		{name: "single char", length: 1},
		{name: "opIDLen", length: opIDLen},
		{name: "large", length: 64},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			id := generateShortID(tc.length)
			require.Len(t, id, tc.length)
			if tc.length == 0 {
				return
			}
			require.True(t, isHex(id))
		})
	}
}

func TestGenerateShortIDUnique(t *testing.T) {
	a := generateShortID(opIDLen)
	b := generateShortID(opIDLen)
	require.NotEqual(t, a, b)
}

func isHex(s string) bool {
	s = strings.ToLower(s)
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b-'0' < 10 || b-'a' < 6 {
			continue
		}
		return false
	}
	return true
}

func TestKubeletDirDerivation(t *testing.T) {
	const driverName = "cpu.dra.example.com"

	// The registrar, plugins, and per-driver socket directories are always
	// derived from the kubelet root, both at the default location and when the
	// root is relocated. filepath.Join also cleans a trailing slash.
	for _, tc := range []struct {
		name          string
		root          string
		wantRegistrar string
		wantPlugins   string
		wantData      string
	}{
		{
			name:          "default kubelet root",
			root:          "/var/lib/kubelet",
			wantRegistrar: "/var/lib/kubelet/plugins_registry",
			wantPlugins:   "/var/lib/kubelet/plugins",
			wantData:      "/var/lib/kubelet/plugins/cpu.dra.example.com",
		},
		{
			name:          "relocated kubelet root with trailing slash",
			root:          "/mnt/fast/k8s/kubelet/",
			wantRegistrar: "/mnt/fast/k8s/kubelet/plugins_registry",
			wantPlugins:   "/mnt/fast/k8s/kubelet/plugins",
			wantData:      "/mnt/fast/k8s/kubelet/plugins/cpu.dra.example.com",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cp := &CPUDriver{driverName: driverName, kubeletRootDir: tc.root}
			require.Equal(t, tc.wantRegistrar, registrarDir(cp.kubeletRootDir))
			require.Equal(t, tc.wantPlugins, filepath.Join(cp.kubeletRootDir, "plugins"))
			require.Equal(t, tc.wantData, pluginDataDir(cp.kubeletRootDir, cp.driverName))
			// The socket dir must not be the shared plugins parent, per the
			// kubeletplugin "not shared" contract.
			require.NotEqual(t, filepath.Join(cp.kubeletRootDir, "plugins"), pluginDataDir(cp.kubeletRootDir, cp.driverName))
		})
	}
}

// The suffix the helper appends is fixed, so the arithmetic is what decides
// whether a root is usable.
func TestSocketPathFits(t *testing.T) {
	const driver = "dra.cpu"
	// The registrar path is the root plus "/plugins_registry/dra.cpu-reg.sock".
	suffix := len(filepath.Join("/", "plugins_registry", driver+"-reg.sock"))

	for _, tc := range []struct {
		name    string
		root    string
		wantErr bool
	}{
		{"default root", "/var/lib/kubelet", false},
		{"relocated root", "/mnt/fast/k8s/kubelet", false},
		{"exactly at the limit", "/" + strings.Repeat("x", unixPathMax-suffix-1), false},
		{"one byte over", "/" + strings.Repeat("x", unixPathMax-suffix), true},
		// Bytes, not characters: this is well under any character count that
		// would fit, and still too long for sun_path.
		{"multibyte, short in characters", "/" + strings.Repeat("界", 30), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSocketPathFits(tc.root, driver)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), "Unix socket path")
		})
	}
}

// Pins the check to Start, not only to itself. It runs before Start touches the
// filesystem or the kubelet, so it needs no more than the fields the check reads.
func TestStartRefusesARootWithNoRoomForTheSocket(t *testing.T) {
	cp := &CPUDriver{
		kubeletRootDir: "/" + strings.Repeat("x", unixPathMax),
		driverName:     "dra.cpu",
	}

	_, err := cp.Start(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "Unix socket path")
}

func TestStartRefusesWithoutCancelRun(t *testing.T) {
	// Not t.TempDir: its path carries the test name and could fail the socket check first.
	root, err := os.MkdirTemp("", "kubelet")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cp := &CPUDriver{kubeletRootDir: root, driverName: "dra.cpu"}

	_, err = cp.Start(t.Context())

	require.ErrorContains(t, err, "CancelRun")
	require.NoDirExists(t, pluginDataDir(root, cp.driverName), "Start must refuse before it writes to the node")
}

// fakeNRIStub stands in for the NRI stub; only Run and Stop are called.
type fakeNRIStub struct {
	stub.Stub
	runFunc  func(ctx context.Context) error
	stopFunc func()
}

func (f *fakeNRIStub) Run(ctx context.Context) error { return f.runFunc(ctx) }

func (f *fakeNRIStub) Stop() { f.stopFunc() }

// fakeEventBroadcaster stands in for the event broadcaster; only Shutdown is called.
type fakeEventBroadcaster struct {
	record.EventBroadcaster
	shutDown bool
}

func (f *fakeEventBroadcaster) Shutdown() { f.shutDown = true }

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestStopStopsTheNRIPluginLast(t *testing.T) {
	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()

	var stops []string
	var nriCtx context.Context
	nriCtxs := make(chan context.Context, 1)
	runReturned := make(chan struct{})
	events := &fakeEventBroadcaster{}
	cp := &CPUDriver{health: newHealthTracker(), eventBroadcaster: events}
	cp.draPlugin = &mockKubeletPlugin{stopFunc: func() {
		require.True(t, isClosed(cp.health.stopCh), "the health tracker stops before the DRA plugin")
		require.True(t, events.shutDown, "the event recorder stops with the health tracker, before the DRA plugin")
		stops = append(stops, "dra")
	}}
	cp.nriPlugin = &fakeNRIStub{
		runFunc: func(ctx context.Context) error {
			defer close(runReturned)
			nriCtxs <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
		stopFunc: func() {
			require.Error(t, nriCtx.Err(), "the NRI context is cancelled before the stub stops")
			stops = append(stops, "nri")
		},
	}
	go cp.healthResendLoop(runCtx)
	asyncErr := make(chan error, 1)
	cp.startNRIPlugin(runCtx, asyncErr)
	nriCtx = <-nriCtxs

	cancelRun()
	require.NoError(t, nriCtx.Err(), "a cancelled run context must not stop the NRI plugin")

	cp.Stop()
	require.Equal(t, []string{"dra", "nri"}, stops)
	<-runReturned
	require.Empty(t, asyncErr, "stopping the NRI plugin is not a failure")
}
