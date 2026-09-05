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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
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

// Tiny durations so the backoff and the healthy-run threshold do not slow the
// suite down; only their relative order (initial < max, and both far below
// healthyRun) matters to the behaviour under test.
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

func TestRunNRIPluginWithRetry_BacksOffBetweenAttempts(t *testing.T) {
	// The original bug: a down socket fails to dial instantly, so without a
	// delay every attempt is spent within microseconds. Assert real time
	// elapses between attempts, proportional to the number of retries.
	ctx := context.Background()
	var timestamps []time.Time
	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			timestamps = append(timestamps, time.Now())
			return fmt.Errorf("connection refused")
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, 4, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.Error(t, err)
	require.Len(t, timestamps, 4)
	for i := 1; i < len(timestamps); i++ {
		require.GreaterOrEqual(t, timestamps[i].Sub(timestamps[i-1]), testRetryInitialBackoff,
			"attempt %d ran without waiting for the backoff", i)
	}
}

func TestRunNRIPluginWithRetry_BackoffDoublesUpToTheCap(t *testing.T) {
	ctx := context.Background()
	var gaps []time.Duration
	var last time.Time
	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			now := time.Now()
			if !last.IsZero() {
				gaps = append(gaps, now.Sub(last))
			}
			last = now
			return fmt.Errorf("connection refused")
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, 5, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	require.Error(t, err)
	require.Len(t, gaps, 4)
	// Doubles: 1x, 2x, then capped at testRetryMaxBackoff (4x the initial).
	require.GreaterOrEqual(t, gaps[1], 2*testRetryInitialBackoff)
	require.GreaterOrEqual(t, gaps[2], testRetryMaxBackoff)
	require.GreaterOrEqual(t, gaps[3], testRetryMaxBackoff)
}

func TestRunNRIPluginWithRetry_HealthyRunResetsTheBudget(t *testing.T) {
	// A connection that lasted a while before failing again must not be
	// charged against the crash-loop budget from a fast failure long before it
	// connected. maxAttempts is 2: without the reset, the one fast failure
	// below plus the failure after the healthy run would exhaust it and the
	// function would return before a third call ever happens.
	const maxAttempts = 2
	const healthyRun = 2 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var calls int
	runner := &mockNRIRunner{
		runFunc: func(ctx context.Context) error {
			calls++
			switch calls {
			case 1:
				return fmt.Errorf("crash loop")
			case 2:
				time.Sleep(2 * healthyRun)
				return fmt.Errorf("fresh problem")
			default:
				cancel()
				return context.Canceled
			}
		},
	}

	err := runNRIPluginWithRetry(ctx, runner, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, healthyRun)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 3, calls, "the healthy second run must have bought a fresh attempt budget")
}

// TestNewReportsResourceSliceCountUnderExposePCIeRoots: exposePCIeRoots halves
// devicesPerResourceSlice (128 -> 64) invisibly to the config, which can
// double how many ResourceSlices a node publishes; New must report the actual
// count reached rather than leave it something only a slice count in the API
// would reveal.
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

// TestSeedAllocationStoreFromDiskRecoversPriorPlacements: a kubelet replaying
// Prepare for an already-running claim after a restart must not be told the
// claim's CPUs are free, or a different claim allocated afresh in that window
// could pick the same ones.
func TestSeedAllocationStoreFromDiskRecoversPriorPlacements(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-recovered")
	cdiMgr := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{claimUID: cpuset.New(0, 1)})
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		cdiMgr:             cdiMgr,
	}

	d.seedAllocationStoreFromDisk(logger)

	require.Equal(t, 1, cdiMgr.refreshCalls, "must refresh the CDI cache before reading it")
	got, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok, "the recovered claim must be reserved")
	require.Equal(t, cpuset.New(0, 1), got)

	// A Prepare replayed for a different, new claim must not be able to land on
	// the recovered claim's CPUs.
	require.Error(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-new", cpuset.New(0), false))
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, "claim-new", cpuset.New(2), false))
}

// TestSeedAllocationStoreFromDiskSkipsAConflictingRecordWithoutFailingStartup:
// self-consistent CDI specs never conflict, but a corrupted or hand-edited one
// must not cost every other recovered claim its own recovery.
func TestSeedAllocationStoreFromDiskSkipsAConflictingRecordWithoutFailingStartup(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	cdiMgr := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
		"claim-good":       cpuset.New(0, 1),
		"claim-conflicted": cpuset.New(0, 1),
	})
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		cdiMgr:             cdiMgr,
	}

	d.seedAllocationStoreFromDisk(logger)

	// One of the two conflicting claims won recovery; the other is absent
	// rather than the whole recovery having failed.
	_, goodOK := d.cpuAllocationStore.GetResourceClaimAllocation("claim-good")
	_, conflictedOK := d.cpuAllocationStore.GetResourceClaimAllocation("claim-conflicted")
	require.True(t, goodOK != conflictedOK, "exactly one of the conflicting claims must have been recovered")
}

// TestSeedAllocationStoreFromDiskToleratesARefreshFailure: a failed refresh
// must not panic or otherwise stop Start from proceeding.
func TestSeedAllocationStoreFromDiskToleratesARefreshFailure(t *testing.T) {
	logger := testr.New(t)
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: []cpuinfo.CPUInfo{{CpuID: 0}}}).GetCPUTopology(logger)
	require.NoError(t, err)

	cdiMgr := newMockCdiMgr()
	cdiMgr.refreshError = fmt.Errorf("cannot read CDI spec directory")
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		cdiMgr:             cdiMgr,
	}

	require.NotPanics(t, func() { d.seedAllocationStoreFromDisk(logger) })
	require.True(t, d.cpuAllocationStore.GetSharedCPUs().Equals(cpuset.New(0)), "nothing must have been recovered")
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

func TestValidateReservedCPUsAlignment(t *testing.T) {
	// Two 2-way SMT cores: (0,2) and (1,3).
	topo := &cpuinfo.CPUTopology{CPUDetails: cpuinfo.CPUDetails{
		0: {CpuID: 0, CoreID: 0, SocketID: 0},
		1: {CpuID: 1, CoreID: 1, SocketID: 0},
		2: {CpuID: 2, CoreID: 0, SocketID: 0},
		3: {CpuID: 3, CoreID: 1, SocketID: 0},
	}}

	require.NoError(t, validateReservedCPUsAlignment(topo, cpuset.New(0, 2)), "whole core reserved")
	require.NoError(t, validateReservedCPUsAlignment(topo, cpuset.New()), "nothing reserved")

	err := validateReservedCPUsAlignment(topo, cpuset.New(0))
	require.Error(t, err)
	require.Contains(t, err.Error(), "splits physical cores")
	require.Contains(t, err.Error(), "0")
}

func TestNewRejectsReservedCPUsSplittingACoreUnderFullPhysicalCPUsOnly(t *testing.T) {
	infos := []cpuinfo.CPUInfo{
		{CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 2},
		{CpuID: 1, CoreID: 1, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 3},
		{CpuID: 2, CoreID: 0, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 0},
		{CpuID: 3, CoreID: 1, SocketID: 0, NUMANodeID: 0, SiblingCPUID: 1},
	}
	prov := Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:   testSysFS(infos),
	}

	_, err := New(testr.New(t), prov, &Config{
		DriverName:           testDriverName,
		NodeName:             testNodeName,
		CPUDeviceMode:        devattr.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:     devattr.GROUP_BY_NUMA_NODE,
		ReservedCPUs:         cpuset.New(0),
		FullPhysicalCPUsOnly: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "splits physical cores")

	_, err = New(testr.New(t), prov, &Config{
		DriverName:           testDriverName,
		NodeName:             testNodeName,
		CPUDeviceMode:        devattr.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:     devattr.GROUP_BY_NUMA_NODE,
		ReservedCPUs:         cpuset.New(0, 2),
		FullPhysicalCPUsOnly: true,
	})
	require.NoError(t, err, "reserving both siblings does not split the core")
}

// TestPlacementChangesAreSerialized drives every hook that reads or writes a
// placement against Synchronize, which replaces all three stores wholesale.
//
// Under -race this is what catches the pointer swap going unsynchronized: before
// applyMu, a hook could be reading the store Synchronize was in the middle of
// replacing. It also covers the CDI manager, which the same lock is what keeps
// serial.
func TestPlacementChangesAreSerialized(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-uid-1")
	claimedCPUs := cpuset.New(0, 1)
	pod := &api.PodSandbox{Id: "pod-id-1", Name: "pod", Namespace: "ns", Uid: "pod-uid-1"}
	ctr := &api.Container{
		Id:           "ctr-id-1",
		PodSandboxId: pod.Id,
		Name:         "ctr",
		Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, claimedCPUs.String())},
	}

	allocationStore := store.NewCPUAllocation(topo, cpuset.New())
	require.NoError(t, allocationStore.ReserveResourceClaimAllocation(logger, claimUID, claimedCPUs, false))

	d := &CPUDriver{
		cdiMgr:             newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{claimUID: claimedCPUs}),
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: allocationStore,
		claimTracker:       store.NewClaimTracker(),
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
	}

	ctx := context.Background()
	claims := []kubeletplugin.NamespacedObject{{UID: claimUID}}
	var wg sync.WaitGroup
	for range 30 {
		wg.Add(4)
		// Every one of these may legitimately fail depending on the interleaving;
		// what must not happen is a race or a torn view of the stores.
		go func() { defer wg.Done(); _, _ = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{ctr}) }()
		go func() { defer wg.Done(); _, _, _ = d.CreateContainer(ctx, pod, ctr) }()
		go func() { defer wg.Done(); _, _ = d.StopContainer(ctx, pod, ctr) }()
		go func() { defer wg.Done(); _, _ = d.UnprepareResourceClaims(ctx, claims) }()
	}
	wg.Wait()

	// Whatever order they ran in, the node still accounts for every CPU exactly
	// once: prepared and shared partition the allocatable set.
	prepared := d.cpuAllocationStore.GetPreparedCPUs()
	shared := d.cpuAllocationStore.GetSharedCPUs()
	require.True(t, prepared.Intersection(shared).IsEmpty(), "a CPU is both claimed and shared")
	require.True(t, prepared.Union(shared).Equals(allCPUs), "CPUs went missing")
}
