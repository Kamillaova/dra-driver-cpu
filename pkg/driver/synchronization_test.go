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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

type allocationRecorder struct {
	Recorder
	state cpumetrics.AllocationState
}

func (r *allocationRecorder) SetAllocationState(state cpumetrics.AllocationState) { r.state = state }

// cancelWhenLogged returns a context that is cancelled as soon as a line
// holding msg is logged through it, so that a call is cancelled after it has
// checked its context once.
func cancelWhenLogged(t *testing.T, msg string) context.Context {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return logr.NewContext(ctx, funcr.New(func(_, args string) {
		if strings.Contains(args, msg) {
			cancel()
		}
	}, funcr.Options{}))
}

const syncBegins = "begin: synchronize state with the runtime"

func TestSynchronizeFailurePreservesState(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	recorder := &allocationRecorder{Recorder: cpumetrics.Noop()}
	d.metrics = recorder
	d.refreshAllocationMetrics()
	oldMetrics := recorder.state
	oldCPU, oldPods := d.allocationState, d.podState
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"full": cpuset.New(0, 1, 2, 3)})
	ctx := cancelWhenLogged(t, syncBegins)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{
		{Id: "exclusive", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_full=0-3"}},
		{Id: "shared", Name: "sidecar", PodSandboxId: pod.Id},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, updates)
	require.Same(t, oldCPU, d.allocationState)
	require.Same(t, oldPods, d.podState)
	require.Equal(t, oldMetrics, recorder.state)
	require.True(t, d.allocationState.GetPreparedCPUs().IsEmpty())
	require.False(t, d.allocationState.IsOwner("full", types.UID(pod.Uid), "app"))
	require.False(t, d.hasSynchronized)
}

func TestSynchronizeRetainsPreparedClaimAndOwner(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	claim := individualMetricsClaim("pending", "cpudev0")
	prepared, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, prepared[claim.UID].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	app := &api.Container{Id: "old", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_pending=0"}}
	_, _, err = d.CreateContainer(t.Context(), pod, app)
	require.NoError(t, err)
	_, err = d.StopContainer(t.Context(), pod, app)
	require.NoError(t, err)
	// The old shared-container ID must also be discarded by recovery.
	d.podState.SetContainerState("old-pod", store.NewContainerState("old-shared", "stale"))
	updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
		{Id: "shared", Name: "sidecar", PodSandboxId: pod.Id},
	})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "shared", updates[0].ContainerId)
	require.Equal(t, "1-3", updates[0].Linux.Resources.Cpu.Cpus)
	cpus, found := d.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, found)
	require.Equal(t, "0", cpus.String())
	require.True(t, d.allocationState.IsOwner(claim.UID, types.UID(pod.Uid), "app"))
	require.Nil(t, d.podState.GetContainerState("old-pod", "old-shared"))
	// Ownership survives an empty snapshot as well.
	_, err = d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	_, _, err = d.CreateContainer(t.Context(), &api.PodSandbox{Uid: "other"}, app)
	require.Error(t, err)
	app.Id = "replacement"
	_, _, err = d.CreateContainer(t.Context(), pod, app)
	require.NoError(t, err)
}

// A reconnect judges each container against the records this process keeps. A
// record never changes, and only the container that fails it loses its CPUs.
func TestWarmSynchronizeContainsConflictingClaims(t *testing.T) {
	testCases := []struct {
		name     string
		mutate   func(*mockCdiMgr, *api.Container)
		wantCPUs string
	}{
		{
			// The CDI spec is written from the record, not the other way round.
			name: "missing CDI",
			mutate: func(cdi *mockCdiMgr, _ *api.Container) {
				delete(cdi.devices, getCDIDeviceName("a"))
			},
			wantCPUs: "0",
		},
		{
			name: "changed CPU",
			mutate: func(_ *mockCdiMgr, app *api.Container) {
				app.Env = []string{"DRA_CPUSET_a=1"}
			},
			wantCPUs: "1-3",
		},
		{
			name: "different owner",
			mutate: func(_ *mockCdiMgr, app *api.Container) {
				app.Name = "other"
			},
			wantCPUs: "1-3",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
			_, err := d.Synchronize(t.Context(), nil, nil)
			require.NoError(t, err)
			cdi := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)})
			d.cdiMgr = cdi
			logger := testr.New(t)
			require.NoError(t, d.allocationState.Reserve(logger, "a", oneRequest(cpuset.New(0)), false))
			_, err = d.allocationState.Bind(logger, "pod", "app", nil, wholeClaim("a", cpuset.New(0)))
			require.NoError(t, err)
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
			app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
			testCase.mutate(cdi, app)

			updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app})
			require.NoError(t, err)
			require.Equal(t, map[string]string{"app": testCase.wantCPUs}, cpusByContainer(updates))
			require.Equal(t, "0", d.allocationState.GetPreparedCPUs().String())
			require.True(t, d.allocationState.IsOwner("a", "pod", "app"))
			state := d.podState.GetContainerState("pod", app.Name)
			require.NotNil(t, state)
			require.Equal(t, testCase.wantCPUs == "0", state.HasExclusiveCPUAllocation())
		})
	}
}

// A claim the runtime and CDI both report, but this process never prepared, is
// skipped instead of failing Synchronize: it gains no reservation, and the
// prepared claim it displaced keeps its allocation.
func TestWarmSynchronizeSkipsUnknownAuthenticatedClaim(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	cdi := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)})
	cdi.devices[getCDIDeviceName("b")] = "DRA_CPUSET_b=1"
	d.cdiMgr = cdi
	logger := testr.New(t)
	require.NoError(t, d.allocationState.Reserve(logger, "a", oneRequest(cpuset.New(0)), false))
	_, err = d.allocationState.Bind(logger, "pod", "app", nil, wholeClaim("a", cpuset.New(0)))
	require.NoError(t, err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_b=1"}}

	_, err = d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app})
	require.NoError(t, err)
	require.Equal(t, "0", d.allocationState.GetPreparedCPUs().String())
	_, prepared := d.allocationState.GetResourceClaimAllocation("b")
	require.False(t, prepared)
	state := d.podState.GetContainerState("pod", "app")
	require.NotNil(t, state)
	require.False(t, state.HasExclusiveCPUAllocation())
}

func TestWarmSynchronizeDoesNotResurrectUnpreparedClaim(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	claim := individualMetricsClaim("a", "cpudev0")
	results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	ctr := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
	_, _, err = d.CreateContainer(t.Context(), pod, ctr)
	require.NoError(t, err)
	_, err = d.StopContainer(t.Context(), pod, ctr)
	require.NoError(t, err)
	unprepared, err := d.UnprepareResourceClaims(t.Context(), []kubeletplugin.NamespacedObject{{UID: claim.UID}})
	require.NoError(t, err)
	require.NoError(t, unprepared[claim.UID])
	_, err = d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err)
	require.True(t, d.allocationState.GetPreparedCPUs().IsEmpty())
	require.False(t, d.allocationState.IsOwner(claim.UID, types.UID(pod.Uid), ctr.Name))
	// Even stale CDI cannot override an authoritative in-process release, and the
	// stale claim must not block recovery of the other containers on the node.
	d.cdiMgr.(*mockCdiMgr).devices[getCDIDeviceName("a")] = "DRA_CPUSET_a=0"
	other := &api.Container{Id: "other", Name: "other", PodSandboxId: pod.Id}
	_, err = d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{ctr, other})
	require.NoError(t, err)
	require.True(t, d.allocationState.GetPreparedCPUs().IsEmpty())
	require.False(t, d.allocationState.IsOwner(claim.UID, types.UID(pod.Uid), ctr.Name))
	for _, name := range []string{"app", "other"} {
		state := d.podState.GetContainerState("pod", name)
		require.NotNil(t, state, "container %q was not recovered", name)
		require.False(t, state.HasExclusiveCPUAllocation(), "container %q must not be exclusive", name)
	}
}

type pausedRecorder struct {
	Recorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *pausedRecorder) SetAllocationState(state cpumetrics.AllocationState) {
	r.once.Do(func() { close(r.entered); <-r.release })
	r.Recorder.SetAllocationState(state)
}

func requireStateLockHeld(t *testing.T, d *CPUDriver) {
	t.Helper()
	if d.stateMu.mu.TryLock() {
		d.stateMu.mu.Unlock()
		t.Fatal("state lock is not held while Synchronize is paused")
	}
}

func TestSynchronizeSerializesPrepare(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	claim := individualMetricsClaim("pending", "cpudev0")
	cdi := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"running": cpuset.New(1)})
	d.cdiMgr = cdi
	d.seedAllocationStoreFromDisk(testr.New(t))
	// Synchronize reads no CDI spec, so pause it where it publishes its metrics.
	paused := &pausedRecorder{Recorder: d.metrics, entered: make(chan struct{}), release: make(chan struct{})}
	d.metrics = paused
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	syncDone := make(chan error, 1)
	go func() {
		_, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
			{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_running=1"}},
		})
		syncDone <- err
	}()
	<-paused.entered
	requireStateLockHeld(t, d)

	prepareDone := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
		if err == nil {
			err = results[claim.UID].Err
		}
		prepareDone <- err
	}()
	<-started
	close(paused.release)
	require.NoError(t, <-syncDone)
	require.NoError(t, <-prepareDone)

	cpus, found := d.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, found)
	require.Equal(t, "0", cpus.String())
	require.Equal(t, "DRA_CPUSET_pending=0", cdi.devices[getCDIDeviceName(claim.UID)])
}

func TestSynchronizeSerializesUnprepare(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	claim := individualMetricsClaim("pending", "cpudev0")
	cdi := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"running": cpuset.New(1)})
	d.cdiMgr = cdi
	prepared, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, prepared[claim.UID].Err)
	// A reconnect reads no CDI spec, so pause it where it publishes its metrics.
	paused := &pausedRecorder{Recorder: d.metrics, entered: make(chan struct{}), release: make(chan struct{})}
	d.metrics = paused

	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	syncDone := make(chan error, 1)
	go func() {
		_, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
			{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_running=1"}},
		})
		syncDone <- err
	}()
	<-paused.entered
	requireStateLockHeld(t, d)

	unprepareDone := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		results, err := d.UnprepareResourceClaims(t.Context(), []kubeletplugin.NamespacedObject{{UID: claim.UID}})
		if err == nil {
			err = results[claim.UID]
		}
		unprepareDone <- err
	}()
	<-started
	close(paused.release)
	require.NoError(t, <-syncDone)
	require.NoError(t, <-unprepareDone)

	_, found := d.allocationState.GetResourceClaimAllocation(claim.UID)
	require.False(t, found)
	require.NotContains(t, cdi.devices, getCDIDeviceName(claim.UID))
}

func TestSynchronizeSerializesCancelledPrepare(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	claim := individualMetricsClaim("pending", "cpudev0")
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"running": cpuset.New(1)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	// Synchronize reads no CDI spec, so pause it where it publishes its metrics.
	paused := &pausedRecorder{Recorder: d.metrics, entered: make(chan struct{}), release: make(chan struct{})}
	d.metrics = paused
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	syncDone := make(chan error, 1)
	go func() {
		_, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
			{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_running=1"}},
		})
		syncDone <- err
	}()
	<-paused.entered
	requireStateLockHeld(t, d)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	prepareDone := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
		if err == nil {
			err = results[claim.UID].Err
		}
		prepareDone <- err
	}()
	<-started
	cancel()
	close(paused.release)
	require.NoError(t, <-syncDone)
	require.ErrorIs(t, <-prepareDone, context.Canceled)

	_, found := d.allocationState.GetResourceClaimAllocation(claim.UID)
	require.False(t, found)
}

func TestSynchronizeCancelledBeforeCommit(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	old := d.allocationState
	ctx := cancelWhenLogged(t, syncBegins)
	_, err := d.Synchronize(ctx, []*api.PodSandbox{{Id: "sandbox", Uid: "pod"}}, []*api.Container{{Id: "app", Name: "app", PodSandboxId: "sandbox", Env: []string{fmt.Sprintf("%s_a=0", cdiEnvVarPrefix)}}})
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, old, d.allocationState)
	require.False(t, d.hasSynchronized)
	_, bound := d.allocationState.Owner("a")
	require.False(t, bound, "the binding of a cancelled Synchronize is not swapped in")
}

// A claim prepared before a restart whose container does not exist yet keeps
// its CPUs: Start seeds the store from every record on disk, and Synchronize
// adds bindings to a copy of that store, never rebuilding it from the
// containers the runtime lists.
func TestSeededClaimWithoutContainerSurvivesSynchronize(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"idle": cpuset.New(0), "busy": cpuset.New(1)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	busy := &api.Container{Id: "c-busy", Name: "busy", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_busy=1"}}

	// The first attempt is a fresh process, the second a reconnect.
	for attempt := 1; attempt <= 2; attempt++ {
		updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{busy})
		require.NoError(t, err)
		require.Equal(t, map[string]string{"c-busy": "1"}, cpusByContainer(updates), "attempt %d", attempt)
		require.True(t, d.allocationState.IsOwner("busy", "pod-uid", "busy"), "attempt %d", attempt)
		_, idleBound := d.allocationState.Owner("idle")
		require.False(t, idleBound, "attempt %d", attempt)
		require.Equal(t, "0-1", d.allocationState.GetPreparedCPUs().String(), "attempt %d: the claim no container names keeps its CPUs", attempt)
	}

	taken := individualMetricsClaim("taken", "cpudev0")
	free := individualMetricsClaim("free", "cpudev2")
	results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{taken, free})
	require.NoError(t, err)
	require.Error(t, results[taken.UID].Err, "a new claim cannot take the CPUs of a claim no container names")
	require.NoError(t, results[free.UID].Err)
}

func TestWarmSynchronizeIgnoresInvalidEnvFromNonOwner(t *testing.T) {
	for _, missingCDI := range []bool{false, true} {
		t.Run(fmt.Sprintf("missingCDI=%v", missingCDI), func(t *testing.T) {
			d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
			_, err := d.Synchronize(t.Context(), nil, nil)
			require.NoError(t, err)
			claim := individualMetricsClaim("a", "cpudev0")
			results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			require.NoError(t, results[claim.UID].Err)
			_, err = d.allocationState.Bind(testr.New(t), "real-pod", "app", nil, wholeClaim(claim.UID, cpuset.New(0)))
			require.NoError(t, err)
			if missingCDI {
				delete(d.cdiMgr.(*mockCdiMgr).devices, getCDIDeviceName(claim.UID))
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "other-pod"}
			updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{{Id: "other", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=1"}}})
			require.NoError(t, err, "an untrusted env must not break node-wide recovery")
			require.Equal(t, "0", d.allocationState.GetPreparedCPUs().String())
			require.True(t, d.allocationState.IsOwner(claim.UID, "real-pod", "app"))
			require.Len(t, updates, 1)
			require.Equal(t, "1-3", updates[0].Linux.Resources.Cpu.Cpus)
		})
	}
}

func TestWarmSynchronizeRetainsKnownSharedContainerWithMalformedEnv(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	shared := &api.Container{Id: "shared", Name: "sidecar", PodSandboxId: pod.Id}
	_, _, err = d.CreateContainer(t.Context(), pod, shared)
	require.NoError(t, err)
	// Another NRI plugin may change the environment after our CreateContainer.
	shared.Env = []string{"DRA_CPUSET_other=bad"}
	_, err = d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{shared})
	require.NoError(t, err)
	claim := individualMetricsClaim("a", "cpudev0")
	prepared, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, prepared[claim.UID].Err)
	_, updates, err := d.CreateContainer(t.Context(), pod, &api.Container{
		Id: "exclusive", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"},
	})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "shared", updates[0].ContainerId)
	require.Equal(t, "1-3", updates[0].Linux.Resources.Cpu.Cpus)
}

func TestWarmSynchronizeMalformedContainerIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		podUID        string
		containerName string
		containerID   string
		env           []string
		known         bool
		present       bool
		wantRetain    bool
	}{
		{name: "unknown container", podUID: "pod", containerName: "sidecar", containerID: "shared", env: []string{"DRA_CPUSET_other=bad"}, present: true},
		{name: "same name replacement", podUID: "pod", containerName: "sidecar", containerID: "replacement", env: []string{"DRA_CPUSET_other=bad"}, known: true, present: true},
		{name: "different pod", podUID: "other-pod", containerName: "sidecar", containerID: "shared", env: []string{"DRA_CPUSET_other=bad"}, known: true, present: true},
		{name: "different name", podUID: "pod", containerName: "other-name", containerID: "shared", env: []string{"DRA_CPUSET_other=bad"}, known: true, present: true},
		{name: "absent from snapshot", podUID: "pod", known: true},
		{name: "missing equals", podUID: "pod", containerName: "sidecar", containerID: "shared", env: []string{"DRA_CPUSET_other"}, known: true, present: true, wantRetain: true},
		{name: "malformed before valid claim", podUID: "pod", containerName: "sidecar", containerID: "shared", env: []string{"DRA_CPUSET_other=bad", "DRA_CPUSET_a=0"}, known: true, present: true, wantRetain: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
			_, err := d.Synchronize(t.Context(), nil, nil)
			require.NoError(t, err)
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
			if tc.known {
				_, _, err = d.CreateContainer(t.Context(), pod, &api.Container{Id: "shared", Name: "sidecar", PodSandboxId: pod.Id})
				require.NoError(t, err)
			}
			snapshotPod := &api.PodSandbox{Id: pod.Id, Uid: tc.podUID}
			var containers []*api.Container
			if tc.present {
				containers = append(containers, &api.Container{Id: tc.containerID, Name: tc.containerName, PodSandboxId: pod.Id, Env: tc.env})
			}
			// A healthy container must still be recovered alongside malformed input.
			containers = append(containers, &api.Container{Id: "healthy", Name: "healthy", PodSandboxId: pod.Id})
			_, err = d.Synchronize(t.Context(), []*api.PodSandbox{snapshotPod}, containers)
			require.NoError(t, err)
			claim := individualMetricsClaim("a", "cpudev0")
			prepared, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			require.NoError(t, prepared[claim.UID].Err)
			_, updates, err := d.CreateContainer(t.Context(), snapshotPod, &api.Container{
				Id: "exclusive", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"},
			})
			require.NoError(t, err)
			got := make(map[string]string)
			for _, update := range updates {
				got[update.ContainerId] = update.Linux.Resources.Cpu.Cpus
			}
			want := map[string]string{"healthy": "1-3"}
			if tc.wantRetain {
				want["shared"] = "1-3"
			}
			require.Equal(t, want, got)
		})
	}
}

func TestWarmSynchronizeMalformedExclusiveContainerKeepsOwnership(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	claim := individualMetricsClaim("a", "cpudev0")
	prepared, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, prepared[claim.UID].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
	_, _, err = d.CreateContainer(t.Context(), pod, app)
	require.NoError(t, err)
	// A malformed unrelated entry must not turn an exclusive container into
	// a shared one or make us trust a changed allocation later in the env.
	app.Env = []string{"DRA_CPUSET_other=bad", "DRA_CPUSET_a=1"}
	updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
		app, {Id: "shared", Name: "sidecar", PodSandboxId: pod.Id},
	})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "shared", updates[0].ContainerId)
	require.Equal(t, "1-3", updates[0].Linux.Resources.Cpu.Cpus)
	_, _, err = d.CreateContainer(t.Context(), &api.PodSandbox{Uid: "other"}, &api.Container{
		Id: "other-app", Name: app.Name, Env: []string{"DRA_CPUSET_a=0"},
	})
	require.Error(t, err, "another pod cannot take the retained claim")
	app.Id = "replacement"
	app.Env = []string{"DRA_CPUSET_a=0"}
	adjustment, _, err := d.CreateContainer(t.Context(), pod, app)
	require.NoError(t, err)
	require.Equal(t, "0", adjustment.Linux.Resources.Cpu.Cpus)
}

func pinnedTo(cpus string) *api.LinuxContainer {
	return &api.LinuxContainer{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: cpus}}}
}

func cpusByContainer(updates []*api.ContainerUpdate) map[string]string {
	byID := make(map[string]string, len(updates))
	for _, update := range updates {
		byID[update.ContainerId] = update.GetLinux().GetResources().GetCpu().GetCpus()
	}
	return byID
}

// Two containers name one claim, the second created while the driver was not
// connected. Every process must synchronize, and reach the same answer at each
// attempt and in either list order.
func TestSynchronizeClaimNamedByTwoContainers(t *testing.T) {
	const claimUID = types.UID("shared-claim")
	held := cpuset.New(0, 2)
	env := []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, held.String())}
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	otherPod := &api.PodSandbox{Id: "other-pod", Uid: "other-pod-uid"}

	testCases := []struct {
		name       string
		containers []*api.Container
		wantOwner  *store.OwnerIdent
		wantCPUs   map[string]string
	}{
		{
			name: "neither was admitted by the driver",
			containers: []*api.Container{
				{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: env},
				{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: env},
				{Id: "c-plain", Name: "plain", PodSandboxId: pod.Id},
			},
			wantCPUs: map[string]string{"c-app": "1,3", "c-sidecar": "1,3", "c-plain": "1,3"},
		},
		{
			name: "the one the driver admitted keeps the claim",
			containers: []*api.Container{
				{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: env, Linux: pinnedTo("0,2")},
				{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: env},
				{Id: "c-plain", Name: "plain", PodSandboxId: pod.Id},
			},
			wantOwner: &store.OwnerIdent{PodUID: "pod-uid", ContainerName: "app"},
			wantCPUs:  map[string]string{"c-app": "0,2", "c-sidecar": "1,3", "c-plain": "1,3"},
		},
		{
			name: "a second pod names a standalone claim",
			containers: []*api.Container{
				{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: env, Linux: pinnedTo("0,2")},
				{Id: "c-intruder", Name: "app", PodSandboxId: otherPod.Id, Env: env, Linux: pinnedTo("0-3")},
			},
			wantOwner: &store.OwnerIdent{PodUID: "pod-uid", ContainerName: "app"},
			wantCPUs:  map[string]string{"c-app": "0,2", "c-intruder": "1,3"},
		},
		{
			name: "two containers pinned to the claim prove nothing",
			containers: []*api.Container{
				{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: env, Linux: pinnedTo("0,2")},
				{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: env, Linux: pinnedTo("0,2")},
			},
			wantCPUs: map[string]string{"c-app": "1,3", "c-sidecar": "1,3"},
		},
		{
			name: "a container naming other CPUs does not contend for the claim",
			containers: []*api.Container{
				{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: env},
				{Id: "c-forged", Name: "app", PodSandboxId: otherPod.Id, Env: []string{fmt.Sprintf("%s_%s=1", cdiEnvVarPrefix, claimUID)}},
			},
			wantOwner: &store.OwnerIdent{PodUID: "pod-uid", ContainerName: "app"},
			wantCPUs:  map[string]string{"c-app": "0,2", "c-forged": "1,3"},
		},
	}

	for _, tc := range testCases {
		for _, reversed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reversed=%v", tc.name, reversed), func(t *testing.T) {
				containers := slices.Clone(tc.containers)
				if reversed {
					slices.Reverse(containers)
				}
				pods := []*api.PodSandbox{pod, otherPod}
				d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
				d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{claimUID: held})
				d.seedAllocationStoreFromDisk(testr.New(t))

				// The first attempt is a fresh process, the others are reconnects.
				for attempt := range 3 {
					updates, err := d.Synchronize(t.Context(), pods, containers)
					require.NoError(t, err, "attempt %d", attempt)
					require.Equal(t, tc.wantCPUs, cpusByContainer(updates), "attempt %d", attempt)
					require.Equal(t, held.String(), d.allocationState.GetPreparedCPUs().String())
					owner, bound := d.allocationState.Owner(claimUID)
					if tc.wantOwner == nil {
						require.False(t, bound, "attempt %d: bound to %v", attempt, owner)
					} else {
						require.True(t, bound, "attempt %d", attempt)
						require.Equal(t, *tc.wantOwner, owner)
					}
				}
				require.True(t, d.hasSynchronized)

				claim := individualMetricsClaim("new-claim", "cpudev1")
				results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
				require.NoError(t, err)
				require.NoError(t, results[claim.UID].Err, "an unrelated claim must still prepare")
			})
		}
	}
}

func TestSynchronizeBindsClaimOnceUncontested(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	app := &api.Container{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
	sidecar := &api.Container{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}

	_, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app, sidecar})
	require.NoError(t, err)
	_, bound := d.allocationState.Owner("a")
	require.False(t, bound)

	updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"c-app": "0"}, cpusByContainer(updates))
	require.True(t, d.allocationState.IsOwner("a", "pod-uid", "app"))
}

func TestWarmSynchronizeKeepsOwnerAgainstSecondContainer(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	_, err := d.Synchronize(t.Context(), nil, nil)
	require.NoError(t, err)
	claim := individualMetricsClaim("a", "cpudev0")
	results, err := d.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	app := &api.Container{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
	_, _, err = d.CreateContainer(t.Context(), pod, app)
	require.NoError(t, err)
	sidecar := &api.Container{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0"}}
	_, _, err = d.CreateContainer(t.Context(), pod, sidecar)
	require.ErrorIs(t, err, store.ErrClaimAlreadyBound)

	for _, containers := range [][]*api.Container{{app, sidecar}, {sidecar, app}} {
		updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, containers)
		require.NoError(t, err)
		require.Equal(t, map[string]string{"c-app": "0", "c-sidecar": "1-3"}, cpusByContainer(updates))
		require.True(t, d.allocationState.IsOwner("a", "pod-uid", "app"))
	}
}

func TestSynchronizeJudgesEachClaimOfAContainer(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0), "b": cpuset.New(1)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	app := &api.Container{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_a=0", "DRA_CPUSET_b=1"}}
	sidecar := &api.Container{Id: "c-sidecar", Name: "sidecar", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_b=1"}}

	updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app, sidecar})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"c-app": "0", "c-sidecar": "2-3"}, cpusByContainer(updates))
	require.True(t, d.allocationState.IsOwner("a", "pod-uid", "app"))
	_, bound := d.allocationState.Owner("b")
	require.False(t, bound)
	require.Equal(t, "0-1", d.allocationState.GetPreparedCPUs().String())
}

func TestSynchronizeJudgesRequestVariables(t *testing.T) {
	pod := &api.PodSandbox{Id: "pod", Uid: "pod-uid"}
	vars := func(requests ...string) []string {
		var envs []string
		for _, request := range requests {
			envs = append(envs, fmt.Sprintf("%s_vm_%s=%s", cdiEnvVarPrefix, request, cdiEnvDynamicValue))
		}
		return envs
	}
	tests := []struct {
		name        string
		env         []string
		pinned      string
		wantCPUs    string
		wantBound   bool
		wantKept    float64
		wantRefused float64
		wantWarning string
	}{
		{
			name:      "a container naming every request binds the claim",
			env:       vars("vcpus", "helpers"),
			wantCPUs:  "0-2",
			wantBound: true,
		},
		{
			name:        "a partial holder pinned to the whole claim keeps it",
			env:         vars("vcpus"),
			pinned:      "0-2",
			wantCPUs:    "0-2",
			wantBound:   true,
			wantKept:    1,
			wantWarning: `container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule partial_holder: it uses only request "vcpus" of claim "vm-claim", which must be used whole; use the whole claim, or give the container its own claim`,
		},
		{
			name:        "a partial holder off the claim's CPUs runs on the shared CPUs",
			env:         vars("vcpus"),
			pinned:      "0,2",
			wantCPUs:    "3",
			wantRefused: 1,
		},
		{
			name:        "a variable naming a request the record lacks is refused",
			env:         vars("vcpus", "helpers", "gpus"),
			pinned:      "0-2",
			wantCPUs:    "3",
			wantRefused: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, reg := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
			cdi := newMockCdiMgr()
			cdi.records["vm"] = cdiClaimRecord{Namespace: "ns", Name: "vm-claim", Requests: []cdiRequestRecord{
				{Request: "helpers", CPUs: cpuset.New(1)},
				{Request: "vcpus", CPUs: cpuset.New(0, 2)},
			}}
			d.cdiMgr = cdi
			d.seedAllocationStoreFromDisk(testr.New(t))
			events := &capturingRecorder{}
			d.eventRecorder = events
			app := &api.Container{Id: "c-app", Name: "app", PodSandboxId: pod.Id, Env: tc.env}
			if tc.pinned != "" {
				app.Linux = pinnedTo(tc.pinned)
			}

			// The first attempt is a fresh process, the second a reconnect.
			for attempt := 1; attempt <= 2; attempt++ {
				updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{app})
				require.NoError(t, err)
				require.Equal(t, map[string]string{"c-app": tc.wantCPUs}, cpusByContainer(updates), "attempt %d", attempt)
				require.Equal(t, tc.wantBound, d.allocationState.IsOwner("vm", "pod-uid", "app"), "attempt %d", attempt)
				require.Equal(t, "0-2", d.allocationState.GetPreparedCPUs().String(), "the claim stays prepared")
				require.Equal(t, float64(attempt)*tc.wantKept, metricValue(t, reg, "dra_cpu_containers_kept_under_older_rule_total", map[string]string{"rule": string(cpumetrics.RulePartialHolder)}))
				require.Equal(t, float64(attempt)*tc.wantRefused, metricValue(t, reg, "dra_cpu_nri_synchronize_refused_claims_total", nil))
				var want []string
				for range attempt {
					if tc.wantWarning != "" {
						want = append(want, "Warning KeptUnderOlderRule "+tc.wantWarning)
					}
				}
				require.Equal(t, want, events.lines(), "attempt %d: one Warning per kept container and Synchronize", attempt)
			}

			if tc.wantKept > 0 {
				recreated := &api.Container{Id: "c-app-2", Name: "app", PodSandboxId: pod.Id, Env: tc.env}
				_, _, err := d.CreateContainer(t.Context(), pod, recreated)
				require.EqualError(t, err, `container "app": uses only request "vcpus" of claim "vm-claim", which must be used whole; use the whole claim, or give the container its own claim`,
					"the rule applies at the container's next CreateContainer")
			}
		})
	}
}

type capturedEvent struct {
	object    runtime.Object
	eventType string
	reason    string
	message   string
}

// capturingRecorder is a fake event recorder that also keeps each event's
// object, which record.FakeRecorder drops.
type capturingRecorder struct {
	record.EventRecorder
	events []capturedEvent
}

func (r *capturingRecorder) Event(object runtime.Object, eventType, reason, message string) {
	r.events = append(r.events, capturedEvent{object: object, eventType: eventType, reason: reason, message: message})
}

func (r *capturingRecorder) lines() []string {
	var lines []string
	for _, event := range r.events {
		lines = append(lines, event.eventType+" "+event.reason+" "+event.message)
	}
	return lines
}

func TestSynchronizeWarnsOncePerKeptContainer(t *testing.T) {
	podA := &api.PodSandbox{Id: "sandbox-a", Uid: "pod-a-uid", Namespace: "ns", Name: "pod-a"}
	podB := &api.PodSandbox{Id: "sandbox-b", Uid: "pod-b-uid", Namespace: "ns", Name: "pod-b"}
	request := func(claim, name string) string {
		return fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim, name, cdiEnvDynamicValue)
	}
	containers := []*api.Container{
		// Pinned to both of its claims' CPUs, it keeps both and gets one Warning.
		{Id: "c-app", Name: "app", PodSandboxId: podA.Id, Env: []string{request("vm", "vcpus"), request("vm2", "a")}, Linux: pinnedTo("0-3")},
		// Off its claim's CPUs, it runs on the shared CPUs and gets none.
		{Id: "c-stray", Name: "stray", PodSandboxId: podA.Id, Env: []string{request("web", "x")}, Linux: pinnedTo("100")},
		{Id: "c-db", Name: "db", PodSandboxId: podB.Id, Env: []string{request("db", "main")}, Linux: pinnedTo("4-5")},
	}
	cdi := newMockCdiMgr()
	cdi.records["vm"] = cdiClaimRecord{Namespace: "ns", Name: "vm-claim", Requests: []cdiRequestRecord{
		{Request: "helpers", CPUs: cpuset.New(1)},
		{Request: "vcpus", CPUs: cpuset.New(0)},
	}}
	cdi.records["vm2"] = cdiClaimRecord{Namespace: "ns", Name: "vm2-claim", Requests: []cdiRequestRecord{
		{Request: "a", CPUs: cpuset.New(2)},
		{Request: "b", CPUs: cpuset.New(3)},
	}}
	cdi.records["db"] = cdiClaimRecord{Namespace: "ns", Name: "db-claim", Requests: []cdiRequestRecord{
		{Request: "extra", CPUs: cpuset.New(5)},
		{Request: "main", CPUs: cpuset.New(4)},
	}}
	cdi.records["web"] = cdiClaimRecord{Namespace: "ns", Name: "web-claim", Requests: []cdiRequestRecord{
		{Request: "x", CPUs: cpuset.New(6)},
		{Request: "y", CPUs: cpuset.New(7)},
	}}
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_DualSocket_120CPUsPerSocket_HT)
	d.cdiMgr = cdi
	d.seedAllocationStoreFromDisk(testr.New(t))
	events := &capturingRecorder{}
	d.eventRecorder = events

	wantApp := `container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by ` +
		`rule partial_holder: it uses only request "vcpus" of claim "vm-claim", which must be used whole; use the whole claim, or give the container its own claim, ` +
		`and by rule partial_holder: it uses only request "a" of claim "vm2-claim", which must be used whole; use the whole claim, or give the container its own claim`
	wantDB := `container "db" was created under an older rule and keeps its claims until it is created again, when it is refused by ` +
		`rule partial_holder: it uses only request "main" of claim "db-claim", which must be used whole; use the whole claim, or give the container its own claim`
	// The first attempt is a fresh process, the second a reconnect.
	for attempt := 1; attempt <= 2; attempt++ {
		events.events = nil
		_, err := d.Synchronize(t.Context(), []*api.PodSandbox{podA, podB}, containers)
		require.NoError(t, err)
		require.True(t, d.allocationState.IsOwner("vm", "pod-a-uid", "app"))
		require.True(t, d.allocationState.IsOwner("vm2", "pod-a-uid", "app"))
		require.True(t, d.allocationState.IsOwner("db", "pod-b-uid", "db"))
		_, webBound := d.allocationState.Owner("web")
		require.False(t, webBound)

		require.Equal(t, []string{"Warning KeptUnderOlderRule " + wantApp, "Warning KeptUnderOlderRule " + wantDB}, events.lines(), "attempt %d", attempt)
		for i, pod := range []*api.PodSandbox{podA, podB} {
			object, ok := events.events[i].object.(*corev1.Pod)
			require.True(t, ok, "attempt %d: the Warning is on a pod, got %T", attempt, events.events[i].object)
			require.Equal(t, metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: types.UID(pod.Uid)}, object.ObjectMeta)
		}
	}
}

// NRI cannot give the container without claims an empty cpuset, so it stays
// where the runtime put it rather than taking the driver off the node.
func TestSynchronizeLeavesSharedContainersWhenPoolIsExhausted(t *testing.T) {
	d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
	d.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"full": cpuset.New(0, 1, 2, 3)})
	d.seedAllocationStoreFromDisk(testr.New(t))
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{
		{Id: "exclusive", Name: "app", PodSandboxId: pod.Id, Env: []string{"DRA_CPUSET_full=0-3"}},
		{Id: "shared", Name: "sidecar", PodSandboxId: pod.Id},
	})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"exclusive": "0-3"}, cpusByContainer(updates))
	require.True(t, d.hasSynchronized)
	require.True(t, d.allocationState.IsOwner("full", types.UID(pod.Uid), "app"))
	require.NotNil(t, d.podState.GetContainerState(types.UID(pod.Uid), "sidecar"))

	_, _, err = d.CreateContainer(t.Context(), pod, &api.Container{Id: "late", Name: "late", PodSandboxId: pod.Id})
	require.ErrorContains(t, err, "no shared CPUs available")
}
