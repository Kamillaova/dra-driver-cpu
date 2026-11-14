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
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

// Construct a fresh process with an empty allocation store, but CDI and runtime
// inventory from an existing exclusive container. Delay the Synchronize call
// itself, not a callback already holding stateMu: this is the startup window.
// The initial sync wait is short so specs that expect a rejection stay fast.
func startupWindowFixture(t *testing.T) (*CPUDriver, *api.PodSandbox, []*api.Container, *resourceapi.ResourceClaim) {
	t.Helper()
	infos := mockCPUInfos_SingleSocket_4CPUS_HT
	driver, err := New(testr.New(t), Providers{CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}, SysFS: testSysFS(infos)}, &Config{
		DriverName: testDriverName, NodeName: testNodeName,
		CPUDeviceMode: device.CPU_DEVICE_MODE_GROUPED, CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
	})
	require.NoError(t, err)
	driver.initialSyncWait = 10 * time.Millisecond
	held := cpuset.New(0, 2) // Deterministic first choice of this allocator/topology.
	driver.cdiMgr = newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"existing": held})
	pod := &api.PodSandbox{Id: "existing-pod", Uid: "existing-pod-uid"}
	containers := []*api.Container{{Id: "existing-container", Name: "workload", PodSandboxId: pod.Id, State: api.ContainerState_CONTAINER_RUNNING, Env: []string{fmt.Sprintf("%s_existing=%s", cdiEnvVarPrefix, held.String())}}}
	claim := testClaim("new-claim", testDriverName, testNodeName, map[string]int64{"cpudevnuma000": 2})
	return driver, pod, containers, claim
}

func TestStartupWindowPrepareMustWaitForRecovery(t *testing.T) {
	driver, pod, containers, claim := startupWindowFixture(t)
	before := len(driver.cdiMgr.(*mockCdiMgr).devices)
	results, err := driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.ErrorContains(t, results[claim.UID].Err, "initial NRI synchronization", "Prepare must reject a request before the first successful Synchronize")
	_, ok := driver.allocationState.GetResourceClaimAllocation(claim.UID)
	require.False(t, ok)
	require.Len(t, driver.cdiMgr.(*mockCdiMgr).devices, before, "rejection must not write CDI")
	_, err = driver.Synchronize(t.Context(), []*api.PodSandbox{pod}, containers)
	require.NoError(t, err)
	require.True(t, driver.hasSynchronized)
	results, err = driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
	allocated, ok := driver.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, ok)
	require.True(t, allocated.Intersection(cpuset.New(0, 2)).IsEmpty())
	require.Equal(t, 2, allocated.Size())
	t.Logf("after recovery: existing=0,2, new=%s", allocated.String())
}

// A Prepare that arrives just before the first Synchronize waits for it and then
// succeeds on the first attempt, instead of failing and relying on kubelet retry.
func TestStartupWindowPrepareWaitsForInitialSynchronize(t *testing.T) {
	driver, pod, containers, claim := startupWindowFixture(t)
	driver.initialSyncWait = time.Minute // Only the Synchronize below may release Prepare.
	done := make(chan prepareOutcome, 1)
	go func() {
		results, err := driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
		done <- prepareOutcome{results: results, err: err}
	}()
	require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, 10*time.Millisecond, "Prepare must wait for the first Synchronize")

	_, err := driver.Synchronize(t.Context(), []*api.PodSandbox{pod}, containers)
	require.NoError(t, err)
	var outcome prepareOutcome
	require.Eventually(t, func() bool {
		select {
		case outcome = <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, outcome.err)
	require.NoError(t, outcome.results[claim.UID].Err)
	allocated, ok := driver.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, ok)
	require.True(t, allocated.Intersection(cpuset.New(0, 2)).IsEmpty())
}

type prepareOutcome struct {
	results map[types.UID]kubeletplugin.PrepareResult
	err     error
}

func TestStartupWindowFailedSynchronizeKeepsGateClosed(t *testing.T) {
	driver, pod, containers, claim := startupWindowFixture(t)
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := driver.Synchronize(cancelled, []*api.PodSandbox{pod}, containers)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, driver.hasSynchronized)
	results, err := driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.Error(t, results[claim.UID].Err)
	_, ok := driver.allocationState.GetResourceClaimAllocation(claim.UID)
	require.False(t, ok)
	_, err = driver.Synchronize(t.Context(), []*api.PodSandbox{pod}, containers)
	require.NoError(t, err)
	results, err = driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
}

// Two records naming the same CPUs cannot both be right, and nothing says which
// one is. Neither claim is bound, their CPUs stay out of the shared pool and of
// new claims, and the node keeps working until one of them is unprepared.
func TestStartupWindowOverlappingRecordsBindNoContainer(t *testing.T) {
	driver, pod, containers, _ := startupWindowFixture(t)
	driver.cdiMgr.(*mockCdiMgr).devices[getCDIDeviceName("conflict")] = fmt.Sprintf("%s_conflict=0,2", cdiEnvVarPrefix)
	all := append([]*api.Container{}, containers...)
	all = append(all, &api.Container{Id: "conflict-container", Name: "other", PodSandboxId: pod.Id, State: api.ContainerState_CONTAINER_RUNNING, Env: []string{fmt.Sprintf("%s_conflict=0,2", cdiEnvVarPrefix)}})

	updates, err := driver.Synchronize(t.Context(), []*api.PodSandbox{pod}, all)
	require.NoError(t, err)
	require.True(t, driver.hasSynchronized)
	require.Equal(t, map[string]string{"existing-container": "1,3", "conflict-container": "1,3"}, cpusByContainer(updates))
	require.Equal(t, "0,2", driver.allocationState.GetPreparedCPUs().String())
	for _, uid := range []types.UID{"existing", "conflict"} {
		_, bound := driver.allocationState.Owner(uid)
		require.False(t, bound, "claim %q must not be bound", uid)
	}

	claim := testClaim("new-claim", testDriverName, testNodeName, map[string]int64{"cpudevnuma000": 1})
	results, err := driver.PrepareResourceClaims(t.Context(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
	allocated, ok := driver.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, ok)
	require.True(t, allocated.Intersection(cpuset.New(0, 2)).IsEmpty())

	_, _, err = driver.CreateContainer(t.Context(), pod, all[0])
	require.ErrorIs(t, err, store.ErrClaimOverlap, "a restarted container is refused while the records overlap")

	unprepared, err := driver.UnprepareResourceClaims(t.Context(), []kubeletplugin.NamespacedObject{{UID: "conflict"}})
	require.NoError(t, err)
	require.NoError(t, unprepared["conflict"])
	require.Equal(t, "0,2", driver.allocationState.GetPreparedCPUs().Difference(allocated).String())
	updates, err = driver.Synchronize(t.Context(), []*api.PodSandbox{pod}, containers)
	require.NoError(t, err)
	require.Equal(t, "0,2", cpusByContainer(updates)["existing-container"])
	require.True(t, driver.allocationState.IsOwner("existing", types.UID(pod.Uid), "workload"))
}
