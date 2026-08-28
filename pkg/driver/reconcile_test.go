/*
Copyright 2026 The Kubernetes Authors.

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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// fakeContainerUpdater records the unsolicited updates the driver pushes.
type fakeContainerUpdater struct {
	mu     sync.Mutex
	calls  [][]*api.ContainerUpdate
	failed []*api.ContainerUpdate
	err    error
}

func (f *fakeContainerUpdater) UpdateContainers(updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, updates)
	return f.failed, f.err
}

func (f *fakeContainerUpdater) allCalls() [][]*api.ContainerUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]*api.ContainerUpdate{}, f.calls...)
}

// driverWithSharedContainer returns a driver holding one prepared claim on
// claimCPUs plus one running shared container, which is the state a release has
// to reconcile.
func driverWithSharedContainer(t *testing.T, claimUID types.UID, claimCPUs cpuset.CPUSet) (*CPUDriver, *fakeContainerUpdater) {
	t.Helper()
	logger := testr.New(t)

	var infos []cpuinfo.CPUInfo
	for cpu := range 8 {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	updater := &fakeContainerUpdater{}
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		podConfigStore:     store.NewPodConfig(),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             newMockCdiMgr(),
		containerUpdater:   updater,
		reconcileTrigger:   make(chan struct{}, 1),
	}
	requirePreparedResourceClaim(t, logger, d.cpuAllocationStore, claimUID, claimCPUs)
	d.podConfigStore.SetContainerState("shared-pod", store.NewContainerState("shared-ctr", "shared-ctr-id"))
	return d, updater
}

func TestReconcileSharedContainersWidensAfterRelease(t *testing.T) {
	claimUID := types.UID("claim-1")
	d, updater := driverWithSharedContainer(t, claimUID, cpuset.New(0, 1, 2, 3))

	// Before the release the shared container is confined to the other half.
	d.reconcileSharedContainers(context.Background())
	require.Len(t, updater.allCalls(), 1)
	require.Equal(t, "4-7", updater.allCalls()[0][0].GetLinux().GetResources().GetCpu().GetCpus())

	// Releasing the claim must widen it to the whole node.
	d.cpuAllocationStore.RemoveResourceClaimAllocation(testr.New(t), claimUID)
	d.reconcileSharedContainers(context.Background())

	calls := updater.allCalls()
	require.Len(t, calls, 2)
	require.Len(t, calls[1], 1)
	require.Equal(t, "shared-ctr-id", calls[1][0].GetContainerId())
	require.Equal(t, "0-7", calls[1][0].GetLinux().GetResources().GetCpu().GetCpus())
}

func TestReconcileSharedContainersToleratesFailure(t *testing.T) {
	d, updater := driverWithSharedContainer(t, "claim-1", cpuset.New(0, 1))

	// Widening only ever adds CPUs the container is entitled to, so a refusal
	// leaves it on the narrower mask and must not be fatal.
	updater.err = errors.New("runtime refused")
	d.reconcileSharedContainers(context.Background())
	require.Len(t, updater.allCalls(), 1)

	updater.err = nil
	updater.failed = []*api.ContainerUpdate{{ContainerId: "shared-ctr-id"}}
	d.reconcileSharedContainers(context.Background())
	require.Len(t, updater.allCalls(), 2)
}

func TestReconcileSharedContainersNoOpWithoutUpdater(t *testing.T) {
	d, _ := driverWithSharedContainer(t, "claim-1", cpuset.New(0, 1))
	d.containerUpdater = nil

	// Must not panic when unsolicited updates were never permitted.
	d.reconcileSharedContainers(context.Background())
}

func TestRequestReconcileCoalesces(t *testing.T) {
	d, _ := driverWithSharedContainer(t, "claim-1", cpuset.New(0, 1))

	// A burst of releases must cost one pass, not one per release, and must
	// never block the caller: these run on kubelet's Unprepare path.
	for range 100 {
		d.requestReconcile()
	}
	require.Len(t, d.reconcileTrigger, 1)
}

func TestRequestReconcileIsSafeWhenDisabled(t *testing.T) {
	d, _ := driverWithSharedContainer(t, "claim-1", cpuset.New(0, 1))
	d.reconcileTrigger = nil

	// Unprepare calls this unconditionally, so it must be inert rather than
	// panic when the feature is off.
	d.requestReconcile()
}

func TestReconcileWorkerServesRequests(t *testing.T) {
	claimUID := types.UID("claim-1")
	d, updater := driverWithSharedContainer(t, claimUID, cpuset.New(0, 1, 2, 3))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		d.runReconcileWorker(ctx)
		close(done)
	}()

	d.cpuAllocationStore.RemoveResourceClaimAllocation(testr.New(t), claimUID)
	d.requestReconcile()

	require.Eventually(t, func() bool {
		calls := updater.allCalls()
		return len(calls) > 0 && calls[0][0].GetLinux().GetResources().GetCpu().GetCpus() == "0-7"
	}, 2*time.Second, 5*time.Millisecond, "worker did not widen the shared container")

	cancel()
	<-done
}
