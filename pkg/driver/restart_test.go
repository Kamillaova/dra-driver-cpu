/*
Copyright 2025 The Kubernetes Authors.

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
	"testing/fstest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

func newRestartTestDriver(t *testing.T, cdi *mockCdiMgr, cgroups fstest.MapFS) *CPUDriver {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, err := mockProvider.GetCPUTopology(logger)
	require.NoError(t, err)

	return &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		podConfigStore:     store.NewPodConfig(),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             cdi,
		cgroupfs:           cgroups,
		poisonedNodes:      make(map[int]*poisonedNode),
		metrics:            cpumetrics.Noop(),
	}
}

func setContainerLiveCPUs(cgroups fstest.MapFS, containerUID types.UID, cpus cpuset.CPUSet) {
	dir, err := cgroupfs.Dir("/kubepods/" + string(containerUID))
	if err != nil {
		panic(err)
	}
	cgroups[dir+"/cpuset.cpus.effective"] = &fstest.MapFile{Data: []byte(cpus.String() + "\n")}
}

func TestRestartIncompleteRoundRevertsToOrigin(t *testing.T) {
	claim1 := types.UID("claim-1")
	claim2 := types.UID("claim-2")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID:  "round-42",
			Origin:   cpuset.New(0, 1),
			Target:   cpuset.New(2, 3),
			Partners: []types.UID{claim2},
		},
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	rec2 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive}},
		Relocatable: true,
	}
	cdi.placements[getCDIDeviceName(claim2)] = rec2
	cdi.devices[getCDIDeviceName(claim2)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim2)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(0, 1))
	setContainerLiveCPUs(cgroups, "ctr-2", cpuset.New(2, 3))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}},
		},
	}
	pod2 := &api.PodSandbox{Id: "p2", Uid: "pod-2", Name: "pod-2", Namespace: "default"}
	ctr2 := &api.Container{
		Id:           "ctr-2",
		PodSandboxId: "p2",
		Name:         "ctr-2",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim2)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-2",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1, pod2}, []*api.Container{ctr1, ctr2})
	require.NoError(t, err)

	revertedRec1, err := d.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claim1))
	require.NoError(t, err)
	require.Equal(t, cpuset.New(0, 1), store.UnionOf(revertedRec1.Requests))
	require.Nil(t, revertedRec1.Round)

	c1Alloc, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claim1)
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), c1Alloc)

	c2Alloc, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claim2)
	require.True(t, ok)
	require.Equal(t, cpuset.New(2, 3), c2Alloc)

	require.Empty(t, updates)
}

func TestRestartCompleteRoundConvergesForward(t *testing.T) {
	claim1 := types.UID("claim-1")
	claim2 := types.UID("claim-2")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID:  "round-42",
			Origin:   cpuset.New(0, 1),
			Target:   cpuset.New(2, 3),
			Partners: []types.UID{claim2},
		},
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	rec2 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID:  "round-42",
			Origin:   cpuset.New(2, 3),
			Target:   cpuset.New(0, 1),
			Partners: []types.UID{claim1},
		},
	}
	cdi.placements[getCDIDeviceName(claim2)] = rec2
	cdi.devices[getCDIDeviceName(claim2)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim2)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(0, 1))
	setContainerLiveCPUs(cgroups, "ctr-2", cpuset.New(2, 3))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}},
		},
	}
	pod2 := &api.PodSandbox{Id: "p2", Uid: "pod-2", Name: "pod-2", Namespace: "default"}
	ctr2 := &api.Container{
		Id:           "ctr-2",
		PodSandboxId: "p2",
		Name:         "ctr-2",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim2)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-2",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1, pod2}, []*api.Container{ctr1, ctr2})
	require.NoError(t, err)

	require.Len(t, updates, 2)
	require.Equal(t, "2-3", updateFor(t, updates, "ctr-1"))
	require.Equal(t, "0-1", updateFor(t, updates, "ctr-2"))

	rec1After, err := d.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claim1))
	require.NoError(t, err)
	require.Nil(t, rec1After.Round)

	rec2After, err := d.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claim2))
	require.NoError(t, err)
	require.Nil(t, rec2After.Round)
}

func TestRestartSettledContainerClearsRoundAnnotations(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID:  "round-42",
			Origin:   cpuset.New(0, 1),
			Target:   cpuset.New(2, 3),
			Partners: nil,
		},
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(2, 3))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)
	require.Empty(t, updates)

	recAfter, err := d.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claim1))
	require.NoError(t, err)
	require.Nil(t, recAfter.Round)
}

// TestRestartUnknownKernelStateWithoutARoundConverges: a claim with no round in
// its record was never mid-move, so a kernel that disagrees with the record is
// not an ambiguity to fence over -- it is a container to pin. Fencing here is
// what F5 did on a real node: the claim was in perfect order and its only fault
// was a third source nobody could read.
func TestRestartUnknownKernelStateWithoutARoundConverges(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
		Relocatable: true,
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(4, 5))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)

	require.Len(t, updates, 1)
	require.Equal(t, "0-1", updateFor(t, updates, "ctr-1"))
	require.False(t, d.nodeIsPoisoned(0), "a claim that was never mid-move fences nothing")
}

// TestRestartUnknownKernelStateWithARoundPoisonsNUMANode: with a round in the
// record the same disagreement is the ambiguity the fence exists for -- the
// container is on neither the origin nor the target, so which claim holds which
// CPUs cannot be said.
func TestRestartUnknownKernelStateWithARoundPoisonsNUMANode(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID: "round-7",
			Origin:  cpuset.New(0, 1),
			Target:  cpuset.New(2, 3),
			// A partner that is not running, so the round stays incomplete and
			// the record keeps its provenance through Synchronize.
			Partners: []types.UID{"claim-gone"},
		},
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(4, 5))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)
	require.Empty(t, updates)

	require.True(t, d.nodeIsPoisoned(0))
}

func TestRestartRollbackApplicableContainer(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	rec1 := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
		Relocatable: true,
		Round: &store.RoundProvenance{
			RoundID:  "round-42",
			Origin:   cpuset.New(0, 1),
			Target:   cpuset.New(2, 3),
			Partners: nil,
		},
	}
	cdi.placements[getCDIDeviceName(claim1)] = rec1
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(2, 3))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)

	require.Len(t, updates, 1)
	require.Equal(t, "0-1", updateFor(t, updates, "ctr-1"))
}

// A round moves only a claim's exclusive CPUs, so a container that also holds a
// pool share is never on the round's origin or target alone.
func TestRestartForwardApplicableContainerHoldingAPoolShare(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	cdi.placements[getCDIDeviceName(claim1)] = store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "helpers", CPUs: cpuset.New(6, 7), Role: store.RoleShared},
			{Request: "vcpus", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive},
		},
		Relocatable: true,
		Round:       &store.RoundProvenance{RoundID: "round-42", Origin: cpuset.New(0, 1), Target: cpuset.New(2, 3)},
	}
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(0, 1, 6, 7))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1,6-7"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)

	require.Len(t, updates, 1)
	require.Equal(t, "2-3,6-7", updateFor(t, updates, "ctr-1"))
	require.False(t, d.nodeIsPoisoned(0), "still on its origin plus its pool share is forward-applicable, not unknown")
}

func TestRestartRollbackApplicableContainerHoldingAPoolShare(t *testing.T) {
	claim1 := types.UID("claim-1")
	cdi := newMockCdiMgr()

	cdi.placements[getCDIDeviceName(claim1)] = store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "helpers", CPUs: cpuset.New(6, 7), Role: store.RoleShared},
			{Request: "vcpus", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive},
		},
		Relocatable: true,
		Round:       &store.RoundProvenance{RoundID: "round-42", Origin: cpuset.New(0, 1), Target: cpuset.New(2, 3)},
	}
	cdi.devices[getCDIDeviceName(claim1)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-1", cpuset.New(2, 3, 6, 7))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr1 := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: "p1",
		Name:         "ctr-1",
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim1)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-1",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3,6-7"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr1})
	require.NoError(t, err)

	require.Len(t, updates, 1)
	require.Equal(t, "0-1,6-7", updateFor(t, updates, "ctr-1"))
	require.False(t, d.nodeIsPoisoned(0), "already on its target plus its pool share is rollback-applicable, not unknown")
}

func TestRestartMultiClaimContainerUnion(t *testing.T) {
	claimA := types.UID("claim-A")
	claimB := types.UID("claim-B")
	cdi := newMockCdiMgr()

	recA := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
		Relocatable: true,
	}
	recB := store.ClaimRecord{
		Requests:    []store.RequestAllocation{{Request: "req-0", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive}},
		Relocatable: true,
	}
	cdi.placements[getCDIDeviceName(claimA)] = recA
	cdi.placements[getCDIDeviceName(claimB)] = recB
	cdi.devices[getCDIDeviceName(claimA)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claimA)
	cdi.devices[getCDIDeviceName(claimB)] = fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claimB)

	cgroups := fstest.MapFS{}
	setContainerLiveCPUs(cgroups, "ctr-multi", cpuset.New(0, 1, 2, 3))

	d := newRestartTestDriver(t, cdi, cgroups)

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	ctr := &api.Container{
		Id:           "ctr-multi",
		PodSandboxId: "p1",
		Name:         "ctr-multi",
		Env: []string{
			fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claimA),
			fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claimB),
		},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-multi",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctr})
	require.NoError(t, err)
	require.Empty(t, updates)

	c1Alloc, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimA)
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), c1Alloc)

	c2Alloc, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimB)
	require.True(t, ok)
	require.Equal(t, cpuset.New(2, 3), c2Alloc)
}

// TestRestartPinsAClaimHolderTheRuntimeReportsUnpinned is the acceptance
// criterion from the T0 rollout (finding F13): a crash-looping driver leaves a
// claim-holding container unpinned on its next restart, and after a node reboot
// it comes back on every CPU while its claim still charges a handful. Nothing
// else will pin it -- CreateContainer has already run -- so Synchronize must.
func TestRestartPinsAClaimHolderTheRuntimeReportsUnpinned(t *testing.T) {
	claimA := types.UID("claim-A")
	claimB := types.UID("claim-B")
	cdi := newMockCdiMgr()

	for _, claim := range []struct {
		uid  types.UID
		cpus cpuset.CPUSet
	}{{claimA, cpuset.New(0, 1)}, {claimB, cpuset.New(2, 3)}} {
		cdi.placements[getCDIDeviceName(claim.uid)] = store.ClaimRecord{
			Requests: []store.RequestAllocation{{Request: "req-0", CPUs: claim.cpus, Role: store.RoleExclusive}},
		}
		cdi.devices[getCDIDeviceName(claim.uid)] = fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claim.uid, claim.cpus.String())
	}

	// No cgroup tree at all, which is how the driver runs with defragmentation
	// off: the kernel cannot be consulted, and the answer must not depend on it.
	d := newRestartTestDriver(t, cdi, nil)
	d.cgroupfs = nil

	pod1 := &api.PodSandbox{Id: "p1", Uid: "pod-1", Name: "pod-1", Namespace: "default"}
	// The whole node, which is what a container with no cpuset of its own gets.
	ctrA := &api.Container{
		Id:           "ctr-a",
		PodSandboxId: "p1",
		Name:         "ctr-a",
		Env:          []string{fmt.Sprintf("%s_%s=0-1", cdiEnvVarPrefix, claimA)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-a",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-7"}},
		},
	}
	ctrB := &api.Container{
		Id:           "ctr-b",
		PodSandboxId: "p1",
		Name:         "ctr-b",
		Env:          []string{fmt.Sprintf("%s_%s=2-3", cdiEnvVarPrefix, claimB)},
		Linux: &api.LinuxContainer{
			CgroupsPath: "/kubepods/ctr-b",
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}},
		},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod1}, []*api.Container{ctrA, ctrB})
	require.NoError(t, err)

	require.Equal(t, "0-1", updateFor(t, updates, "ctr-a"),
		"the unpinned container is carried back onto the CPUs its claim holds")
	for _, update := range updates {
		require.NotEqual(t, "ctr-b", update.ContainerId, "a container already where it belongs is left alone")
	}
	require.False(t, d.nodeIsPoisoned(0), "an unreadable cgroup tree is not an ambiguity to fence over")
}

func TestRevertRoundOriginPicksTheRequestTheRoundMoved(t *testing.T) {
	// A claim holding a pool share besides its own CPUs: "helpers" sorts before
	// "vcpus", so writing the origin into the first request would put the
	// exclusive CPUs into the pool share and leave the vCPUs where the round
	// put them.
	record := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "helpers", CPUs: cpuset.New(6, 7), Role: store.RoleShared},
			{Request: "vcpus", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive},
		},
		Round: &store.RoundProvenance{RoundID: "r", Origin: cpuset.New(0, 1), Target: cpuset.New(2, 3)},
	}
	require.True(t, revertRoundOrigin(&record))
	require.Equal(t, cpuset.New(6, 7), record.Requests[0].CPUs, "the pool share never moves")
	require.Equal(t, cpuset.New(0, 1), record.Requests[1].CPUs)

	// Two exclusive requests share the target, each keeping its own size.
	split := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "a", CPUs: cpuset.New(4), Role: store.RoleExclusive},
			{Request: "b", CPUs: cpuset.New(5, 6), Role: store.RoleExclusive},
		},
		Round: &store.RoundProvenance{RoundID: "r", Origin: cpuset.New(0, 1, 2), Target: cpuset.New(4, 5, 6)},
	}
	require.True(t, revertRoundOrigin(&split))
	require.Equal(t, cpuset.New(0), split.Requests[0].CPUs)
	require.Equal(t, cpuset.New(1, 2), split.Requests[1].CPUs)

	// Something already rewrote the record: guessing is how a claim ends up
	// holding CPUs twice.
	moved := store.ClaimRecord{
		Requests: []store.RequestAllocation{{Request: "vcpus", CPUs: cpuset.New(8, 9), Role: store.RoleExclusive}},
		Round:    &store.RoundProvenance{RoundID: "r", Origin: cpuset.New(0, 1), Target: cpuset.New(2, 3)},
	}
	require.False(t, revertRoundOrigin(&moved))
	require.Equal(t, cpuset.New(8, 9), moved.Requests[0].CPUs)
}
