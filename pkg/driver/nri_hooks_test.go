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

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
)

func TestParseDRAEnvToClaimAllocations(t *testing.T) {
	logger := testr.New(t)
	testCases := []struct {
		name                  string
		envs                  []string
		expectedAllocations   map[types.UID]cpuset.CPUSet
		expectedErrorContains string
	}{
		{
			name: "single valid env",
			envs: []string{fmt.Sprintf("%s_claim-uid-1=%s", cdiEnvVarPrefix, "0-1")},
			expectedAllocations: map[types.UID]cpuset.CPUSet{
				"claim-uid-1": cpuset.New(0, 1),
			},
		},
		{
			name: "multiple valid envs",
			envs: []string{
				fmt.Sprintf("%s_claim-uid-1=%s", cdiEnvVarPrefix, "0,1"),
				fmt.Sprintf("%s_claim-uid-2=%s", cdiEnvVarPrefix, "2,3"),
			},
			expectedAllocations: map[types.UID]cpuset.CPUSet{
				"claim-uid-1": cpuset.New(0, 1),
				"claim-uid-2": cpuset.New(2, 3),
			},
		},
		{
			name:                "no relevant envs",
			envs:                []string{"OTHER_ENV=value"},
			expectedAllocations: map[types.UID]cpuset.CPUSet{},
		},
		{
			name:                  "malformed env - no equals",
			envs:                  []string{fmt.Sprintf("%s_claim-uid-1", cdiEnvVarPrefix)},
			expectedErrorContains: "malformed DRA env entry",
		},
		{
			name:                  "malformed env - invalid cpuset",
			envs:                  []string{fmt.Sprintf("%s_claim-uid-1=%s", cdiEnvVarPrefix, "a-b")},
			expectedErrorContains: "failed to parse cpuset value",
		},
		{
			name:                "empty env",
			envs:                []string{},
			expectedAllocations: map[types.UID]cpuset.CPUSet{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocations, err := parseDRAEnvToClaimAllocations(logger, tc.envs)
			if tc.expectedErrorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrorContains)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(tc.expectedAllocations), len(allocations))
				for uid, expectedCpus := range tc.expectedAllocations {
					actualCpus, ok := allocations[uid]
					require.True(t, ok)
					require.True(t, expectedCpus.Equals(actualCpus))
				}
			}
		})
	}
}

func TestCreateContainer(t *testing.T) {
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	pod := &api.PodSandbox{Id: "pod-id-1", Name: "my-pod", Namespace: "my-ns", Uid: "pod-uid-1"}
	claimUID := "claim-uid-1"

	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	logger := testr.New(t)
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)

	// newTestContainer is a local helper to simplify test case definitions.
	newTestContainer := func(claimUID, cpus string) *api.Container {
		var envs []string
		if cpus != "" {
			envs = append(envs, fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cpus))
		}
		return &api.Container{
			Id:           "ctr-id-1",
			PodSandboxId: pod.Id,
			Name:         "my-ctr",
			Env:          envs,
		}
	}

	// reservedForPod builds a ClaimTracker recording pod as the sole reserved
	// consumer of each claim, the bookkeeping a real Prepare call leaves behind
	// (see PrepareResourceClaims) and which CreateContainer falls back to when
	// the runtime reports no CDI devices at all.
	reservedForPod := func(claimUIDs ...string) *store.ClaimTracker {
		ct := store.NewClaimTracker()
		for _, claimUID := range claimUIDs {
			ct.SetReservedFor(types.UID(claimUID), []types.UID{types.UID(pod.Uid)})
		}
		return ct
	}

	testCases := []struct {
		name                        string
		podConfigStore              *store.PodConfig
		cpuAllocationStore          *store.CPUAllocation
		claimTracker                *store.ClaimTracker
		container                   *api.Container
		expectedContainerAdjustment *api.ContainerAdjustment
		expectedContainerUpdates    []*api.ContainerUpdate
		expectedErrorContains       string
	}{
		{
			name:           "guaranteed container triggers container adjustment with cpus in resource claim",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				store := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, store, types.UID(claimUID), cpuset.New(0, 1, 2, 3))
				return store
			}(),
			claimTracker: reservedForPod(claimUID),
			container:    newTestContainer(claimUID, "0-3"),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-3"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
		{
			name:               "shared container triggers container adjustment with all cpus",
			podConfigStore:     store.NewPodConfig(),
			cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
			claimTracker:       store.NewClaimTracker(),
			container:          newTestContainer("", ""),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-7"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
		{
			name:           "shared container created after guaranteed allocation uses shared pool",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				store := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, store, types.UID(claimUID), cpuset.New(0, 1, 2, 3))
				return store
			}(),
			claimTracker: store.NewClaimTracker(),
			container:    newTestContainer("", ""),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "4-7"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
		{
			name:           "shared container is rejected when shared pool is empty",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				allocation := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, allocation, types.UID(claimUID), allCPUs)
				return allocation
			}(),
			claimTracker:          store.NewClaimTracker(),
			container:             newTestContainer("", ""),
			expectedErrorContains: "cannot create shared container: no shared CPUs available",
		},
		{
			name: "guaranteed container triggers container adjustment and update for other shared container",
			podConfigStore: func() *store.PodConfig {
				conf := store.NewPodConfig()
				conf.SetContainerState("shared-pod-1", store.NewContainerState("shared-ctr-1", "shared-uid-1"))
				conf.SetContainerState("shared-pod-2", store.NewContainerState("shared-ctr-2", "shared-uid-2"))
				return conf
			}(),
			cpuAllocationStore: func() *store.CPUAllocation {
				store := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, store, types.UID(claimUID), cpuset.New(2, 3))
				return store
			}(),
			claimTracker: reservedForPod(claimUID),
			container:    newTestContainer(claimUID, "2-3"),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "shared-uid-1",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1,4-7"}}},
				},
				{
					ContainerId: "shared-uid-2",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1,4-7"}}},
				},
			},
		},
		{
			name: "guaranteed container is rejected when it would empty an existing shared pool",
			podConfigStore: func() *store.PodConfig {
				conf := store.NewPodConfig()
				conf.SetContainerState("shared-pod", store.NewContainerState("shared-ctr", "shared-uid"))
				return conf
			}(),
			cpuAllocationStore: func() *store.CPUAllocation {
				allocation := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, allocation, types.UID(claimUID), allCPUs)
				return allocation
			}(),
			claimTracker:          reservedForPod(claimUID),
			container:             newTestContainer(claimUID, "0-7"),
			expectedErrorContains: "cannot update shared containers: no shared CPUs available",
		},
		{
			name:           "guaranteed container may consume the full pool when no shared containers exist",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				allocation := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, allocation, types.UID(claimUID), allCPUs)
				return allocation
			}(),
			claimTracker: reservedForPod(claimUID),
			container:    newTestContainer(claimUID, "0-7"),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-7"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
		{
			name:               "guaranteed container with malformed env fails closed",
			podConfigStore:     store.NewPodConfig(),
			cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
			claimTracker:       store.NewClaimTracker(),
			container: &api.Container{
				Id:           "ctr-id-1",
				PodSandboxId: pod.Id,
				Name:         "my-ctr",
				Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, "a-b")},
			},
			expectedErrorContains: "failed to parse cpuset value",
		},
		{
			// The claim was never prepared, so it was never reserved for any
			// pod either: the runtime-CDI-device fallback rejects it before
			// the allocation store is even consulted.
			name:               "container with DRA env for unprepared claim fails closed",
			podConfigStore:     store.NewPodConfig(),
			cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
			claimTracker:       store.NewClaimTracker(),
			container:          newTestContainer(claimUID, "0-3"),
			expectedErrorContains: fmt.Sprintf(
				"container claims %q but the pod is not in its reservation",
				claimUID,
			),
		},
		{
			// A container's environment is fixed when it is created, so once a
			// claim can move the env names where the claim used to be. The store
			// is the live record and decides; disagreeing with it would leave a
			// moved claim unable to start or restart its container.
			name:           "container is pinned to the prepared allocation, not to its DRA env",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				store := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, store, types.UID(claimUID), cpuset.New(0, 1))
				return store
			}(),
			claimTracker: reservedForPod(claimUID),
			container:    newTestContainer(claimUID, "0-3"),
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
		{
			name:           "container holding several claims gets the union of their allocations",
			podConfigStore: store.NewPodConfig(),
			cpuAllocationStore: func() *store.CPUAllocation {
				store := store.NewCPUAllocation(topo, cpuset.New())
				requirePreparedResourceClaim(t, logger, store, "claim-uid-1", cpuset.New(0, 1))
				requirePreparedResourceClaim(t, logger, store, "claim-uid-2", cpuset.New(5))
				return store
			}(),
			claimTracker: reservedForPod("claim-uid-1", "claim-uid-2"),
			container: &api.Container{
				Id:           "ctr-id-1",
				PodSandboxId: pod.Id,
				Name:         "my-ctr",
				Env: []string{
					fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-uid-1", "6"),
					fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-uid-2", "7"),
				},
			},
			expectedContainerAdjustment: &api.ContainerAdjustment{
				Linux: &api.LinuxContainerAdjustment{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1,5"}}},
			},
			expectedContainerUpdates: []*api.ContainerUpdate{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := &CPUDriver{
				podConfigStore:     tc.podConfigStore,
				cpuAllocationStore: tc.cpuAllocationStore,
				claimTracker:       tc.claimTracker,
			}
			adjust, updates, err := driver.CreateContainer(context.Background(), pod, tc.container)
			if tc.expectedErrorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrorContains)
				require.Nil(t, adjust)
				require.Nil(t, updates)
				require.Zero(t, tc.claimTracker.Len(), "failed CreateContainer must not retain claim owners")
				require.Nil(t, tc.podConfigStore.GetContainerState(types.UID(pod.Uid), tc.container.Name), "failed CreateContainer must not retain container state")
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedContainerAdjustment, adjust)
			require.ElementsMatch(t, tc.expectedContainerUpdates, updates)
		})
	}
}

func TestCreateContainerRuntimeCDIDeviceAuthentication(t *testing.T) {
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	pod := &api.PodSandbox{Id: "pod-id-1", Name: "my-pod", Namespace: "my-ns", Uid: "pod-uid-1"}
	victimClaim := types.UID("claim-uid-victim")

	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	logger := testr.New(t)
	topo, _ := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)

	cdiNameFor := func(uid types.UID) string {
		return cdiparser.QualifiedName(cdiVendor, cdiClass, getCDIDeviceName(uid))
	}

	// A forged env entry naming a claim prepared for another container.
	forgingContainer := func(reported []*api.CDIDevice) *api.Container {
		return &api.Container{
			Id:           "forging-ctr",
			PodSandboxId: pod.Id,
			Name:         "forging-ctr",
			Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, victimClaim, "0-3")},
			CDIDevices:   reported,
		}
	}

	testCases := []struct {
		name      string
		container *api.Container
		// reservedFor is who victimClaim's own reservation (recorded at
		// Prepare) names, consulted only when the runtime reports no CDI
		// devices at all. Empty means the reservation was never recorded.
		reservedFor           types.UID
		expectedErrorContains string
	}{
		{
			name:      "runtime confirms the claim",
			container: forgingContainer([]*api.CDIDevice{{Name: cdiNameFor(victimClaim)}}),
		},
		{
			name:                  "runtime reports a different claim",
			container:             forgingContainer([]*api.CDIDevice{{Name: cdiNameFor("claim-uid-other")}}),
			expectedErrorContains: "the runtime injected no CDI device for it",
		},
		{
			name: "runtime reports another driver's device only",
			container: forgingContainer([]*api.CDIDevice{
				{Name: "example.com/gpu=gpu0"},
			}),
			expectedErrorContains: "the runtime injected no CDI device for it",
		},
		{
			// CRI-O never populates the field. An empty list must stay
			// inconclusive rather than an outright rejection, so this driver
			// keeps working there -- but it must fall back to the claim's own
			// reservation rather than trust the forged env entry blindly.
			name:                  "runtime reports nothing and the pod is not in the claim's reservation",
			container:             forgingContainer(nil),
			reservedFor:           "pod-uid-legitimate-owner",
			expectedErrorContains: "the pod is not in its reservation",
		},
		{
			name:        "runtime reports nothing but the pod is the claim's reserved consumer",
			container:   forgingContainer(nil),
			reservedFor: types.UID(pod.Uid),
		},
		{
			name:                  "runtime reports nothing and the claim's reservation was never recorded",
			container:             forgingContainer(nil),
			expectedErrorContains: "the pod is not in its reservation",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			allocation := store.NewCPUAllocation(topo, cpuset.New())
			requirePreparedResourceClaim(t, logger, allocation, victimClaim, cpuset.New(0, 1, 2, 3))

			claimTracker := store.NewClaimTracker()
			if tc.reservedFor != "" {
				claimTracker.SetReservedFor(victimClaim, []types.UID{tc.reservedFor})
			}
			plugin := &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: allocation,
				claimTracker:       claimTracker,
			}

			_, _, err := plugin.CreateContainer(context.Background(), pod, tc.container)
			if tc.expectedErrorContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrorContains)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSynchronizeSkipsClaimsTheRuntimeDoesNotConfirm(t *testing.T) {
	allCPUs := cpuset.New(0, 1, 2, 3)
	claimUID := types.UID("claim-uid-1")

	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	logger := testr.New(t)
	topo, _ := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)

	pod := &api.PodSandbox{Id: "pod-1", Uid: "pod-uid-1", Name: "pod", Namespace: "ns"}
	ctr := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: pod.Id,
		Name:         "ctr",
		Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, "0,1")},
		// The runtime reports a device for some other claim, so this
		// container's DRA env entry is not corroborated.
		CDIDevices: []*api.CDIDevice{
			{Name: cdiparser.QualifiedName(cdiVendor, cdiClass, getCDIDeviceName("claim-uid-other"))},
		},
	}

	mgr := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{claimUID: cpuset.New(0, 1)})
	plugin := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             mgr,
	}

	_, err := plugin.Synchronize(context.Background(), []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err, "an unconfirmed claim is skipped, not fatal")

	// The claim was not adopted, so its CPUs stay in the shared pool rather
	// than being reserved on the strength of a forgeable env entry.
	require.Equal(t, allCPUs, plugin.cpuAllocationStore.GetSharedCPUs())
	require.Zero(t, plugin.claimTracker.Len())
}

func TestStopContainer(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	pod1 := &api.PodSandbox{Id: "pod-id-1", Name: "my-pod-1", Namespace: "my-ns", Uid: "pod-uid-1"}
	ctr1 := &api.Container{Id: "ctr-id-1", PodSandboxId: pod1.Id, Name: "my-ctr-1"}
	pod2 := &api.PodSandbox{Id: "pod-id-2", Name: "my-pod-2", Namespace: "my-ns", Uid: "pod-uid-2"}
	ctr2 := &api.Container{Id: "ctr-id-2", PodSandboxId: pod2.Id, Name: "my-ctr-2"}

	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)

	testCases := []struct {
		name   string
		driver *CPUDriver
	}{
		{
			name: "Stop guaranteed container does not update shared containers",
			driver: func() *CPUDriver {
				driver := &CPUDriver{
					podConfigStore:     store.NewPodConfig(),
					cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
					claimTracker:       store.NewClaimTracker(),
					topology:           deviceTopology{cpuTopology: topo},
				}
				claimUID := types.UID("claim-uid-1")
				requirePreparedResourceClaim(t, logger, driver.cpuAllocationStore, claimUID, cpuset.New(0, 1))
				driver.podConfigStore.SetContainerState(types.UID(pod1.Uid), store.NewContainerState(ctr1.Name, types.UID(ctr1.Id), claimUID))
				driver.podConfigStore.SetContainerState(types.UID(pod2.Uid), store.NewContainerState(ctr2.Name, types.UID(ctr2.Id)))
				return driver
			}(),
		},
		{
			name: "Stop non-guaranteed container does not set update required",
			driver: func() *CPUDriver {
				driver := &CPUDriver{
					podConfigStore:     store.NewPodConfig(),
					cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
					claimTracker:       store.NewClaimTracker(),
					topology:           deviceTopology{cpuTopology: topo},
				}
				driver.podConfigStore.SetContainerState(types.UID(pod1.Uid), store.NewContainerState(ctr1.Name, types.UID(ctr1.Id)))
				driver.podConfigStore.SetContainerState(types.UID(pod2.Uid), store.NewContainerState(ctr2.Name, types.UID(ctr2.Id)))
				return driver
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			upd, err := tc.driver.StopContainer(context.Background(), pod1, ctr1)
			require.NoError(t, err)
			require.Empty(t, upd)
		})
	}
}

func TestGuaranteedContainerRestartWithoutReprepare(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-restart")
	claimCPUs := cpuset.New(0, 1)
	cpuStore := store.NewCPUAllocation(topo, cpuset.New())
	require.NoError(t, cpuStore.ReserveResourceClaimAllocation(logger, claimUID, claimCPUs, false))
	claimTracker := store.NewClaimTracker()
	claimTracker.SetReservedFor(claimUID, []types.UID{"pod"})
	driver := &CPUDriver{
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: cpuStore,
		claimTracker:       claimTracker,
		topology:           deviceTopology{cpuTopology: topo},
	}
	driver.podConfigStore.SetContainerState("shared-pod", store.NewContainerState("shared", "shared-container"))

	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "ns"}
	container := func(id string) *api.Container {
		return &api.Container{
			Id:           id,
			PodSandboxId: pod.Id,
			Name:         "app",
			Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, claimCPUs.String())},
		}
	}

	first := container("container-1")
	adjustment, updates, err := driver.CreateContainer(context.Background(), pod, first)
	require.NoError(t, err)
	require.Equal(t, claimCPUs.String(), adjustment.Linux.Resources.Cpu.Cpus)
	require.Len(t, updates, 1)
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))

	updates, err = driver.StopContainer(context.Background(), pod, first)
	require.NoError(t, err)
	require.Empty(t, updates)
	preparedCPUs, ok := cpuStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.True(t, preparedCPUs.Equals(claimCPUs))
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))
	require.Equal(t, 1, driver.claimTracker.Len())

	restarted := container("container-2")
	adjustment, updates, err = driver.CreateContainer(context.Background(), pod, restarted)
	require.NoError(t, err)
	require.Equal(t, claimCPUs.String(), adjustment.Linux.Resources.Cpu.Cpus)
	require.Empty(t, updates)
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))
	require.Equal(t, 1, driver.claimTracker.Len())

	// Delayed events for the old runtime ID must not remove or release the replacement.
	require.NoError(t, driver.RemoveContainer(context.Background(), pod, first))
	require.NotNil(t, driver.podConfigStore.GetContainerState(types.UID(pod.Uid), restarted.Name))
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))

	updates, err = driver.StopContainer(context.Background(), pod, first)
	require.NoError(t, err)
	require.Empty(t, updates)
	require.NotNil(t, driver.podConfigStore.GetContainerState(types.UID(pod.Uid), restarted.Name))
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))

	updates, err = driver.StopContainer(context.Background(), pod, restarted)
	require.NoError(t, err)
	require.Empty(t, updates)
	require.Nil(t, driver.podConfigStore.GetContainerState(types.UID(pod.Uid), restarted.Name))
	require.True(t, cpuStore.GetSharedCPUs().Equals(cpuset.New(2, 3)))
}

func TestGuaranteedContainerRestartNotBlockedByEmptySharedPool(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	infos := make([]cpuinfo.CPUInfo, 0, allCPUs.Size())
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-full")
	cpuStore := store.NewCPUAllocation(topo, cpuset.New())
	require.NoError(t, cpuStore.ReserveResourceClaimAllocation(logger, claimUID, allCPUs, false))
	claimTracker := store.NewClaimTracker()
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "ns"}
	_, err = claimTracker.SetOwner(logger, types.UID(pod.Uid), "app", claimUID)
	require.NoError(t, err)
	claimTracker.SetReservedFor(claimUID, []types.UID{types.UID(pod.Uid)})

	driver := &CPUDriver{
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: cpuStore,
		claimTracker:       claimTracker,
		topology:           deviceTopology{cpuTopology: topo},
	}
	driver.podConfigStore.SetContainerState("shared-pod", store.NewContainerState("shared", "shared-container"))
	container := &api.Container{
		Id:           "replacement",
		PodSandboxId: pod.Id,
		Name:         "app",
		Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, allCPUs.String())},
	}

	// An existing exclusive container may restart even if the shared pool is
	// empty: restart does not emit shared-container updates, and exhausting the
	// shared pool is rejected earlier during DRA claim preparation.
	adjustment, updates, err := driver.CreateContainer(context.Background(), pod, container)
	require.NoError(t, err)
	require.NotNil(t, adjustment)
	require.Empty(t, updates)
	require.Equal(t, 1, claimTracker.Len(), "an existing owner must not be removed on restart rejection")
}

func TestNRISynchronize(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)

	pod1 := &api.PodSandbox{Id: "pod-id-1", Name: "my-pod-1", Namespace: "my-ns", Uid: "pod-uid-1"}
	pod2 := &api.PodSandbox{Id: "pod-id-2", Name: "my-pod-2", Namespace: "my-ns", Uid: "pod-uid-2"}

	testCases := []struct {
		name                 string
		driver               *CPUDriver
		runtimePods          []*api.PodSandbox
		runtimeCtrs          []*api.Container
		expectedUpdates      []*api.ContainerUpdate
		expectedError        string
		expectedRefreshCalls int
	}{
		{
			name: "empty runtime state clears the store",
			driver: func() *CPUDriver {
				driver := &CPUDriver{
					podConfigStore:     store.NewPodConfig(),
					cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
					claimTracker:       store.NewClaimTracker(),
					cdiMgr:             newMockCdiMgr(),
					topology:           deviceTopology{cpuTopology: topo},
				}
				driver.podConfigStore.SetContainerState(types.UID(pod1.Uid), store.NewContainerState("stale-ctr", "stale-id", types.UID("stale-claim")))
				return driver
			}(),
			runtimePods:     []*api.PodSandbox{},
			runtimeCtrs:     []*api.Container{},
			expectedUpdates: []*api.ContainerUpdate{},
		},
		{
			name: "mixed containers across multiple pods",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
				{Id: "p1-shared", PodSandboxId: pod1.Id, Name: "shared-ctr"},
				{Id: "p2-shared", PodSandboxId: pod2.Id, Name: "shared-ctr"},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
				},
				{
					ContainerId: "p1-shared",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-7"}}},
				},
				{
					ContainerId: "p2-shared",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-7"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			name: "only shared containers",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr:             newMockCdiMgr(),
				topology:           deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-shared", PodSandboxId: pod1.Id, Name: "shared-ctr"},
				{Id: "p2-shared", PodSandboxId: pod2.Id, Name: "shared-ctr"},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-shared",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-7"}}},
				},
				{
					ContainerId: "p2-shared",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-7"}}},
				},
			},
		},
		{
			name: "synchronize rejects an empty shared pool with existing shared containers",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-full": allCPUs,
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1},
			runtimeCtrs: []*api.Container{
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-full=%s", cdiEnvVarPrefix, allCPUs.String())}},
				{Id: "p1-shared", PodSandboxId: pod1.Id, Name: "shared-ctr"},
			},
			expectedError:        "cannot update shared containers: no shared CPUs available",
			expectedRefreshCalls: 1,
		},
		{
			name: "only guaranteed containers",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
					"claim-B": cpuset.New(2, 3),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
				{Id: "p2-guaranteed", PodSandboxId: pod2.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-B=%s", cdiEnvVarPrefix, "2,3")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
				},
				{
					ContainerId: "p2-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			name: "container with multiple claims",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
					"claim-B": cpuset.New(2, 3),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1},
			runtimeCtrs: []*api.Container{
				{Id: "p1-multi-claim", PodSandboxId: pod1.Id, Name: "multi-claim-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1"), fmt.Sprintf("%s_claim-B=%s", cdiEnvVarPrefix, "2,3")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-multi-claim",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-3"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			name: "malformed DRA env does not prevent other containers from synchronizing",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1},
			runtimeCtrs: []*api.Container{
				{Id: "p1-malformed", PodSandboxId: pod1.Id, Name: "malformed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "a-b")}},
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			name: "CDI cache refresh error does not prevent valid claims from synchronizing",
			driver: func() *CPUDriver {
				cdiMgr := newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
					"claim-B": cpuset.New(2, 3),
				})
				cdiMgr.refreshError = fmt.Errorf("unrelated invalid CDI spec")
				return &CPUDriver{
					podConfigStore:     store.NewPodConfig(),
					cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
					claimTracker:       store.NewClaimTracker(),
					cdiMgr:             cdiMgr,
					topology:           deviceTopology{cpuTopology: topo},
				}
			}(),
			runtimePods: []*api.PodSandbox{pod1},
			runtimeCtrs: []*api.Container{
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1"), fmt.Sprintf("%s_claim-B=%s", cdiEnvVarPrefix, "2,3")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-3"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			name: "invalid claim does not prevent other containers from synchronizing",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-B": cpuset.New(2, 3),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-invalid", PodSandboxId: pod1.Id, Name: "invalid-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
				{Id: "p2-guaranteed", PodSandboxId: pod2.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-B=%s", cdiEnvVarPrefix, "2,3")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p2-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}}},
				},
				{
					ContainerId: "p1-invalid",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1,4-7"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			// A container whose env names CPUs other than the driver's record is
			// one that was moved after it started: its environment was frozen at
			// creation and cannot be corrected. The claim must be carried to the
			// recorded placement, not dropped, or its CPUs leak into the shared
			// pool while the container still runs on them.
			name: "container on a stale cpuset is converged, not dropped",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(2, 3),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1},
			runtimeCtrs: []*api.Container{
				{Id: "p1-stale", PodSandboxId: pod1.Id, Name: "stale-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-stale",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-3"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			// Two distinct claims whose driver-owned CDI specs record the same
			// CPUs: an inconsistency in the runtime's reported state, not
			// something that should cost every other pod and container a
			// driver until the runtime gives up retrying it.
			name: "a claim overlapping an earlier one during synchronize is skipped, not fatal",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
					"claim-B": cpuset.New(0, 1),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-guaranteed", PodSandboxId: pod1.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
				{Id: "p2-guaranteed", PodSandboxId: pod2.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-B=%s", cdiEnvVarPrefix, "0,1")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
				},
				{
					// claim-B lost the race for 0-1 and is skipped, so its
					// container is treated as unclaimed and falls back to the
					// shared pool rather than the call failing outright.
					ContainerId: "p2-guaranteed",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-7"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
		{
			// The same claim named by two different containers: whichever is
			// processed second loses claimTracker's ownership check. Also an
			// inconsistency in the runtime's reported state, and also must not
			// fail every other pod and container being synchronized.
			name: "a claim already owned by a different container during synchronize is skipped, not fatal",
			driver: &CPUDriver{
				podConfigStore:     store.NewPodConfig(),
				cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
				claimTracker:       store.NewClaimTracker(),
				cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
					"claim-A": cpuset.New(0, 1),
				}),
				topology: deviceTopology{cpuTopology: topo},
			},
			runtimePods: []*api.PodSandbox{pod1, pod2},
			runtimeCtrs: []*api.Container{
				{Id: "p1-first-owner", PodSandboxId: pod1.Id, Name: "first-owner-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
				{Id: "p2-second-owner", PodSandboxId: pod2.Id, Name: "second-owner-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
			},
			expectedUpdates: []*api.ContainerUpdate{
				{
					ContainerId: "p1-first-owner",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-1"}}},
				},
				{
					ContainerId: "p2-second-owner",
					Linux:       &api.LinuxContainerUpdate{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "2-7"}}},
				},
			},
			expectedRefreshCalls: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			updates, err := tc.driver.Synchronize(context.Background(), tc.runtimePods, tc.runtimeCtrs)
			require.Equal(t, tc.expectedRefreshCalls, tc.driver.cdiMgr.(*mockCdiMgr).refreshCalls)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
				return
			}
			require.NoError(t, err)

			require.ElementsMatch(t, tc.expectedUpdates, updates)
		})
	}
}

func TestSynchronizeRestoresClaimReservationsForCreateContainer(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)

	pod := &api.PodSandbox{Id: "pod-id-1", Name: "my-pod", Namespace: "my-ns", Uid: "pod-uid-1"}
	driver := &CPUDriver{
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{
			"claim-A": cpuset.New(0, 1),
		}),
		topology: deviceTopology{cpuTopology: topo},
	}
	runtimeCtrs := []*api.Container{
		{Id: "p1-guaranteed", PodSandboxId: pod.Id, Name: "guaranteed-ctr", Env: []string{fmt.Sprintf("%s_claim-A=%s", cdiEnvVarPrefix, "0,1")}},
	}

	_, err := driver.Synchronize(context.Background(), []*api.PodSandbox{pod}, runtimeCtrs)
	require.NoError(t, err)

	// This is what CreateContainer's CRI-O fallback (nil reportedCDIDevices)
	// reads, on a container recreated after a driver restart with no fresh
	// Prepare in between.
	reserved, recorded := driver.claimTracker.ReservedFor("claim-A", types.UID(pod.Uid))
	require.True(t, recorded)
	require.True(t, reserved)

	reserved, recorded = driver.claimTracker.ReservedFor("claim-A", types.UID("some-other-pod"))
	require.True(t, recorded)
	require.False(t, reserved)

	_, recorded = driver.claimTracker.ReservedFor("claim-never-seen", types.UID(pod.Uid))
	require.False(t, recorded)
}

func TestStopContainerKeepsClaimOutOfSharedPoolUntilUnprepare(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)

	guaranteedPod := &api.PodSandbox{Id: "pod-id-g", Name: "guaranteed-pod", Namespace: "ns", Uid: "pod-uid-g"}
	guaranteedCtr := &api.Container{Id: "ctr-id-g", PodSandboxId: guaranteedPod.Id, Name: "guaranteed-ctr"}
	sharedPod := &api.PodSandbox{Id: "pod-id-s", Name: "shared-pod", Namespace: "ns", Uid: "pod-uid-s"}
	sharedCtr := &api.Container{Id: "ctr-id-s", PodSandboxId: sharedPod.Id, Name: "shared-ctr"}
	claimUID := types.UID("claim-uid-1")
	claimedCPUs := cpuset.New(0, 1)

	cpuAllocationStore := store.NewCPUAllocation(topo, cpuset.New())
	requirePreparedResourceClaim(t, logger, cpuAllocationStore, claimUID, claimedCPUs)

	driver := &CPUDriver{
		cdiMgr:             newMockCdiMgr(),
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: cpuAllocationStore,
		claimTracker:       store.NewClaimTracker(),
		topology:           deviceTopology{cpuTopology: topo},
	}
	driver.cdiMgr.(*mockCdiMgr).devices[getCDIDeviceName(claimUID)] = []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, claimedCPUs.String())}
	_, err := driver.claimTracker.SetOwner(logger, types.UID(guaranteedPod.Uid), guaranteedCtr.Name, claimUID)
	require.NoError(t, err)
	driver.podConfigStore.SetContainerState(types.UID(guaranteedPod.Uid),
		store.NewContainerState(guaranteedCtr.Name, types.UID(guaranteedCtr.Id), claimUID))
	driver.podConfigStore.SetContainerState(types.UID(sharedPod.Uid),
		store.NewContainerState(sharedCtr.Name, types.UID(sharedCtr.Id)))

	// Precondition: the claimed CPUs are held out of the shared pool.
	require.True(t, driver.cpuAllocationStore.GetSharedCPUs().Equals(allCPUs.Difference(claimedCPUs)),
		"precondition: shared pool should exclude the claimed CPUs")

	updates, err := driver.StopContainer(context.Background(), guaranteedPod, guaranteedCtr)
	require.NoError(t, err)

	// StopContainer does not change DRA-owned allocation state or shared container cpusets.
	require.True(t, driver.cpuAllocationStore.GetSharedCPUs().Equals(allCPUs.Difference(claimedCPUs)),
		"StopContainer must keep the claim's CPUs out of the shared pool")
	preparedCPUs, ok := driver.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok, "StopContainer must retain the prepared claim")
	require.True(t, preparedCPUs.Equals(claimedCPUs))
	require.Empty(t, updates)

	// UnprepareResourceClaims is the authoritative release point.
	unprepared, err := driver.UnprepareResourceClaims(context.Background(), []kubeletplugin.NamespacedObject{{UID: claimUID}})
	require.NoError(t, err)
	require.NoError(t, unprepared[claimUID])
	require.True(t, driver.cpuAllocationStore.GetSharedCPUs().Equals(allCPUs))
}

func TestSynchronizeKeepsStaleContainersClaimCPUsReserved(t *testing.T) {
	// The failure this guards against: a claim moved after its container started
	// has a frozen, now-wrong env var. Dropping it on that basis would return
	// CPUs the container is still pinned to into the shared pool, so two
	// workloads would end up on the same "exclusive" CPUs.
	logger := testr.New(t)
	var infos []cpuinfo.CPUInfo
	for cpu := range 8 {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-moved")
	pod := &api.PodSandbox{Id: "pod-1", Uid: "pod-uid-1", Name: "pod", Namespace: "ns"}
	ctr := &api.Container{
		Id:           "ctr-1",
		PodSandboxId: pod.Id,
		Name:         "ctr",
		// Frozen at creation: the claim was on 0-1 back then.
		Env: []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, "0-1")},
	}

	// The driver's own record says it has since moved to 4-5.
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{claimUID: cpuset.New(4, 5)}),
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err)

	// The recorded placement is reserved, so it stays out of the shared pool.
	got, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok, "claim must be adopted, not dropped")
	require.Equal(t, cpuset.New(4, 5), got)
	require.Equal(t, cpuset.New(0, 1, 2, 3, 6, 7), d.cpuAllocationStore.GetSharedCPUs())

	// And the container is carried to it.
	require.Len(t, updates, 1)
	require.Equal(t, "ctr-1", updates[0].GetContainerId())
	require.Equal(t, "4-5", updates[0].GetLinux().GetResources().GetCpu().GetCpus())
}

func TestDynamicEnvIsAcceptedAndNamesItsClaim(t *testing.T) {
	logger := testr.New(t)
	entries, err := parseDRAEnv(logger, []string{
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-dynamic", cdiEnvDynamicValue),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-fixed", "2-3"),
		"UNRELATED=whatever",
	})
	require.NoError(t, err)
	require.Len(t, entries, 2)

	require.Equal(t, types.UID("claim-dynamic"), entries[0].claimUID)
	require.True(t, entries[0].dynamic)
	require.True(t, entries[0].cpus.IsEmpty())

	require.Equal(t, types.UID("claim-fixed"), entries[1].claimUID)
	require.False(t, entries[1].dynamic)
	require.Equal(t, cpuset.New(2, 3), entries[1].cpus)

	// A value that is neither is still rejected: the prefix is reserved, and a
	// pod setting a bad one must not be started as if it held nothing.
	_, err = parseDRAEnv(logger, []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-bad", "a-b")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse cpuset value")
}

func TestSynchronizeAdoptsAClaimWithADynamicEnv(t *testing.T) {
	// The env says nothing about placement, so the spec on disk is the only
	// record, and the container must be pinned from it.
	logger := testr.New(t)
	var infos []cpuinfo.CPUInfo
	for cpu := range 8 {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: 0})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	claimUID := types.UID("claim-1")
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New()},
		podConfigStore:     store.NewPodConfig(),
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             newMockCdiMgr(),
	}
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID),
		[]string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue)}, cpuset.New(2, 3)))

	pod := &api.PodSandbox{Id: "pod-1", Uid: "pod-uid-1", Name: "pod", Namespace: "ns"}
	ctr := &api.Container{
		Id:           "ctr-uid-1",
		PodSandboxId: pod.Id,
		Name:         "ctr",
		Env:          []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue)},
	}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err)

	got, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok, "the claim must be adopted, not dropped")
	require.Equal(t, cpuset.New(2, 3), got)
	require.Len(t, updates, 1)
	require.Equal(t, "ctr-uid-1", updates[0].GetContainerId())
	require.Equal(t, "2-3", updates[0].GetLinux().GetResources().GetCpu().GetCpus())
}
