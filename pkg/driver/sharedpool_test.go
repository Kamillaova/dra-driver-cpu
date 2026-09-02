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
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// smtPoolInfos is 2 NUMA nodes x 2 caches x 2 cores x 2 threads (16 CPUs):
// core c = {c, c+8}, cache = c/2, NUMA node = c/4.
func smtPoolInfos() []cpuinfo.CPUInfo {
	var infos []cpuinfo.CPUInfo
	for cpu := range 16 {
		core := cpu % 8
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: core, SocketID: 0,
			NUMANodeID:    core / 4,
			UncoreCacheID: core / 2,
			SiblingCPUID:  (cpu + 8) % 16,
		})
	}
	return infos
}

// newSharedPoolTestDriver carves core 1 of NUMA node 0 ({1,9}) and core 5 of
// NUMA node 1 ({5,13}) as the static pool.
func newSharedPoolTestDriver(t *testing.T) *defragTestDriver {
	t.Helper()
	logger := testr.New(t)
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: smtPoolInfos()}).GetCPUTopology(logger)
	require.NoError(t, err)

	pool := cpuset.New(1, 9, 5, 13)
	allCPUs := topo.CPUDetails.CPUs()
	updater := &fakeContainerUpdater{}
	cdi := newMockCdiMgr()
	d := &CPUDriver{
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: pool, onlineCPUs: allCPUs},
		sharedPool:         pool,
		cpuAllocationStore: store.NewCPUAllocation(topo, pool),
		podConfigStore:     store.NewPodConfig(),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             cdi,
		containerUpdater:   updater,
		reconcileTrigger:   make(chan struct{}, 1),
		lastMoved:          make(map[types.UID]time.Time),
		sysfs: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte(allCPUs.String() + "\n")},
		},
		defrag: defragOptions{enabled: true, maxMoves: 4, minGain: 1},
	}
	return &defragTestDriver{CPUDriver: d, updater: updater, cdi: cdi, allCPUs: allCPUs}
}

func TestSharedPoolPinsClaimlessContainersConstantly(t *testing.T) {
	d := newSharedPoolTestDriver(t)
	logger := testr.New(t)
	pod := &api.PodSandbox{Id: "pod-s", Uid: "pod-uid-s", Name: "shared", Namespace: "ns"}
	ctr := &api.Container{Id: "ctr-s", PodSandboxId: pod.Id, Name: "shared-ctr"}

	adjust, updates, err := d.CreateContainer(context.Background(), pod, ctr)
	require.NoError(t, err)
	require.Empty(t, updates)
	require.Equal(t, "1,5,9,13", adjust.GetLinux().GetResources().GetCpu().GetCpus(),
		"a claimless container is confined to the whole pool, nothing else")

	// A claim being prepared and released must not touch it: the pool is
	// static, so there is nothing to narrow onto or widen back to.
	require.NoError(t, d.reserveResourceClaimAllocation(logger, "claim-1", cpuset.New(0, 8)))
	shared, err := d.getSharedContainerUpdates(logger, "")
	require.NoError(t, err)
	require.Len(t, shared, 1)
	require.Equal(t, "1,5,9,13", shared[0].GetLinux().GetResources().GetCpu().GetCpus())

	d.cpuAllocationStore.RemoveResourceClaimAllocation(logger, "claim-1")
	d.reconcileSharedContainers(context.Background())
	require.Empty(t, d.updater.allCalls(), "a static pool has nothing to reconcile after a release")
}

func TestSharedPoolAppendsOnlyTheClaimsNUMANodesPool(t *testing.T) {
	d := newSharedPoolTestDriver(t)
	d.placeClaim(t, "claim-1", cpuset.New(0, 8))
	pod := &api.PodSandbox{Id: "pod-g", Uid: "pod-uid-g", Name: "vm", Namespace: "ns"}
	ctr := &api.Container{
		Id: "ctr-g", PodSandboxId: pod.Id, Name: "vm-ctr",
		Env: []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-1", cdiEnvDynamicValue)},
	}

	adjust, _, err := d.CreateContainer(context.Background(), pod, ctr)
	require.NoError(t, err)
	require.Equal(t, "0-1,8-9", adjust.GetLinux().GetResources().GetCpu().GetCpus(),
		"the claim on NUMA node 0 gets node 0's pool core {1,9}, not node 1's")
}

func TestSharedPoolSurvivesSynchronize(t *testing.T) {
	d := newSharedPoolTestDriver(t)
	logger := testr.New(t)
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName("claim-1"),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-1", cdiEnvDynamicValue), cpuset.New(0, 8)))

	pod := &api.PodSandbox{Id: "pod-1", Uid: "pod-uid-1", Name: "pod", Namespace: "ns"}
	guaranteed := &api.Container{
		Id: "ctr-g", PodSandboxId: pod.Id, Name: "vm-ctr",
		Env: []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, "claim-1", cdiEnvDynamicValue)},
	}
	shared := &api.Container{Id: "ctr-s", PodSandboxId: pod.Id, Name: "shared-ctr"}

	updates, err := d.Synchronize(context.Background(), []*api.PodSandbox{pod}, []*api.Container{guaranteed, shared})
	require.NoError(t, err)
	require.Equal(t, "0-1,8-9", updateFor(t, updates, "ctr-g"))
	require.Equal(t, "1,5,9,13", updateFor(t, updates, "ctr-s"))
}

func TestSharedPoolDefragRoundCarriesOnlyGuaranteedContainers(t *testing.T) {
	// NUMA node 0 has caches 0 {0,1,8,9} and 1 {2,3,10,11}; the pool holds core
	// {1,9}, so the claim on core {0,8} plus core {2,10} straddles both caches
	// while cache 1 could hold it whole.
	d := newSharedPoolTestDriver(t)
	d.wholeCoreStep = 2
	d.placeClaim(t, "claim-1", cpuset.New(0, 8, 2, 10))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "shared-ctr", "shared-uid")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 1, "a static pool never changes, so the batch is the guaranteed container alone")
	require.Equal(t, "ctr-uid-1", calls[0][0].GetContainerId())
	moved, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, moved), "the claim must end inside one cache")
	require.Equal(t, moved.Union(cpuset.New(1, 9)).String(), calls[0][0].GetLinux().GetResources().GetCpu().GetCpus(),
		"the moved container keeps its NUMA node's pool appended")
	require.Empty(t, d.reconcileTrigger, "nothing to widen afterwards: the pool never narrowed")
}

func TestSharedPoolIsUnreachableByClaims(t *testing.T) {
	d := newSharedPoolTestDriver(t)
	err := d.reserveResourceClaimAllocation(testr.New(t), "claim-greedy", cpuset.New(1, 2))
	require.Error(t, err, "a reservation touching the pool must be refused")
	require.Contains(t, err.Error(), "overlapping")
}

func TestNewCarvesTheSharedPoolOutOfCapacity(t *testing.T) {
	logger := testr.New(t)
	providers := Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: smtPoolInfos()},
		SysFS: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte("0-15\n")},
		},
	}
	d, err := New(logger, providers, &Config{
		DriverName:       "dra.cpu",
		NodeName:         "test-node",
		CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
		ReservedCPUs:     cpuset.New(0),
		SharedPoolCPUs:   cpuset.New(1, 9, 5, 13),
		KubeletRootDir:   "/var/lib/kubelet",
	})
	require.NoError(t, err)

	require.Equal(t, cpuset.New(1, 9, 5, 13), d.sharedPool)
	require.Equal(t, cpuset.New(0, 1, 5, 9, 13), d.topology.reservedCPUs,
		"the pool folds into the effective reserved set")

	require.Len(t, d.topology.deviceSlices, 1)
	byName := map[string]int64{}
	for _, dev := range d.topology.deviceSlices[0] {
		capacity := dev.Capacity["dra.cpu/cpu"].Value
		byName[dev.Name] = capacity.Value()
	}
	// Node 0: 8 CPUs minus reserved 0 minus pool {1,9} = 5. Node 1: 8 minus
	// pool {5,13} = 6. Capacity must exclude the pool on both.
	require.Equal(t, map[string]int64{"cpudevnuma000": 5, "cpudevnuma001": 6}, byName)
}

func TestNewRefusesABadSharedPool(t *testing.T) {
	base := func() *Config {
		return &Config{
			DriverName:       "dra.cpu",
			NodeName:         "test-node",
			CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
			CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
			KubeletRootDir:   "/var/lib/kubelet",
		}
	}
	providers := func(online string) Providers {
		return Providers{
			CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: smtPoolInfos()},
			SysFS: fstest.MapFS{
				"devices/system/cpu/online": &fstest.MapFile{Data: []byte(online + "\n")},
			},
		}
	}
	logger := testr.New(t)

	cfg := base()
	cfg.SharedPoolCPUs = cpuset.New(1, 9, 99)
	_, err := New(logger, providers("0-15"), cfg)
	require.ErrorContains(t, err, "names CPUs this node does not have: 99")

	cfg = base()
	cfg.SharedPoolCPUs = cpuset.New(14, 15)
	_, err = New(logger, providers("0-13"), cfg)
	require.ErrorContains(t, err, "names offline CPUs")

	// Splitting a core: hard failure only under whole-core allocation, whose
	// promise it would contradict; a warning otherwise.
	cfg = base()
	cfg.SharedPoolCPUs = cpuset.New(1)
	cfg.FullPhysicalCPUsOnly = true
	_, err = New(logger, providers("0-15"), cfg)
	require.ErrorContains(t, err, "splits physical cores")

	cfg = base()
	cfg.SharedPoolCPUs = cpuset.New(1)
	d, err := New(logger, providers("0-15"), cfg)
	require.NoError(t, err, "without whole-core allocation a split core is the operator's trade")
	require.Equal(t, cpuset.New(1), d.sharedPool)
}

func TestSharedPoolIsReportedDistinctFromReserved(t *testing.T) {
	d := newSharedPoolTestDriver(t)
	d.nodeName = "worker"
	report, err := d.placements(testr.New(t), false)
	require.NoError(t, err)
	require.Equal(t, "1,5,9,13", report.SharedPoolCPUs)
	require.Equal(t, "1,5,9,13", report.SharedCPUs, "the shared mask is the pool")
	require.Equal(t, "", report.ReservedCPUs,
		"nothing here is truly reserved; the fold must not leak into the report")
}
