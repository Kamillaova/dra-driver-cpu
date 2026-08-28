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
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// defragTestDriver is a driver on one NUMA node of caches x cpusPerCache CPUs,
// with defragmentation on and no cooldown, so a test that wants one can set it.
type defragTestDriver struct {
	*CPUDriver
	updater *fakeContainerUpdater
	cdi     *mockCdiMgr
	metrics *prometheus.Registry
	allCPUs cpuset.CPUSet
}

func newDefragTestDriver(t *testing.T, caches, cpusPerCache int) *defragTestDriver {
	t.Helper()
	logger := testr.New(t)

	var infos []cpuinfo.CPUInfo
	for cpu := range caches * cpusPerCache {
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: 0,
			UncoreCacheID: cpu / cpusPerCache,
		})
	}
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
	require.NoError(t, err)

	allCPUs := topo.CPUDetails.CPUs()
	updater := &fakeContainerUpdater{}
	cdi := newMockCdiMgr()
	reg := prometheus.NewRegistry()
	d := &CPUDriver{
		metrics:            cpumetrics.New(reg),
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New(), onlineCPUs: allCPUs},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
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
	return &defragTestDriver{CPUDriver: d, updater: updater, cdi: cdi, metrics: reg, allCPUs: allCPUs}
}

// errUpdateFailed stands in for a runtime that could not be reached.
var errUpdateFailed = errors.New("connection reset")

// placeClaim records a prepared claim on the given CPUs, as Prepare would.
func (d *defragTestDriver) placeClaim(t *testing.T, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	logger := testr.New(t)
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, cpus, false))
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cpus.String()), cpus))
}

// runContainer binds claims to a running container, as CreateContainer would.
func (d *defragTestDriver) runContainer(t *testing.T, podUID types.UID, name string, containerUID types.UID, claimUIDs ...types.UID) {
	t.Helper()
	if len(claimUIDs) > 0 {
		_, err := d.claimTracker.SetOwner(testr.New(t), podUID, name, claimUIDs...)
		require.NoError(t, err)
	}
	d.podConfigStore.SetContainerState(podUID, store.NewContainerState(name, containerUID, claimUIDs...))
}

// recordedPlacement is the cpuset a claim's CDI spec names.
func (d *defragTestDriver) recordedPlacement(t *testing.T, claimUID types.UID) cpuset.CPUSet {
	t.Helper()
	cpus, err := d.cdiMgr.GetDeviceCPUSet(getCDIDeviceName(claimUID))
	require.NoError(t, err)
	return cpus
}

func updateFor(t *testing.T, updates []*api.ContainerUpdate, containerID string) string {
	t.Helper()
	for _, update := range updates {
		if update.GetContainerId() == containerID {
			return update.GetLinux().GetResources().GetCpu().GetCpus()
		}
	}
	t.Fatalf("no update for container %q in %v", containerID, updates)
	return ""
}

func TestDefragPassMovesASplitClaim(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	// One CPU in each cache, where one cache would hold both.
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1, "one round, whatever it contains")
	require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-1"))

	moved, ok := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), moved)
	require.Equal(t, cpuset.New(0, 1), d.recordedPlacement(t, "claim-1"),
		"the spec on disk is what a restart rebuilds from")
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight, "the move must be committed, not left half-done")

	// The CPUs it left are back in the pool, and nothing is double-counted.
	require.Equal(t, d.allCPUs.Difference(cpuset.New(0, 1)), d.cpuAllocationStore.GetSharedCPUs())

	// A node that is already packed is left alone.
	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 1, "a packed node must not be disturbed")
}

func TestDefragPassDoesNothingWhenDisabled(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.defrag.enabled = false
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 4), cpus)
}

func TestDefragPassDoesNothingWithoutAnUpdater(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.containerUpdater = nil
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	// Must not panic, and must not record a move it cannot apply.
	d.defragPass(context.Background())
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 4), cpus)
}

func TestDefragPassLeavesARefusedMoveWhereItWas(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.updater.failed = []*api.ContainerUpdate{{ContainerId: "ctr-uid-1"}}

	d.defragPass(context.Background())

	// The container never left its CPUs, so the claim must not either -- and its
	// recorded placement has to say so, since that is what a restart trusts.
	cpus, ok := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 4), cpus)
	require.Equal(t, cpuset.New(0, 4), d.recordedPlacement(t, "claim-1"))
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
	require.Equal(t, d.allCPUs.Difference(cpuset.New(0, 4)), d.cpuAllocationStore.GetSharedCPUs())
}

func TestDefragPassRetriesAnUnconfirmedRound(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	// A failed call says nothing about what was applied, so the claim must keep
	// holding both halves rather than release CPUs its container may now be on.
	d.updater.err = errors.New("connection reset")
	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1)
	origin, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.True(t, inFlight, "an unconfirmed move must stay in flight")
	require.Equal(t, cpuset.New(0, 4), origin)
	require.Equal(t, d.allCPUs.Difference(cpuset.New(0, 1, 4)), d.cpuAllocationStore.GetSharedCPUs(),
		"both halves stay out of the shared pool")

	// The next pass re-sends the same round rather than planning a new one.
	d.updater.err = nil
	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 2)
	require.Equal(t, calls[0], calls[1], "the retry must be the identical round")
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), cpus)
	_, inFlight = d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
}

func TestDefragPassDropsARoundWhoseStoresWereRebuilt(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.updater.err = errors.New("connection reset")
	d.defragPass(context.Background())
	require.NotNil(t, d.pendingRound)

	// A driver restart or an NRI reconnect rebuilds the stores from the specs on
	// disk, which already name the targets. The pending round belongs to a store
	// nothing reads any more.
	d.cpuAllocationStore = store.NewCPUAllocation(d.topology.cpuTopology, cpuset.New())
	d.placeClaim(t, "claim-1", cpuset.New(0, 1))
	d.updater.err = nil

	d.defragPass(context.Background())
	require.Nil(t, d.pendingRound, "the stale round must be dropped, not replayed")
	require.Len(t, d.updater.allCalls(), 1, "and nothing further sent for an already packed node")
}

func TestDefragPassMovesAClaimWithNoContainer(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	// Prepared but never started: there is nothing to update, and the store and
	// the spec are the whole of its state.
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls(), "no container, no round")
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), cpus)
	require.Equal(t, cpuset.New(0, 1), d.recordedPlacement(t, "claim-1"))
}

func TestDefragPassPinsAContainerToAllItsClaims(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	// One container, two claims, only one of them worth moving. The update has to
	// carry both or the container loses the CPUs of the claim that stayed.
	d.placeClaim(t, "claim-split", cpuset.New(0, 4))
	d.placeClaim(t, "claim-settled", cpuset.New(5, 6))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-split", "claim-settled")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1)
	split, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-split")
	settled, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-settled")
	require.Equal(t, cpuset.New(5, 6), settled, "a settled claim must not be disturbed")
	require.Equal(t, split.Union(settled).String(), updateFor(t, calls[0], "ctr-uid-1"))
}

func TestDefragPassNarrowsSharedContainersInTheSameRound(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "shared-ctr", "shared-uid")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1, "the moves and the shared containers go out together")
	// While the move is in flight the claim holds 0,1 and 4, so the shared
	// container is confined to what is left. That is what gets it off CPU 1
	// before the guaranteed container arrives there.
	require.Equal(t, "2-3,5-7", updateFor(t, calls[0], "shared-uid"))
	require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-1"))

	// Once committed the origin is back in the pool, so the shared container is
	// owed a wider mask and the worker is asked for one.
	require.Len(t, d.reconcileTrigger, 1)
}

func TestDefragPassHonoursTheClaimCooldown(t *testing.T) {
	d := newDefragTestDriver(t, 4, 4)
	d.defrag.claimCooldown = time.Hour
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 1)
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, cpus), "moved once")

	// Split it again behind the driver's back: without the cooldown this would be
	// moved straight back.
	require.NoError(t, d.cpuAllocationStore.BeginRebind(testr.New(t), "claim-1", cpuset.New(8, 12)))
	require.NoError(t, d.cpuAllocationStore.CommitRebind(testr.New(t), "claim-1"))

	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 1, "a claim just moved must be left to settle")

	d.lastMoved["claim-1"] = time.Now().Add(-2 * time.Hour)
	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 2, "and moved again once the cooldown has passed")
}

// cpuinfoSpread counts the uncore caches a cpuset touches.
func cpuinfoSpread(d *CPUDriver, cpus cpuset.CPUSet) int {
	caches := map[int]struct{}{}
	for _, cpu := range cpus.List() {
		caches[d.topology.cpuTopology.CPUDetails[cpu].UncoreCacheID] = struct{}{}
	}
	return len(caches)
}

func TestDefragWorkerWidensSharedContainersAfterAMove(t *testing.T) {
	// A move narrows the shared containers to get them off its targets. Nothing
	// widens them again on its own, so the worker has to, even where the
	// unprepare reconcile is switched off.
	d := newDefragTestDriver(t, 2, 4)
	d.reconcileSharedOnUnprepare = false
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "shared-ctr", "shared-uid")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		d.runReconcileWorker(ctx)
		close(done)
	}()

	d.requestReconcile()

	// The claim ends on one cache, and the shared container ends on everything
	// the claim no longer holds.
	require.Eventually(t, func() bool {
		for _, call := range d.updater.allCalls() {
			for _, update := range call {
				if update.GetContainerId() == "shared-uid" &&
					update.GetLinux().GetResources().GetCpu().GetCpus() == "2-7" {
					return true
				}
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "shared container was never widened onto the vacated CPUs")

	cancel()
	<-done
}

func TestDefragPassKeepsTheDynamicEnvWhenItMovesAClaim(t *testing.T) {
	// A move rewrites the spec to record the new placement. The environment edit
	// has to survive that untouched, or the claim would lose the only thing that
	// ties it to its container.
	d := newDefragTestDriver(t, 2, 4)
	logger := testr.New(t)
	claimUID := types.UID("claim-1")
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue)
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, cpuset.New(0, 4), false))
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID), envVar, cpuset.New(0, 4)))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", claimUID)

	d.defragPass(context.Background())

	require.Equal(t, cpuset.New(0, 1), d.recordedPlacement(t, claimUID))
	envs, err := d.cdiMgr.GetDeviceEnv(getCDIDeviceName(claimUID))
	require.NoError(t, err)
	require.Equal(t, []string{envVar}, envs)
}

func TestDefragPassRecordsWhatItSaw(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	// Before the pass: the claim spans two caches where one would do, and the
	// largest claim this node could still take unsplit is three CPUs.
	d.defragPass(context.Background())

	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_moves_total",
		map[string]string{"result": "success"}), 0.01)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_passes_total",
		map[string]string{"result": "success"}), 0.01)
	require.InDelta(t, 3, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "0"}), 0.01)

	// After it, the node reports itself packed, and the cache the claim left is
	// free whole.
	d.defragPass(context.Background())
	require.InDelta(t, 0, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
	require.InDelta(t, 4, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "0"}), 0.01)
	require.InDelta(t, 2, metricValue(t, d.metrics, "dra_cpu_defrag_passes_total",
		map[string]string{"result": "success"}), 0.01)
}

func TestDefragPassRecordsARefusedMoveAsAnError(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.updater.failed = []*api.ContainerUpdate{{ContainerId: "ctr-uid-1"}}

	d.defragPass(context.Background())

	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_moves_total",
		map[string]string{"result": "error"}), 0.01)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_passes_total",
		map[string]string{"result": "error"}), 0.01)
	require.InDelta(t, 0, metricValue(t, d.metrics, "dra_cpu_defrag_moves_total",
		map[string]string{"result": "success"}), 0.01)
}

func TestDefragPassRecordsBlockedMoves(t *testing.T) {
	// Two claims each sitting exactly where the other belongs, with no free CPU
	// to stage a swap through: a better placement exists and no move can reach it.
	d := newDefragTestDriver(t, 2, 2)
	d.placeClaim(t, "claim-1", cpuset.New(0, 3))
	d.placeClaim(t, "claim-2", cpuset.New(1, 2))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "ctr-2", "ctr-uid-2", "claim-2")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	require.InDelta(t, 2, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
	require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_blocked_moves_total", nil))
}

func TestDefragPassMeasuresANodeWithNoClaims(t *testing.T) {
	// An idle node has nothing to move but still has a shape. Reporting nothing
	// for it would make the leading indicator vanish exactly when it is most
	// favourable, and a gauge that disappears cannot be alerted on.
	d := newDefragTestDriver(t, 2, 4)

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	require.InDelta(t, 0, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
	require.InDelta(t, 4, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "0"}), 0.01,
		"an empty node can take a whole cache")
}

func TestDefragPassMovesAClaimWithTheRealCDIManager(t *testing.T) {
	// Against the real CDI manager, not the double. The manager's cache only
	// learns of a spec when it is refreshed, so anything in the move path that
	// reads a spec back can fail to find a claim this driver prepared itself --
	// which a double whose AddDevice updates a map in place can never show.
	logger := testr.New(t)
	d := newDefragTestDriver(t, 2, 4)
	realCDI, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)
	d.cdiMgr = realCDI

	claimUID := types.UID("claim-real-cdi")
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, cpuset.New(0, 4), false))
	// Exactly as Prepare writes it, with no Refresh afterwards.
	require.NoError(t, realCDI.AddDevice(logger, getCDIDeviceName(claimUID),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue), cpuset.New(0, 4)))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", claimUID)

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "the move never reached the runtime")
	moved, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), moved)

	// And the spec on disk agrees, since that is what a restart rebuilds from.
	require.NoError(t, realCDI.Refresh())
	recorded, err := realCDI.GetDeviceCPUSet(getCDIDeviceName(claimUID))
	require.NoError(t, err)
	require.Equal(t, cpuset.New(0, 1), recorded)
}

func TestDefragPassPrunesExpiredCooldowns(t *testing.T) {
	// Claim UIDs never repeat, so a cooldown entry that outlives its window is
	// pure growth: a node with claim churn and defragmentation on would
	// accumulate one forever for every claim ever moved.
	d := newDefragTestDriver(t, 2, 4)
	d.defrag.claimCooldown = time.Minute
	d.lastMoved["claim-expired"] = time.Now().Add(-2 * time.Minute)
	d.lastMoved["claim-cooling"] = time.Now()

	d.defragPass(context.Background())

	_, ok := d.lastMoved["claim-expired"]
	require.False(t, ok, "an entry past its cooldown must be pruned")
	_, ok = d.lastMoved["claim-cooling"]
	require.True(t, ok, "an entry still inside its cooldown must survive")

	// With no cooldown configured, nothing may accumulate at all.
	d.defrag.claimCooldown = 0
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.defragPass(context.Background())
	require.Contains(t, d.lastMoved, types.UID("claim-1"), "the commit records the move")
	d.defragPass(context.Background())
	require.Empty(t, d.lastMoved, "with no cooldown the record has no reader and must not linger")
}
