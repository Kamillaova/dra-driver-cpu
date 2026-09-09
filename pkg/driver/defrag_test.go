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
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	v1alpha1 "github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/cpuset"
)

// defragTestDriver is a driver on one NUMA node of caches x cpusPerCache CPUs,
// with defragmentation on.
type defragTestDriver struct {
	*CPUDriver
	updater *fakeContainerUpdater
	cdi     *mockCdiMgr
	metrics *prometheus.Registry
	allCPUs cpuset.CPUSet
	// cgroups is the kernel's answer about where each container really runs,
	// which only a test that fences a node has to write to.
	cgroups fstest.MapFS
}

func newDefragTestDriver(t *testing.T, caches, cpusPerCache int) *defragTestDriver {
	t.Helper()
	return newDefragTestDriverTopo(t, 1, caches, cpusPerCache)
}

// newDefragTestDriverTopo spreads the caches over numaNodes NUMA nodes.
func newDefragTestDriverTopo(t *testing.T, numaNodes, cachesPerNode, cpusPerCache int) *defragTestDriver {
	t.Helper()

	cpusPerNode := cachesPerNode * cpusPerCache
	var infos []cpuinfo.CPUInfo
	for cpu := range numaNodes * cpusPerNode {
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: cpu, SocketID: 0, NUMANodeID: cpu / cpusPerNode,
			UncoreCacheID: cpu / cpusPerCache,
		})
	}
	return newDefragTestDriverWith(t, infos)
}

// newDefragTestDriverWith builds the same driver over an explicit topology, for
// a test whose point is a shape the two constructors above cannot describe.
func newDefragTestDriverWith(t *testing.T, infos []cpuinfo.CPUInfo) *defragTestDriver {
	t.Helper()
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)

	allCPUs := topo.CPUDetails.CPUs()
	updater := &fakeContainerUpdater{}
	cdi := newMockCdiMgr()
	reg := prometheus.NewRegistry()
	cgroups := fstest.MapFS{}
	d := &CPUDriver{
		metrics:            cpumetrics.New(reg),
		topology:           deviceTopology{cpuTopology: topo, reservedCPUs: cpuset.New(), onlineCPUs: allCPUs},
		cpuAllocationStore: store.NewCPUAllocation(topo, cpuset.New()),
		podConfigStore:     store.NewPodConfig(),
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             cdi,
		containerUpdater:   updater,
		reconcileTrigger:   make(chan struct{}, 1),
		cgroupfs:           cgroups,
		poisonedNodes:      make(map[int]*poisonedNode),
		pendingRounds:      make(map[defragScope]*defragRound),
		defragRetries:      workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[defragScope]()),
		defragRetryDue:     make(chan defragScope),
		sysfs: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte(allCPUs.String() + "\n")},
		},
		defrag: defragOptions{enabled: true, allowTransientOverlap: true, batchTimeout: defaultDefragBatchTimeout},
	}
	t.Cleanup(d.defragRetries.ShutDown)
	return &defragTestDriver{CPUDriver: d, updater: updater, cdi: cdi, metrics: reg, allCPUs: allCPUs, cgroups: cgroups}
}

// describe resolves the driver's cores into the given partitions plus the
// implicit remainder, as New does, so that a test can name the region a round
// covers. Exclusive partitions only: a reserved or shared one would also have to
// be folded into the reservation the allocation store was built with.
func (d *defragTestDriver) describe(t *testing.T, partitions ...devattr.Partition) {
	t.Helper()
	for _, partition := range partitions {
		require.True(t, partition.PublishesExclusiveDevices(),
			"describe takes exclusive partitions; %q has role %q", partition.Name, partition.Role)
	}
	d.partitions = devattr.WithImplicitDefault(partitions, d.allCPUs)
	d.defaultPartitionCPUs = cpuset.New()
	for _, partition := range d.partitions {
		if partition.Role == devattr.PARTITION_ROLE_DEFAULT {
			d.defaultPartitionCPUs = d.defaultPartitionCPUs.Union(partition.CPUs)
		}
	}
}

// defaultScope is the region a pass covers on a node whose cores nobody
// described: the whole of one NUMA node.
func defaultScope(numaNodeID int) defragScope {
	return defragScope{numaNodeID: numaNodeID, partition: devattr.DefaultPartitionName}
}

// smtCacheInfos is one NUMA node of four SMT2 cores, core c being CPUs
// {c, c+4}: cores 0-1 share cache 0 and cores 2-3 cache 1.
func smtCacheInfos() []cpuinfo.CPUInfo {
	var infos []cpuinfo.CPUInfo
	for cpu := range 8 {
		core := cpu % 4
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: core, SocketID: 0, NUMANodeID: 0,
			UncoreCacheID: core / 2, SiblingCPUID: (cpu + 4) % 8,
			SiblingCPUSet: cpuset.New(core, core+4),
		})
	}
	return infos
}

// errUpdateFailed stands in for a runtime that could not be reached.
var errUpdateFailed = errors.New("connection reset")

// setErr changes what the runtime will answer while the worker may already be
// calling it, which the field cannot be assigned directly for.
func (f *fakeContainerUpdater) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// placeClaim records a prepared claim on the given CPUs, as Prepare would. The
// claim permits moves, since that is what these tests are about; placeFixedClaim
// is the claim that does not.
func (d *defragTestDriver) placeClaim(t *testing.T, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	d.place(t, claimUID, relocatableOn(cpus))
}

// placeFixedClaim records a claim whose configuration says nothing, so it never
// permits its CPUs to change.
func (d *defragTestDriver) placeFixedClaim(t *testing.T, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	d.place(t, claimUID, exclusiveOn(cpus))
}

func (d *defragTestDriver) placeRepairableClaim(t *testing.T, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	rec := relocatableOn(cpus)
	rec.Alignment = v1alpha1.AlignmentRepairable
	d.place(t, claimUID, rec)
}

func (d *defragTestDriver) place(t *testing.T, claimUID types.UID, record store.ClaimRecord) {
	t.Helper()
	logger := testr.New(t)
	cpus := store.UnionOf(record.Requests)
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, record, false))
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cpus.String()), record))
}

// runContainer binds claims to a running container, as CreateContainer would.
func (d *defragTestDriver) runContainer(t *testing.T, podUID types.UID, name string, containerUID types.UID, claimUIDs ...types.UID) {
	t.Helper()
	if len(claimUIDs) > 0 {
		_, err := d.claimTracker.SetOwner(testr.New(t), podUID, name, claimUIDs...)
		require.NoError(t, err)
	}
	d.podConfigStore.SetContainerState(podUID, store.NewContainerState(name, containerUID, claimUIDs...).WithCgroup(cgroupOf(containerUID)))
}

// cgroupOf is where the fake runtime puts a container, in the form the cgroupfs
// driver reports.
func cgroupOf(containerUID types.UID) string {
	return "/kubepods/" + string(containerUID)
}

// liveCPUs is what the kernel says a container is really confined to, which is
// what lifts a fence.
func (d *defragTestDriver) liveCPUs(containerUID types.UID, cpus cpuset.CPUSet) {
	dir, err := cgroupfs.Dir(cgroupOf(containerUID))
	if err != nil {
		panic(err)
	}
	d.cgroups[dir+"/cpuset.cpus.effective"] = &fstest.MapFile{Data: []byte(cpus.String() + "\n")}
}

// recordedPlacement is the cpuset a claim's CDI spec names.
func (d *defragTestDriver) recordedPlacement(t *testing.T, claimUID types.UID) cpuset.CPUSet {
	t.Helper()
	record, err := d.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claimUID))
	require.NoError(t, err)
	return store.UnionOf(record.Requests)
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
	d.updater.err = errUpdateFailed
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

	d.updater.err = errUpdateFailed
	d.defragPass(context.Background())
	require.Contains(t, d.pendingRounds, defaultScope(0))

	// A driver restart or an NRI reconnect rebuilds the stores from the specs on
	// disk, which already name the targets. The pending round belongs to a store
	// nothing reads any more.
	d.cpuAllocationStore = store.NewCPUAllocation(d.topology.cpuTopology, cpuset.New())
	d.placeClaim(t, "claim-1", cpuset.New(0, 1))
	d.updater.err = nil

	d.defragPass(context.Background())
	require.NotContains(t, d.pendingRounds, defaultScope(0), "the stale round must be dropped, not replayed")
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

func TestDefragPassMovesAClaimAsOftenAsItIsSplit(t *testing.T) {
	// Nothing makes a claim wait between moves: a claim split again is repaired
	// again, since the pass that would be delayed is the one a workload is
	// waiting on.
	d := newDefragTestDriver(t, 4, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 1)
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, cpus), "moved once")

	// Split it again behind the driver's back.
	require.NoError(t, d.cpuAllocationStore.BeginRebind(testr.New(t), "claim-1", cpuset.New(8, 12)))
	require.NoError(t, d.cpuAllocationStore.CommitRebind(testr.New(t), "claim-1"))

	d.defragPass(context.Background())
	require.Len(t, d.updater.allCalls(), 2, "the next pass repairs it again")
	cpus, _ = d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, cpus))
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
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, relocatableOn(cpuset.New(0, 4)), false))
	require.NoError(t, d.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID), envVar, relocatableOn(cpuset.New(0, 4))))
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
	// to move either through and the exchange that would fix it forbidden: a
	// better placement exists and nothing can reach it.
	d := newDefragTestDriver(t, 2, 2)
	d.defrag.allowTransientOverlap = false
	d.placeClaim(t, "claim-1", cpuset.New(0, 3))
	d.placeClaim(t, "claim-2", cpuset.New(1, 2))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "ctr-2", "ctr-uid-2", "claim-2")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	require.InDelta(t, 2, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
	require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_blocked_moves_total", nil))
}

func TestDefragPassNeverTargetsOfflineCPUs(t *testing.T) {
	// The whole of cache 0 fits this claim, but half of that cache went offline
	// after startup. The pass must re-read the online set and place around the
	// hole: a cpuset naming an offline CPU is rejected by the kernel outright.
	d := newDefragTestDriver(t, 2, 4)
	d.sysfs = fstest.MapFS{
		"devices/system/cpu/online": &fstest.MapFile{Data: []byte("0-1,4-7\n")},
	}
	d.placeClaim(t, "claim-1", cpuset.New(0, 1, 4, 5))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(4, 5, 6, 7), cpus,
		"the only whole cache the claim fits in without the offline CPUs")
	require.True(t, cpus.Intersection(cpuset.New(2, 3)).IsEmpty(), "moved onto offline CPUs")
	require.Equal(t, cpuset.New(4, 5, 6, 7), d.recordedPlacement(t, "claim-1"))
}

func TestDefragPassCommitsAMoveWhoseContainerWasReplaced(t *testing.T) {
	// The container restarted while the round was at the runtime: the update for
	// the old container failed, but its replacement was pinned from the store,
	// which already holds the target. That is convergence, not refusal -- an
	// abort here would put the claim where the new container is not.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.updater.failed = []*api.ContainerUpdate{{ContainerId: "ctr-uid-1"}}
	d.updater.onUpdate = func([]*api.ContainerUpdate) {
		d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-2", "claim-1")
	}

	d.defragPass(context.Background())

	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), cpus, "the move must commit")
	require.Equal(t, cpuset.New(0, 1), d.recordedPlacement(t, "claim-1"))
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
}

func TestDefragPassCommitsAMoveWhoseContainerIsGone(t *testing.T) {
	// The container stopped while the round was at the runtime. Nothing runs on
	// either half, and whatever starts next is pinned from the store, so the
	// recorded target is the right place to end up.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.updater.failed = []*api.ContainerUpdate{{ContainerId: "ctr-uid-1"}}
	d.updater.onUpdate = func([]*api.ContainerUpdate) {
		d.podConfigStore.RemoveContainerState("pod-1", "ctr-1", "ctr-uid-1")
	}

	d.defragPass(context.Background())

	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), cpus)
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
}

func TestDefragPassAbortsAMoveItCannotRecord(t *testing.T) {
	// The spec on disk is what a restart rebuilds from, so a move that cannot be
	// recorded must not happen: undo the reservation and tell the runtime
	// nothing.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.cdi.addError = errors.New("no space left on device")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls(), "an unrecorded move must not reach the runtime")
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 4), cpus)
	require.Equal(t, cpuset.New(0, 4), d.recordedPlacement(t, "claim-1"))
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
	require.Equal(t, d.allCPUs.Difference(cpuset.New(0, 4)), d.cpuAllocationStore.GetSharedCPUs())

	// The failure was transient, so the next pass completes the move.
	d.cdi.addError = nil
	d.defragPass(context.Background())
	cpus, _ = d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), cpus)
}

func TestDefragPassAbandonsARoundItCannotBuildUpdatesFor(t *testing.T) {
	// The container claims something the store has no placement for, which means
	// this driver's view of it is inconsistent -- an unprepare raced, or worse.
	// Pinning it to a partial union would take CPUs away from a running
	// workload, so the whole round is undone instead: reservations, specs, and
	// nothing sent.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1", "claim-ghost")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 4), cpus)
	require.Equal(t, cpuset.New(0, 4), d.recordedPlacement(t, "claim-1"))
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-1")
	require.False(t, inFlight)
}

func TestDefragPassSendsARoundPerNUMANode(t *testing.T) {
	// A round is one NUMA node's worth of moves, which is what bounds how much
	// of a machine one batch disturbs. Two nodes each needing a move are two
	// rounds in one pass, not one batch spanning both.
	d := newDefragTestDriverTopo(t, 2, 2, 4)
	d.placeClaim(t, "claim-n0", cpuset.New(0, 4))
	d.runContainer(t, "pod-0", "ctr-0", "ctr-uid-0", "claim-n0")
	d.placeClaim(t, "claim-n1", cpuset.New(8, 12))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-n1")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 2, "one round per node")
	require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-0"))
	require.Equal(t, "8-9", updateFor(t, calls[1], "ctr-uid-1"))
	for _, claimUID := range []types.UID{"claim-n0", "claim-n1"} {
		cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
		require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, cpus), "claim %s is still split", claimUID)
	}
}

func TestDefragPassKeepsGoingPastANodeItCannotSettle(t *testing.T) {
	// One round in flight per node means exactly that: a node whose round the
	// runtime never confirmed holds up its own next round and nothing else.
	d := newDefragTestDriverTopo(t, 2, 2, 4)
	d.placeClaim(t, "claim-n0", cpuset.New(0, 4))
	d.runContainer(t, "pod-0", "ctr-0", "ctr-uid-0", "claim-n0")

	d.updater.err = errUpdateFailed
	d.defragPass(context.Background())
	require.Contains(t, d.pendingRounds, defaultScope(0), "node 0's round is unconfirmed")

	// Node 1 only now needs a move, and node 0 is still stuck.
	d.updater.err = nil
	d.placeClaim(t, "claim-n1", cpuset.New(8, 12))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-n1")

	d.defragPass(context.Background())

	moved, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-n1")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, moved), "node 1 was held up by node 0")
	require.NotContains(t, d.pendingRounds, defaultScope(1))
	require.NotContains(t, d.pendingRounds, defaultScope(0), "node 0's own round was re-sent and settled")
}

func TestDefragPassMovesWholeCores(t *testing.T) {
	// SMT topology: core c is CPUs {c, c+4}; cores 0-1 share cache 0, cores 2-3
	// cache 1. With whole-core allocation on, a move may never split a core, and
	// only whole free cores count as free. The step it must respect is derived
	// from the scope's own cores, so this also pins that derivation end to end.
	d := newDefragTestDriverWith(t, smtCacheInfos())
	d.fullPhysicalCPUsOnly = true
	topo := d.topology.cpuTopology
	// Cores 0 and 2, one per cache; cores 1 and 3 are free.
	d.placeClaim(t, "claim-1", cpuset.New(0, 4, 2, 6))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpus, topo.CPUDetails.CompleteCores(cpus), "a move split a physical core")
	require.Equal(t, 1, cpuinfoSpread(d.CPUDriver, cpus), "the claim must end inside one cache")
	require.Equal(t, 4, cpus.Size())
}

func TestReconcileWorkerRetriesANodeTheRuntimeLeftUnsettled(t *testing.T) {
	// Nothing else will look at a node whose round failed until a claim arrives
	// or leaves, so the round arms its own next attempt through the queue.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		d.runReconcileWorker(ctx)
		close(done)
	}()

	// The first attempt fails at the runtime and is never confirmed; the retry
	// alone has to carry the claim home.
	d.updater.setErr(errUpdateFailed)
	d.requestReconcile()
	require.Eventually(t, func() bool {
		return len(d.updater.allCalls()) > 0
	}, 2*time.Second, 5*time.Millisecond, "the first round never reached the runtime")
	d.updater.setErr(nil)

	require.Eventually(t, func() bool {
		cpus, ok := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
		return ok && cpuinfoSpread(d.CPUDriver, cpus) == 1
	}, 2*time.Second, 5*time.Millisecond, "the unsettled round was never retried")

	cancel()
	<-done
}

func TestDefragNodeForgetsANodesBackoffOnceItSettles(t *testing.T) {
	// The rate limiter counts a node's failures, so a node that settles has to
	// start its next failure from the shortest delay rather than from wherever
	// an old crash loop left it.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.updater.err = errUpdateFailed
	d.defragPass(context.Background())
	require.Positive(t, d.defragRetries.NumRequeues(defaultScope(0)), "an unconfirmed round must arm the retry")

	d.updater.err = nil
	d.defragPass(context.Background())
	require.Zero(t, d.defragRetries.NumRequeues(defaultScope(0)), "a settled round must clear the backoff")
}

func TestDefragPassReleasesApplyMuDuringTheRuntimeCall(t *testing.T) {
	// The rule the pass's whole shape exists for: applyMu must not be held
	// across the call into the runtime, because the runtime may be holding, on
	// behalf of an inbound hook of ours, the lock our call needs. A fake updater
	// cannot deadlock, so assert the contract directly from inside the call.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	var lockWasFree atomic.Bool
	d.updater.onUpdate = func([]*api.ContainerUpdate) {
		if d.applyMu.TryLock() {
			d.applyMu.Unlock()
			lockWasFree.Store(true)
		}
	}

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "the move must actually reach the runtime")
	require.True(t, lockWasFree.Load(), "applyMu was held across the runtime call")
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

func TestDefragPassMeasuresEveryNUMANodeNotOnlyOccupiedOnes(t *testing.T) {
	// One claim on node 0 must not stop node 1 from being reported.
	d := newDefragTestDriverTopo(t, 2, 2, 4)
	// Two CPUs taken out of each of node 0's caches, so the largest aligned claim
	// it could still take is two; node 1 is untouched and could take a whole cache.
	d.placeClaim(t, "claim-1", cpuset.New(0, 1, 4, 5))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	require.InDelta(t, 2, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "0"}), 0.01, "node 0's emptiest cache has two CPUs left")
	require.InDelta(t, 4, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "1"}), 0.01, "node 1 holds no claims and must still be reported")
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
	require.NoError(t, d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, relocatableOn(cpuset.New(0, 4)), false))
	// Exactly as Prepare writes it, with no Refresh afterwards.
	require.NoError(t, realCDI.AddDevice(logger, getCDIDeviceName(claimUID),
		fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cdiEnvDynamicValue), relocatableOn(cpuset.New(0, 4))))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", claimUID)

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "the move never reached the runtime")
	moved, ok := d.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), moved)

	// And the spec on disk agrees, since that is what a restart rebuilds from.
	require.NoError(t, realCDI.Refresh())
	recorded, err := realCDI.GetDeviceAllocations(getCDIDeviceName(claimUID))
	require.NoError(t, err)
	require.Equal(t, relocatableOn(cpuset.New(0, 1)), recorded)
}

func TestDefragPassAsksForTheNextPassWhenItCommitsAnything(t *testing.T) {
	// A target this round vacates is not free until the round commits, so the
	// moves it made possible belong to the next pass. Without this a chain of
	// moves would stop at whatever the first round could reach.
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1)
	require.Len(t, d.reconcileTrigger, 1, "a committed round must ask for the next pass")
}

func TestDefragPassLeavesAClaimThatNeverAskedToBeMoved(t *testing.T) {
	// A move changes the CPUs under a running workload, and only that workload
	// knows whether it survives one, so a claim whose configuration says nothing
	// is an obstacle rather than a candidate: the node keeps the spread it
	// cannot repair, and reports it.
	d := newDefragTestDriver(t, 2, 4)
	d.placeFixedClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls(), "an immobile claim must not be moved")
	cpus, ok := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 4), cpus)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
}

func TestDefragPassStillMovesAClaimAfterARestart(t *testing.T) {
	// Mobility is the claim's, and the driver does not watch ResourceClaims: a
	// restart rebuilds what it knows from the specs on disk. Recorded nowhere,
	// every claim would read as immobile afterwards and defragmentation would
	// stop for the life of those pods -- silently, since refusing to move is
	// also the correct answer for a claim that never asked.
	logger := testr.New(t)
	claimUID := types.UID("claim-across-restart")

	before := newDefragTestDriver(t, 2, 4)
	realCDI, err := NewCdiManager(logger, testDriverName, t.TempDir())
	require.NoError(t, err)
	before.cdiMgr = realCDI
	before.place(t, claimUID, relocatableOn(cpuset.New(0, 4)))

	after := newDefragTestDriver(t, 2, 4)
	after.cdiMgr = realCDI
	after.seedAllocationStoreFromDisk(logger)

	require.True(t, after.cpuAllocationStore.IsRelocatable(claimUID),
		"the claim's own answer did not survive the restart")
	after.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", claimUID)
	after.defragPass(context.Background())

	moved, ok := after.cpuAllocationStore.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), moved, "the claim was not consolidated after the restart")
}

// mixedSMTInfos is one NUMA node whose two caches disagree about thread arity:
// cache 0 holds two SMT2 cores (CPUs 0-3), cache 1 two cores whose siblings the
// platform took offline and which the kernel therefore reports as one thread
// each (CPUs 4 and 5).
func mixedSMTInfos() []cpuinfo.CPUInfo {
	var infos []cpuinfo.CPUInfo
	for _, cpu := range []int{0, 1, 2, 3} {
		core := cpu % 2
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: core, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0,
			SiblingCPUID: (cpu + 2) % 4, SiblingCPUSet: cpuset.New(core, core+2),
		})
	}
	for _, cpu := range []int{4, 5} {
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID: cpu, CoreID: cpu - 2, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 1,
			SiblingCPUID: -1, SiblingCPUSet: cpuset.New(cpu),
		})
	}
	return infos
}

// partitionedDefragDriver is four caches of four CPUs on one NUMA node, with
// caches 0 and 1 (CPUs 0-7) declared as a dataplane partition and caches 2 and 3
// (CPUs 8-15) left as the implicit default one.
func partitionedDefragDriver(t *testing.T) *defragTestDriver {
	t.Helper()
	d := newDefragTestDriverTopo(t, 1, 4, 4)
	d.describe(t, devattr.Partition{
		Name: "dataplane",
		Role: devattr.PARTITION_ROLE_EXCLUSIVE,
		CPUs: cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
	})
	return d
}

func TestDefragPassNeverMovesAClaimOutOfItsPartition(t *testing.T) {
	// A repair may only use the CPUs of the partition the claim already sits in,
	// so a pass never mixes the dataplane's cores with the virtual machines'.
	//
	// The claim below is split across both caches of its own partition and has
	// nowhere left to go inside it, while the whole of the other partition is
	// free. The second half of the test is what proves the partition is doing the
	// work rather than the arithmetic happening to agree: the same node with its
	// cores undescribed moves the same claim onto the very CPUs the dataplane
	// would have held.
	place := func(d *defragTestDriver) {
		d.placeClaim(t, "claim-vm", cpuset.New(8, 12))
		d.placeFixedClaim(t, "claim-a", cpuset.New(9, 10, 11))
		d.placeFixedClaim(t, "claim-b", cpuset.New(13, 14, 15))
		d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-vm")
	}

	described := partitionedDefragDriver(t)
	place(described)

	described.defragPass(context.Background())

	require.Empty(t, described.updater.allCalls(),
		"the only repair on offer crosses a partition, so there is no repair")
	cpus, ok := described.cpuAllocationStore.GetResourceClaimAllocation("claim-vm")
	require.True(t, ok)
	require.Equal(t, cpuset.New(8, 12), cpus, "the claim left its partition")

	undescribed := newDefragTestDriverTopo(t, 1, 4, 4)
	place(undescribed)

	undescribed.defragPass(context.Background())

	moved, ok := undescribed.cpuAllocationStore.GetResourceClaimAllocation("claim-vm")
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 1), moved,
		"with no partitions the planner takes the free cache, which is what confinement has to prevent")
}

func TestDefragPassRunsARoundPerPartition(t *testing.T) {
	// One topology per NUMA node and partition means one round each: two claims
	// on one NUMA node, one per partition, are two batches, and each is repaired
	// inside the partition that granted it.
	d := partitionedDefragDriver(t)
	d.placeClaim(t, "claim-dp", cpuset.New(0, 4))
	d.runContainer(t, "pod-dp", "ctr-dp", "ctr-uid-dp", "claim-dp")
	d.placeClaim(t, "claim-vm", cpuset.New(8, 12))
	d.runContainer(t, "pod-vm", "ctr-vm", "ctr-uid-vm", "claim-vm")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 2, "one round per partition, not one spanning both")
	require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-dp"), "the declared partition is planned first")
	require.Equal(t, "8-9", updateFor(t, calls[1], "ctr-uid-vm"))

	dataplane, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-dp")
	require.Equal(t, cpuset.New(0, 1), dataplane)
	vm, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-vm")
	require.Equal(t, cpuset.New(8, 9), vm)
}

func TestDefragPassSkipsAPartitionWithNothingMobile(t *testing.T) {
	// A partition whose claims all decline to move has nothing to plan, and
	// nothing had to tell the planner so: the better placement is built around
	// such a claim, so the partition reports itself as well packed as its claims
	// allow. It is still measured, which is how an operator sees the spread that
	// will not be repaired.
	d := partitionedDefragDriver(t)
	d.placeFixedClaim(t, "claim-spdk", cpuset.New(0, 4))
	d.runContainer(t, "pod-dp", "ctr-dp", "ctr-uid-dp", "claim-spdk")
	d.placeClaim(t, "claim-vm", cpuset.New(8, 12))
	d.runContainer(t, "pod-vm", "ctr-vm", "ctr-uid-vm", "claim-vm")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1, "only the partition holding a mobile claim runs a round")
	require.Equal(t, "8-9", updateFor(t, calls[0], "ctr-uid-vm"))
	spdk, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-spdk")
	require.Equal(t, cpuset.New(0, 4), spdk, "an immobile claim must not be moved")
	require.InDelta(t, 2, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01,
		"both partitions were measured before the pass, and both were split")

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "and nothing further is attempted")
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01,
		"what is left is the spread the immobile claim keeps")
}

func TestDefragPassLeavesACPUInThePoolOnlyWhereThePoolIs(t *testing.T) {
	// While a move is in flight its claim holds both its old and its new CPUs, so
	// a round has to leave one CPU behind for the containers holding no claim.
	// Those run on the default partition alone, so that is the only partition
	// where the rule bites: elsewhere none of the CPUs a round takes were ever
	// theirs to run on, and holding one back would refuse a repair for nothing.
	//
	// Both halves use the same shape: a three-CPU partition, a claim split across
	// its two caches, and exactly one free CPU, which the repair needs.
	split := func(d *defragTestDriver) {
		d.placeClaim(t, "claim-1", cpuset.New(0, 2))
		d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
		d.runContainer(t, "pod-2", "shared-ctr", "shared-uid")
	}

	t.Run("an exclusive partition may take its last free CPU", func(t *testing.T) {
		d := newDefragTestDriverTopo(t, 1, 2, 2)
		// CPU 3 is the whole of the implicit default partition, so the claimless
		// container runs there and nothing the vm partition does can empty it.
		d.describe(t, devattr.Partition{
			Name: "vm", Role: devattr.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 1, 2),
		})
		split(d)

		d.defragPass(context.Background())

		calls := d.updater.allCalls()
		require.Len(t, calls, 1)
		require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-1"))
		require.Equal(t, "3", updateFor(t, calls[0], "shared-uid"),
			"the pool is untouched, which is why the round was free to take everything else")
	})

	t.Run("the default partition may not", func(t *testing.T) {
		d := newDefragTestDriverTopo(t, 1, 2, 2)
		// The same three CPUs, now the default partition: the claimless container
		// runs on them, so the last free CPU is the pool.
		d.describe(t, devattr.Partition{
			Name: "other", Role: devattr.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(3),
		})
		split(d)

		d.defragPass(context.Background())

		require.Empty(t, d.updater.allCalls(), "the repair would have left the pool empty")
		cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
		require.Equal(t, cpuset.New(0, 2), cpus)
		// Held back while planning, which is the whole of the difference: a round
		// built and then abandoned for want of a pool would leave this at zero and
		// an error in the log.
		require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_blocked_moves_total", nil))
	})
}

func TestDefragScopeThreadsPerCoreIsThePartitionsOwn(t *testing.T) {
	// A NUMA node holding an SMT partition beside one whose siblings are offline
	// has no single whole-core step, and neither partition may be handed the
	// other's: each scope is asked for its own, computed from its own cores.
	d := newDefragTestDriverWith(t, mixedSMTInfos())
	d.fullPhysicalCPUsOnly = true
	d.describe(t,
		devattr.Partition{Name: "vm", Role: devattr.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 1, 2, 3)},
		devattr.Partition{Name: "dataplane", Role: devattr.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(4, 5)},
	)
	logger := testr.New(t)
	online, ok := d.defragOnlineCPUs(logger)
	require.True(t, ok)

	vm, ok := d.defragView(logger, defragScope{numaNodeID: 0, partition: "vm"}, online)
	require.True(t, ok)
	require.Equal(t, 2, vm.threadsPerCore, "the SMT partition keeps its whole-core step")

	dataplane, ok := d.defragView(logger, defragScope{numaNodeID: 0, partition: "dataplane"}, online)
	require.True(t, ok)
	require.Equal(t, 0, dataplane.threadsPerCore,
		"a core with one thread has nothing to keep together, so there is no step to respect")

	// Whether the promise applies at all is still the operator's option.
	d.fullPhysicalCPUsOnly = false
	vm, ok = d.defragView(logger, defragScope{numaNodeID: 0, partition: "vm"}, online)
	require.True(t, ok)
	require.Zero(t, vm.threadsPerCore)
}

func TestDefragPassReportsSpreadNoPartitionCanRepair(t *testing.T) {
	// A claim whose CPUs straddle two partitions is what a partition list edited
	// under a running node looks like. No pass can repair it -- a rebind replaces
	// a claim's whole exclusive set, so it belongs to no region -- but its spread
	// is real, and the guide promises that spread a pass cannot repair is reported
	// rather than hidden. A claim that never asked to move is reported on exactly
	// that ground, so this one has to be too.
	d := partitionedDefragDriver(t)
	// CPU 0 is the dataplane's cache 0, CPU 8 the default partition's cache 2.
	d.placeClaim(t, "claim-straddling", cpuset.New(0, 8))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-straddling")

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls(), "no region holds it, so no region can move it")
	cpus, ok := d.cpuAllocationStore.GetResourceClaimAllocation("claim-straddling")
	require.True(t, ok)
	require.Equal(t, cpuset.New(0, 8), cpus)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01,
		"the node spans two caches where one would do, whoever can repair it")
}

func TestDefragPassMeasuresANodeWithNoPlannablePartition(t *testing.T) {
	// Every partition of this node is one the machine contradicts, so it publishes
	// no device and a pass has nowhere to plan. The node still has a shape, and a
	// gauge that vanishes cannot be alerted on -- least of all in the state where
	// an operator most needs to see it.
	d := newDefragTestDriverTopo(t, 1, 2, 4)
	d.describe(t, devattr.Partition{
		Name: "dataplane", Role: devattr.PARTITION_ROLE_EXCLUSIVE, CPUs: d.allCPUs,
	})
	d.degradedPartitions = map[string]string{
		"dataplane": `partition "dataplane" expects at most 1 online thread(s) per core`,
	}

	d.defragPass(context.Background())

	require.Empty(t, d.updater.allCalls())
	require.InDelta(t, 0, metricValue(t, d.metrics, "dra_cpu_defrag_largest_alignable_free_cpus",
		map[string]string{"numa_node": "0"}), 0.01,
		"no partition can take a claim, and the series has to say so rather than disappear")
	require.InDelta(t, 0, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil), 0.01)
}

// exchangeDriver is the node an exchange exists for: two caches of two CPUs,
// entirely held by two claims that each sit exactly where the other belongs.
// There is no free CPU to move either through.
func exchangeDriver(t *testing.T) *defragTestDriver {
	t.Helper()
	d := newDefragTestDriver(t, 2, 2)
	d.placeClaim(t, "claim-1", cpuset.New(0, 3))
	d.placeClaim(t, "claim-2", cpuset.New(1, 2))
	d.runContainer(t, "pod-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-2", "ctr-2", "ctr-uid-2", "claim-2")
	return d
}

func TestDefragPassExchangesTwoClaimsInOneBatch(t *testing.T) {
	d := exchangeDriver(t)

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 1, "both containers are updated in one batch")
	require.Equal(t, "0-1", updateFor(t, calls[0], "ctr-uid-1"))
	require.Equal(t, "2-3", updateFor(t, calls[0], "ctr-uid-2"))

	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	second, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-2")
	require.Equal(t, cpuset.New(0, 1), first)
	require.Equal(t, cpuset.New(2, 3), second)
	require.Equal(t, cpuset.New(0, 1), d.recordedPlacement(t, "claim-1"))
	require.Equal(t, cpuset.New(2, 3), d.recordedPlacement(t, "claim-2"))
	for _, claimUID := range []types.UID{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(claimUID)
		require.False(t, inFlight, "claim %s must be settled, not left half-swapped", claimUID)
	}
	require.True(t, d.cpuAllocationStore.GetSharedCPUs().IsEmpty(), "an exchange releases nothing")
	require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_swap_overlap_seconds", nil))
}

func TestDefragPassRetriesTheRefusedHalfOfAnExchange(t *testing.T) {
	// The runtime moves one container and refuses the other, which is the state
	// that leaves two claims sharing CPUs. Completing the exchange is better than
	// undoing it, so the refused half goes out again first.
	d := exchangeDriver(t)
	d.updater.reply = func(call int, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		if call == 1 {
			return refusing("ctr-uid-2")(call, updates)
		}
		return nil, nil
	}

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 2, "the batch, then the half it refused")
	require.Len(t, calls[1], 1)
	require.Equal(t, "2-3", updateFor(t, calls[1], "ctr-uid-2"))

	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), first, "the exchange completed")
	_, inFlight := d.cpuAllocationStore.GetRebindOrigin("claim-2")
	require.False(t, inFlight)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_partial_batches_total", nil), 0.01)
	require.Empty(t, d.pendingRounds)
}

func TestDefragPassRollsBackTheAppliedHalfOfAnExchange(t *testing.T) {
	// The runtime keeps refusing one container, so the one it did move is put
	// back: two claims sharing CPUs for good is the outcome to avoid.
	d := exchangeDriver(t)
	d.updater.reply = refusing("ctr-uid-2")

	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, 3, "the batch, the refused half again, then the rollback")
	require.Equal(t, "0,3", updateFor(t, calls[2], "ctr-uid-1"), "the applied half goes back where it was")

	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	second, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-2")
	require.Equal(t, cpuset.New(0, 3), first)
	require.Equal(t, cpuset.New(1, 2), second)
	require.Equal(t, cpuset.New(0, 3), d.recordedPlacement(t, "claim-1"), "and so does the spec on disk")
	for _, claimUID := range []types.UID{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(claimUID)
		require.False(t, inFlight)
	}
	require.Empty(t, d.pendingRounds, "an undone exchange is settled; the next pass plans afresh")
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_rollbacks_total", map[string]string{"result": "success"}), 0.01)
}

func TestDefragPassKeepsAnUnsettleableExchangeInTransit(t *testing.T) {
	// The runtime refuses one container and then refuses to put the other back.
	// Nobody can say which CPUs the two containers are on, so both claims keep
	// holding both cpusets and nothing else may be given them.
	d := exchangeDriver(t)
	d.updater.reply = func(call int, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		if call < 3 {
			return refusing("ctr-uid-2")(call, updates)
		}
		return refusing("ctr-uid-1")(call, updates)
	}

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 3)
	for _, claimUID := range []types.UID{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(claimUID)
		require.True(t, inFlight, "claim %s must still hold both cpusets", claimUID)
	}
	require.True(t, d.cpuAllocationStore.GetSharedCPUs().IsEmpty())
	require.Contains(t, d.pendingRounds, defaultScope(0), "the round is sent again rather than settled on a guess")
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_rollbacks_total", map[string]string{"result": "error"}), 0.01)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_passes_total", map[string]string{"result": "error"}), 0.01)
}

func TestDefragPassUndoesAnExchangeTheRuntimeRefusedOutright(t *testing.T) {
	// Neither container moved, so there is nothing to put back and no third
	// batch: the claims go straight back to where they already are.
	d := exchangeDriver(t)
	d.updater.reply = refusing("ctr-uid-1", "ctr-uid-2")

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "nothing was applied, so nothing is undone")
	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 3), first)
	require.Equal(t, cpuset.New(0, 3), d.recordedPlacement(t, "claim-1"))
	require.Empty(t, d.pendingRounds)
	require.Zero(t, metricValue(t, d.metrics, "dra_cpu_defrag_rollbacks_total", map[string]string{"result": "success"}))
}

func TestDefragPassTreatsAHangingBatchAsUnsettled(t *testing.T) {
	// A runtime that never answers must not hold the one goroutine that runs
	// passes: the batch has a deadline of its own, and running out of it is an
	// unsettled round, since the call may still be applied.
	d := exchangeDriver(t)
	d.defrag.batchTimeout = 20 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	d.updater.onUpdate = func([]*api.ContainerUpdate) { <-release }

	done := make(chan struct{})
	go func() {
		d.defragPass(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass waited on the runtime instead of its own deadline")
	}

	for _, claimUID := range []types.UID{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(claimUID)
		require.True(t, inFlight, "claim %s must still hold both cpusets", claimUID)
	}
	require.Contains(t, d.pendingRounds, defaultScope(0))
	require.Zero(t, metricValue(t, d.metrics, "dra_cpu_defrag_swap_overlap_seconds", nil),
		"a batch that ran out of time bounds no overlap, it measures the giving up")
}

func TestRetainUnsettledKeepsOnlyWhatIsWorthSendingAgain(t *testing.T) {
	// A round can hold a move into free CPUs beside an exchange. Sending the
	// whole round again would apply the move a second time, and for a move the
	// runtime refused it would apply it against the ledger, so only the exchange
	// nobody can place survives.
	round := newDefragRound(defaultScope(0), nil)
	round.moves = []defrag.Move{
		{ClaimUID: "moved", From: cpuset.New(0), To: cpuset.New(1)},
		{ClaimUID: "first", From: cpuset.New(2), To: cpuset.New(3), Exchange: 1},
		{ClaimUID: "second", From: cpuset.New(3), To: cpuset.New(2), Exchange: 1},
		{ClaimUID: "third", From: cpuset.New(4), To: cpuset.New(5), Exchange: 2},
		{ClaimUID: "fourth", From: cpuset.New(5), To: cpuset.New(4), Exchange: 2},
	}
	round.outcomes[1] = exchangeApplied
	for exchange, containers := range map[int][]types.UID{1: {"ctr-a"}, 2: {"ctr-b", "ctr-c"}} {
		round.exchangeContainers[exchange] = containers
		for _, containerUID := range containers {
			round.updateByContainer[containerUID] = &api.ContainerUpdate{ContainerId: string(containerUID)}
			round.claimsByContainer[containerUID] = []types.UID{"claim-of-" + containerUID}
		}
	}
	round.updates = []*api.ContainerUpdate{{ContainerId: "moved-ctr"}, {ContainerId: "ctr-a"}, {ContainerId: "ctr-b"}, {ContainerId: "ctr-c"}}

	next := round.retainUnsettled()

	require.Equal(t, []types.UID{"third", "fourth"}, stepClaims(next.moves))
	require.Len(t, next.updates, 2)
	require.Equal(t, []types.UID{"ctr-b", "ctr-c"}, next.exchangeContainers[2])
	require.NotContains(t, next.exchangeContainers, 1)
	require.Contains(t, next.claimsByContainer, types.UID("ctr-b"))
	require.NotContains(t, next.claimsByContainer, types.UID("ctr-a"))
}

func TestDefragPassLeavesAnUnansweredRetryUnsettled(t *testing.T) {
	// The runtime refuses one half, then stops answering the retry. That retry
	// may have been applied before the reply was lost, so undoing the other half
	// could be undoing a completed exchange: the round stays unsettled and the
	// read-back decides, rather than a third batch guessing.
	d := exchangeDriver(t)
	d.defrag.batchTimeout = 20 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	d.updater.reply = func(call int, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		if call == 1 {
			return refusing("ctr-uid-2")(call, updates)
		}
		<-release
		return nil, nil
	}

	done := make(chan struct{})
	go func() {
		d.defragPass(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass waited on the runtime instead of its own deadline")
	}

	require.Len(t, d.updater.allCalls(), 2, "no rollback is sent for a retry nobody answered")
	for _, claimUID := range []types.UID{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(claimUID)
		require.True(t, inFlight, "claim %s must still hold both cpusets", claimUID)
	}
	require.Contains(t, d.pendingRounds, defaultScope(0))
	require.True(t, d.nodeIsPoisoned(0), "an exchange in neither state fences its NUMA node")
}

func TestDefragPassTreatsAReplacedContainerAsMoved(t *testing.T) {
	// A container that died and came back mid-batch is refusing nothing: it was
	// recreated pinned from the store, which already holds the target. Counting
	// it as the refusing half would roll back the other one and leave the ledger
	// describing neither container.
	d := exchangeDriver(t)
	d.updater.reply = func(call int, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		if call == 1 {
			// The runtime refuses the container it no longer has, and the pod
			// brings a replacement up under a new runtime ID while the batch is out.
			d.runContainer(t, "pod-2", "ctr-2", "ctr-uid-2-replacement", "claim-2")
			return refusing("ctr-uid-2")(call, updates)
		}
		return nil, nil
	}

	d.defragPass(context.Background())

	require.Len(t, d.updater.allCalls(), 1, "nothing is retried and nothing is rolled back")
	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	second, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-2")
	require.Equal(t, cpuset.New(0, 1), first, "the exchange is recorded as applied")
	require.Equal(t, cpuset.New(2, 3), second)
	require.False(t, d.nodeIsPoisoned(0))
	require.Empty(t, d.pendingRounds)
	require.Zero(t, metricValue(t, d.metrics, "dra_cpu_defrag_partial_batches_total", nil))
}

func TestExactPlanArbitration(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))

	logger := testr.New(t)
	scope := defaultScope(0)
	online := d.allCPUs

	d.applyMu.Lock()
	moves := d.planScopeMoves(logger, scope, online)
	require.NotEmpty(t, moves, "expected greedy planner to produce moves")

	exactPlan := &defrag.ExactPlan{
		NUMANodeID: 0,
		Status:     defrag.SearchReachable,
		Moves: []defrag.Move{
			{ClaimUID: "claim-1", From: cpuset.New(0, 4), To: cpuset.New(0, 1)},
		},
	}
	d.setActiveExactPlan(0, exactPlan)
	require.True(t, d.hasActiveExactPlan(0))

	skippedMoves := d.planScopeMoves(logger, scope, online)
	require.Nil(t, skippedMoves, "expected greedy planner to skip NUMA node with active exact plan")
	d.applyMu.Unlock()

	// Cause ledger mismatch by unpreparing/rebinding claim-1
	d.applyMu.Lock()
	err := d.cpuAllocationStore.BeginRebind(logger, "claim-1", cpuset.New(2, 3))
	require.NoError(t, err)
	d.applyMu.Unlock()

	// beginDefragRound detects ledger mismatch and clears the active exact plan
	_ = d.beginDefragRound(logger, scope, online)

	d.applyMu.Lock()
	require.False(t, d.hasActiveExactPlan(0), "expected active exact plan to be cleared after ledger mismatch")
	d.applyMu.Unlock()
}

func TestClaimMovableExcludesRepairable(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-permissive", cpuset.New(0, 1))
	d.placeRepairableClaim(t, "claim-repairable", cpuset.New(2, 3))

	d.applyMu.Lock()
	defer d.applyMu.Unlock()

	require.True(t, d.claimMovable("claim-permissive"))
	require.False(t, d.claimMovable("claim-repairable"))
	require.True(t, d.claimMovableForExact("claim-repairable"))
}

func TestDefragPassRepairsRepairableClaimWithExactSearch(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeRepairableClaim(t, "claim-split", cpuset.New(0, 4))
	d.runContainer(t, "pod-1", "cont-1", "container-1", "claim-split")

	logger := testr.New(t)
	scope := defaultScope(0)
	online := d.allCPUs

	d.applyMu.Lock()
	greedyMoves := d.planScopeMoves(logger, scope, online)
	require.Empty(t, greedyMoves)
	d.applyMu.Unlock()

	round := d.beginDefragRound(logger, scope, online)
	require.NotNil(t, round)
	require.Len(t, round.moves, 1)
	require.Equal(t, types.UID("claim-split"), round.moves[0].ClaimUID)
	require.True(t, round.moves[0].To.IsSubsetOf(cpuset.New(0, 1, 2, 3)) || round.moves[0].To.IsSubsetOf(cpuset.New(4, 5, 6, 7)))

	d.applyMu.Lock()
	require.True(t, d.hasActiveExactPlan(0))
	require.False(t, d.cpuAllocationStore.ReservedClosure(0).IsEmpty())
	d.applyMu.Unlock()

	res := d.finishDefragRound(logger, round, nil, nil)
	require.Equal(t, cpumetrics.ResultSuccess, res)

	d.applyMu.Lock()
	require.False(t, d.hasActiveExactPlan(0))
	require.True(t, d.cpuAllocationStore.ReservedClosure(0).IsEmpty())
	record, ok := d.cpuAllocationStore.GetClaimRecord("claim-split")
	require.True(t, ok)
	nodeTopo, err := defrag.NewTopology(d.topology.cpuTopology, 0, d.allCPUs)
	require.NoError(t, err)
	require.Equal(t, 0, nodeTopo.ExcessSpread(store.UnionOf(record.Requests)))
	d.applyMu.Unlock()
}

type fakeClaimReader struct {
	claims      []*resourceapi.ResourceClaim
	deallocated map[types.UID]bool
}

func (f fakeClaimReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	return f.claims, nil
}

func (f fakeClaimReader) IsProjectedDeallocated(claimUID types.UID) bool {
	return f.deallocated != nil && f.deallocated[claimUID]
}

func TestAllocatedUnpreparedCPUsTreatedAsConsumed(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{
		"dev-0": cpuset.New(0, 1),
	}

	d.claimReader = fakeClaimReader{
		claims: []*resourceapi.ResourceClaim{
			{
				ObjectMeta: metav1.ObjectMeta{UID: "unprepared-claim"},
				Status: resourceapi.ResourceClaimStatus{
					Allocation: &resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Driver: d.driverName,
									Device: "dev-0",
								},
							},
						},
					},
				},
			},
		},
	}

	unprepared := d.allocatedUnpreparedCPUs(0)
	require.True(t, unprepared.Equals(cpuset.New(0, 1)))

	logger := testr.New(t)
	scope := defaultScope(0)
	d.applyMu.Lock()
	view, ok := d.defragView(logger, scope, d.allCPUs)
	require.True(t, ok)
	require.False(t, view.free.Contains(0))
	require.False(t, view.free.Contains(1))
	d.applyMu.Unlock()
}


