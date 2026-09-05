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
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/cpuset"
)

const fitTestNodeName = "node-1"

type fitTestDriver struct {
	*defragTestDriver
	client *fake.Clientset
}

// newFitTestDriver reserves CPU 0 of the SMT topology, which leaves its sibling
// 8 allocatable and useless at once: no whole-core claim can take a lone
// thread. So cache 0 offers 2 CPUs where its neighbour offers 4, and that
// raggedness -- the same shape a reserved core and a pool core cut into a real
// node's first cache -- is exactly what the annotation exists to carry.
func newFitTestDriver(t *testing.T, annotations map[string]string) *fitTestDriver {
	t.Helper()
	logger := testr.New(t)
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: smtPoolInfos()}).GetCPUTopology(logger)
	require.NoError(t, err)

	reserved := cpuset.New(0)
	allCPUs := topo.CPUDetails.CPUs()
	client := fake.NewClientset(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: fitTestNodeName, Annotations: annotations},
	})
	updater := &fakeContainerUpdater{}
	cdi := newMockCdiMgr()
	d := &CPUDriver{
		nodeName:   fitTestNodeName,
		kubeClient: client,
		topology: deviceTopology{
			cpuTopology: topo, reservedCPUs: reserved, onlineCPUs: allCPUs,
			numaNodeThreadsPerCore: map[int]int{0: 2, 1: 2},
		},
		cpuAllocationStore:   store.NewCPUAllocation(topo, reserved),
		podConfigStore:       store.NewPodConfig(),
		claimTracker:         store.NewClaimTracker(),
		cdiMgr:               cdi,
		containerUpdater:     updater,
		reconcileTrigger:     make(chan struct{}, 1),
		lastMoved:            make(map[types.UID]time.Time),
		fullPhysicalCPUsOnly: true,
		placementPolicy:      coreselect.Pack,
		sysfs: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte(allCPUs.String() + "\n")},
		},
		defrag: defragOptions{enabled: true, maxMoves: 4, minGain: 1},
		fit:    fitOptions{enabled: true, debounce: time.Millisecond, resync: time.Hour, synchronized: true},
	}
	return &fitTestDriver{
		defragTestDriver: &defragTestDriver{CPUDriver: d, updater: updater, cdi: cdi, allCPUs: allCPUs},
		client:           client,
	}
}

// straddleNUMANode0 gives claim-1 one core in each of NUMA node 0's caches, so
// the node is misaligned in a way a repack can and will fix.
func (d *fitTestDriver) straddleNUMANode0(t *testing.T) {
	t.Helper()
	d.placeClaim(t, "claim-1", cpuset.New(1, 3, 9, 11))
}

// node reads through the tracker rather than the clientset, so looking at the
// result does not itself count as a request the driver made.
func (d *fitTestDriver) node(t *testing.T) *corev1.Node {
	t.Helper()
	obj, err := d.client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", fitTestNodeName)
	require.NoError(t, err)
	node, ok := obj.(*corev1.Node)
	require.True(t, ok)
	return node
}

func (d *fitTestDriver) annotation(t *testing.T) (string, bool) {
	t.Helper()
	value, ok := d.node(t).Annotations[fitAnnotation]
	return value, ok
}

// verbs is what the driver actually asked the API server for, which is the
// whole point of the skip-if-unchanged path.
func (d *fitTestDriver) verbs() []string {
	var verbs []string
	for _, action := range d.client.Actions() {
		verbs = append(verbs, action.GetVerb())
	}
	return verbs
}

func (d *fitTestDriver) publish(t *testing.T, p *fitPublisher, force bool) {
	t.Helper()
	p.publish(testContext(t), force)
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	return ctxlog.NewContext(context.Background(), testr.New(t))
}

// TestFitReportDescribesTheNodesShape is the contract with the scheduler-side
// reader, which lives in another repository and cannot import this one. The
// exact bytes are the only thing holding the two schemas together.
func TestFitReportDescribesTheNodesShape(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)

	p := &fitPublisher{driver: d.CPUDriver}
	d.publish(t, p, false)

	value, ok := d.annotation(t)
	require.True(t, ok, "the annotation must be published")
	assert.JSONEq(t, `{
		"v": 1,
		"policy": "pack",
		"numaNodes": [
			{"id": 0, "cacheCPUs": [2, 4], "freeCPUs": [0, 2], "repackedFreeCPUs": [2, 0]},
			{"id": 1, "cacheCPUs": [4, 4], "freeCPUs": [4, 4], "repackedFreeCPUs": [4, 4]}
		]
	}`, value)

	// The two arrays disagreeing is the whole signal: nothing fits in cache 0
	// today, and a repack that moves the straddling claim into cache 1 gives
	// the whole of cache 0 back.
	assert.Equal(t, []string{"get", "patch"}, d.verbs())
}

// TestFitReportSaysNothingUntilTheRuntimeIsSynchronized: before the NRI hook
// has rebuilt the stores, the driver's allocation store is empty because
// nothing has told it otherwise -- not because the node is idle. Publishing
// then would advertise a full node as free.
func TestFitReportSaysNothingUntilTheRuntimeIsSynchronized(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)
	d.fit.synchronized = false

	report, ok, err := d.buildFitReport(testr.New(t))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Nil(t, report)

	p := &fitPublisher{driver: d.CPUDriver}
	d.publish(t, p, true)
	assert.Empty(t, d.verbs(), "a driver with nothing to say must not touch the API server")

	_, published := d.annotation(t)
	assert.False(t, published)
}

// TestFitReportRepackedIsTodaysFreeWithoutDefragmentation: the repacked shape
// is a promise that something will act on it. With no defragmenter running,
// advertising a consolidation would route claims here for a repair that never
// comes.
func TestFitReportRepackedIsTodaysFreeWithoutDefragmentation(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)
	d.defrag.enabled = false

	report, ok, err := d.buildFitReport(testr.New(t))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, report.NUMA, 2)
	assert.Equal(t, []int{0, 2}, report.NUMA[0].FreeCPUs)
	assert.Equal(t, []int{0, 2}, report.NUMA[0].RepackedFreeCPUs,
		"nothing is going to repack this node, so the repacked shape is today's")
}

// TestFitReportRepackedIsTodaysFreeWhenAClaimSplitsACore: a claim prepared
// before whole-core allocation was turned on holds lone threads, and the CPUs
// a move would vacate then include ones no whole-core claim can use. The free
// CPUs reported alongside are rounded to complete cores and would never say
// that, so the two arrays would contradict each other.
//
// The claim splits two cores while taking a legal whole-core NUMBER of CPUs,
// which is what makes this the case that matters: the allocator has no reason
// to refuse a request of that size, so nothing downstream catches it and the
// guard is the only thing standing between a move and a repacked shape
// offering half-cores as free.
func TestFitReportRepackedIsTodaysFreeWhenAClaimSplitsACore(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.placeClaim(t, "claim-legacy", cpuset.New(1, 3))

	report, ok, err := d.buildFitReport(testr.New(t))
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, report.NUMA, 2)
	assert.Equal(t, report.NUMA[0].FreeCPUs, report.NUMA[0].RepackedFreeCPUs,
		"a repack of a core-splitting claim would offer half-cores as free, so today's shape is the only honest one")
	assertFitInvariants(t, report)

	// Without the guard the settled state is reachable and reports 1 CPU free
	// in each of two caches -- two half-cores, which whole-core allocation can
	// never hand out. Pin that it is not what is published.
	for _, free := range report.NUMA[0].RepackedFreeCPUs {
		assert.NotEqual(t, 1, free, "published a lone thread as free under whole-core allocation")
	}
}

// TestFitPublisherSkipsAnUnchangedShape: a node whose shape has not moved must
// cost nothing at all -- not a write, and not even a read.
func TestFitPublisherSkipsAnUnchangedShape(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)
	p := &fitPublisher{driver: d.CPUDriver}

	d.publish(t, p, false)
	require.Equal(t, []string{"get", "patch"}, d.verbs())

	d.client.ClearActions()
	d.publish(t, p, false)
	assert.Empty(t, d.verbs(), "an unchanged shape must not reach the API server")
}

// TestFitPublisherRestoresAStompedAnnotation: the resync exists because a write
// is not the only way the annotation can change.
func TestFitPublisherRestoresAStompedAnnotation(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)
	p := &fitPublisher{driver: d.CPUDriver}
	d.publish(t, p, false)
	published, ok := d.annotation(t)
	require.True(t, ok)

	require.NoError(t, d.patchFitAnnotation(context.Background(), ptr("someone else was here")))
	d.client.ClearActions()

	// Unforced, the publisher still believes what it last wrote.
	d.publish(t, p, false)
	stomped, _ := d.annotation(t)
	require.Equal(t, "someone else was here", stomped)

	d.publish(t, p, true)
	restored, ok := d.annotation(t)
	require.True(t, ok)
	assert.Equal(t, published, restored)
}

// TestFitPublisherWritesAgainAfterAFailedWrite: a publisher that remembered a
// value it never managed to write would go quiet until the shape changed again.
func TestFitPublisherWritesAgainAfterAFailedWrite(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)

	var refuse bool
	d.client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !refuse {
			return false, nil, nil
		}
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, fitTestNodeName, assert.AnError)
	})

	p := &fitPublisher{driver: d.CPUDriver}
	refuse = true
	d.publish(t, p, false)
	_, ok := d.annotation(t)
	require.False(t, ok, "the write was refused")

	refuse = false
	d.client.ClearActions()
	d.publish(t, p, false)
	_, ok = d.annotation(t)
	assert.True(t, ok, "the next round must try again rather than trust what it never wrote")
}

// TestClearFitAnnotationRemovesOnlyWhatIsThere: an annotation this driver has
// stopped keeping true is worse than none, and nothing else will notice it.
func TestClearFitAnnotationRemovesOnlyWhatIsThere(t *testing.T) {
	t.Run("nothing to remove", func(t *testing.T) {
		d := newFitTestDriver(t, nil)
		d.clearFitAnnotation(testContext(t))
		assert.Equal(t, []string{"get"}, d.verbs(), "a node with no annotation must not be written to")
	})

	t.Run("a stale one", func(t *testing.T) {
		d := newFitTestDriver(t, map[string]string{
			fitAnnotation: `{"v":1,"numaNodes":[]}`,
			"other":       "untouched",
		})
		d.clearFitAnnotation(testContext(t))

		assert.Equal(t, []string{"get", "patch"}, d.verbs())
		_, ok := d.annotation(t)
		assert.False(t, ok, "the stale annotation must be gone")

		assert.Equal(t, "untouched", d.node(t).Annotations["other"], "a merge patch must leave the rest alone")
	})
}

// TestFitOnlyDriverPushesNoContainerUpdates: publishing writes an annotation and
// asks the runtime for nothing, so a driver that only publishes must never send
// an update the runtime did not ask for -- the hazard the operator's
// assumeUnsolicitedUpdatesSafe assertion covers, which publishing does not need.
func TestFitOnlyDriverPushesNoContainerUpdates(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.straddleNUMANode0(t)
	d.defrag.enabled = false
	d.reconcileSharedOnUnprepare = false
	d.runContainer(t, "pod-uid-s", "ctr-s", "ctr-uid-s")

	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	go d.runReconcileWorker(ctx)
	d.requestReconcile()

	require.Eventually(t, func() bool {
		_, ok := d.annotation(t)
		return ok
	}, 5*time.Second, 5*time.Millisecond, "the worker must still publish")
	assert.Empty(t, d.updater.allCalls(), "a driver that only publishes must push no container updates")
}

// assertFitInvariants checks what the scheduler-side reader validates before it
// will use a payload at all: it rejects the whole node on any of these.
func assertFitInvariants(t *testing.T, report *fitReport) {
	t.Helper()
	require.NotEmpty(t, report.NUMA)
	for _, numa := range report.NUMA {
		require.NotEmpty(t, numa.CacheCPUs)
		require.Len(t, numa.FreeCPUs, len(numa.CacheCPUs))
		require.Len(t, numa.RepackedFreeCPUs, len(numa.CacheCPUs))
		freeSum, repackedSum := 0, 0
		for i, capacity := range numa.CacheCPUs {
			assert.GreaterOrEqual(t, capacity, 0)
			assert.GreaterOrEqual(t, numa.FreeCPUs[i], 0)
			assert.GreaterOrEqual(t, numa.RepackedFreeCPUs[i], 0)
			assert.LessOrEqual(t, numa.FreeCPUs[i], capacity)
			assert.LessOrEqual(t, numa.RepackedFreeCPUs[i], capacity)
			freeSum += numa.FreeCPUs[i]
			repackedSum += numa.RepackedFreeCPUs[i]
		}
		assert.LessOrEqual(t, freeSum, repackedSum, "a repack never frees fewer CPUs than are free now")
	}
}

func ptr(s string) *string { return &s }

// TestSynchronizeArmsTheFirstPublish: a node whose claims are not changing has
// nothing else to trigger one, so without this the first annotation waits for
// the resync -- a minute during which a node that has just joined, or one whose
// claims were released while its driver was down, is described wrongly.
func TestSynchronizeArmsTheFirstPublish(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.fit.synchronized = false
	// Drain, so what the hook leaves behind is unambiguous.
	select {
	case <-d.reconcileTrigger:
	default:
	}

	_, err := d.Synchronize(testContext(t), nil, nil)
	require.NoError(t, err)

	assert.True(t, d.fit.synchronized, "the stores now describe what is running")
	select {
	case <-d.reconcileTrigger:
	default:
		t.Fatal("Synchronize left the worker with no reason to publish")
	}
}

// TestSynchronizeDoesNotPokeTheWorkerWhenNotPublishing keeps the poke out of
// the way of a driver that has no annotation to write.
func TestSynchronizeDoesNotPokeTheWorkerWhenNotPublishing(t *testing.T) {
	d := newFitTestDriver(t, nil)
	d.fit.enabled = false
	select {
	case <-d.reconcileTrigger:
	default:
	}

	_, err := d.Synchronize(testContext(t), nil, nil)
	require.NoError(t, err)

	select {
	case <-d.reconcileTrigger:
		t.Fatal("poked the worker for an annotation this driver does not publish")
	default:
	}
}
