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
	"encoding/json"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// fitAnnotation carries the shape of this node's free CPUs to a scheduler.
//
// What the scheduler sees of a device is one number, the capacity nothing has
// consumed yet, and a number has no shape: 32 free CPUs scattered two per
// uncore cache and 32 forming two whole caches are the same 32. So a claim
// wanting a whole cache can be bound to a node that will never give it one
// while a neighbour would have, and a bound claim never moves to another node.
// This is the shape that number leaves out.
const fitAnnotation = "dra.cpu/fit"

// fitVersion is the payload version. Bump it for a change a reader of the
// current schema would misread; readers ignore an annotation they do not know
// the version of, which is what makes a driver upgrade safe to roll out under
// a scheduler that has not been.
const fitVersion = 1

const (
	// fitPublishDebounce is how long a change waits for others to join it. A
	// pod with several claims prepares them one by one, and each prepare
	// changes the shape.
	fitPublishDebounce = 2 * time.Second
	// fitResyncInterval is how often the publisher re-reads its own Node with
	// nothing to say, which is what restores an annotation something else
	// overwrote. Jittered so a fleet restarted together does not read in step.
	fitResyncInterval = 60 * time.Second
	fitResyncJitter   = 0.2
)

// fitOptions is what the driver knows about publishing, fixed at startup apart
// from synchronized.
type fitOptions struct {
	enabled bool
	// debounce and resync are the two intervals above. Fields rather than the
	// constants directly, so a test can drive the worker without waiting.
	debounce time.Duration
	resync   time.Duration
	// synchronized is whether the NRI Synchronize hook has rebuilt the stores
	// from the runtime at least once. Guarded by applyMu.
	//
	// Before it has, the allocation store is empty because nothing has told the
	// driver otherwise -- not because the node is idle. An annotation published
	// then advertises every CPU on a busy node as free, and every whole-cache
	// claim in the cluster would be steered at it.
	synchronized bool
}

// fitReport is the annotation payload.
type fitReport struct {
	V      int       `json:"v"`
	Policy string    `json:"policy,omitempty"`
	NUMA   []numaFit `json:"numaNodes"`
}

// numaFit describes one NUMA node's uncore caches with three index-aligned
// arrays: what each cache can hold, what is free in it now, and what would be
// free once the defragmenter has repacked what it is actually willing to move.
//
// Per cache rather than a count of claims that would fit, because a claim
// larger than one cache needs the distribution, and because a reader
// subtracting the claims it has already placed this scheduling cycle has to
// subtract them from somewhere.
type numaFit struct {
	ID               int   `json:"id"`
	CacheCPUs        []int `json:"cacheCPUs"`
	FreeCPUs         []int `json:"freeCPUs"`
	RepackedFreeCPUs []int `json:"repackedFreeCPUs"`
}

// fitSnapshot is everything a report is computed from, read in one consistent
// pass so that the settling which follows -- up to a full defragmentation plan
// per NUMA node -- can run with applyMu released. Every field is either
// immutable after startup or a copy, so nothing here can be changed underneath
// the computation.
type fitSnapshot struct {
	topo        *cpuinfo.CPUTopology
	allocatable cpuset.CPUSet
	free        cpuset.CPUSet
	usable      cpuset.CPUSet
	placements  map[int][]defrag.Placement
	keepFree    bool
}

// takeFitSnapshot reads the node under applyMu. ok is false when the driver has
// nothing it can stand behind yet.
func (cp *CPUDriver) takeFitSnapshot(online cpuset.CPUSet) (fitSnapshot, bool) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if !cp.fit.synchronized {
		return fitSnapshot{}, false
	}

	topo := cp.topology.cpuTopology
	allocatable := online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)
	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	// usable is the capacity a claim can actually be given. Under whole-core
	// allocation the odd thread of a core the reserved set or the shared pool
	// split is allocatable and unusable at once, and a reader deciding whether
	// a cache is untouched compares its free CPUs against this number.
	usable := allocatable
	if cp.fullPhysicalCPUsOnly {
		free = topo.CPUDetails.CompleteCores(free)
		usable = topo.CPUDetails.CompleteCores(allocatable)
	}

	return fitSnapshot{
		topo:        topo,
		allocatable: allocatable,
		free:        free,
		usable:      usable,
		placements:  defrag.PlacementsByNUMANode(topo, cp.cpuAllocationStore.ResourceClaimAllocations()),
		keepFree:    cp.keepFreePoolNonEmpty(),
	}, true
}

// buildFitReport describes the node from one consistent snapshot. ok is false
// when the driver has nothing it can stand behind yet, which is not a failure
// and not something to publish either.
//
// Only the snapshot is taken under applyMu. The rest -- including settling each
// NUMA node, which is bounded but not cheap -- runs with the lock released,
// because that lock is what every prepare, unprepare and container creation on
// this node serializes on.
func (cp *CPUDriver) buildFitReport(logger logr.Logger) (report *fitReport, ok bool, err error) {
	online, err := cp.currentOnlineCPUs(logger)
	if err != nil {
		return nil, false, err
	}

	snap, ok := cp.takeFitSnapshot(online)
	if !ok {
		return nil, false, nil
	}

	report = &fitReport{V: fitVersion, Policy: string(cp.placementPolicy)}
	for _, numaNodeID := range snap.topo.CPUDetails.NUMANodes().List() {
		nodeTopo, topoErr := defrag.NewTopology(snap.topo, numaNodeID, snap.allocatable)
		if topoErr != nil {
			// A node whose caches cannot be read is left out rather than
			// described wrongly; a reader treats a node it finds nothing for as
			// unknown.
			logger.V(4).Info("leaving a NUMA node out of the fit annotation", "numaNode", numaNodeID, "reason", topoErr.Error())
			continue
		}
		report.NUMA = append(report.NUMA, cp.numaFit(logger, snap, nodeTopo))
	}
	if len(report.NUMA) == 0 {
		return nil, false, nil
	}
	return report, true, nil
}

// numaFit measures one NUMA node from the snapshot.
func (cp *CPUDriver) numaFit(logger logr.Logger, snap fitSnapshot, nodeTopo *defrag.Topology) numaFit {
	caches := nodeTopo.Caches()
	placements := snap.placements[nodeTopo.NUMANodeID()]
	nodeFree := snap.free.Intersection(nodeTopo.CPUs())

	fit := numaFit{
		ID:        nodeTopo.NUMANodeID(),
		CacheCPUs: make([]int, len(caches)),
		FreeCPUs:  make([]int, len(caches)),
	}
	for i, cacheID := range caches {
		inCache := nodeTopo.CPUsInCache(cacheID)
		fit.CacheCPUs[i] = inCache.Intersection(snap.usable).Size()
		fit.FreeCPUs[i] = inCache.Intersection(nodeFree).Size()
	}
	fit.RepackedFreeCPUs = cp.repackedFreeCPUs(logger, snap, nodeTopo, placements, nodeFree, caches, fit.FreeCPUs)
	return fit
}

// repackedFreeCPUs is what would be free per cache once the defragmenter has
// settled this node, and today's free CPUs whenever that cannot be said
// honestly.
//
// The settled state rather than the ideal packing: the pass permanently honours
// its minimum gain and the shared-pool guard, so the ideal can be forever out
// of reach, and a reader steering a claim here for a consolidation that never
// comes is worse off than one that was told nothing.
func (cp *CPUDriver) repackedFreeCPUs(logger logr.Logger, snap fitSnapshot, nodeTopo *defrag.Topology, placements []defrag.Placement, nodeFree cpuset.CPUSet, caches []int, freeNow []int) []int {
	asIs := func() []int { return append([]int(nil), freeNow...) }

	// Nobody is going to repack it.
	if !cp.defrag.enabled {
		return asIs()
	}
	// A claim holding half a physical core -- prepared before whole-core
	// allocation was turned on, and restored from its own spec ever since --
	// hands a move the odd thread to vacate, and that thread lands in the free
	// set as a CPU no whole-core claim can take. The free CPUs reported now are
	// rounded to complete cores and would never say that, so the two arrays
	// would contradict each other. Report today's shape instead.
	//
	// Judged against the claim's own CPUs rather than the node's usable set: a
	// claim splitting a core whose sibling merely happens to be unallocated is
	// a subset of that set and slips straight through.
	if cp.fullPhysicalCPUsOnly {
		details := snap.topo.CPUDetails
		for _, p := range placements {
			if !p.CPUs.Equals(details.CompleteCores(p.CPUs)) {
				logger.V(2).Info("claim is not placed on whole cores, reporting today's free CPUs as the repacked shape",
					"numaNode", nodeTopo.NUMANodeID(), "claimUID", p.ClaimUID, "cpus", p.CPUs.String())
				return asIs()
			}
		}
	}

	_, settled, err := defrag.Settle(nodeTopo, placements, nodeFree, cp.defragSelector(logger, cp.topology.numaNodeThreadsPerCore[nodeTopo.NUMANodeID()]), defrag.Options{
		// No MaxMoves: it is a budget for one pass, not a limit on where the
		// node ends up. No Eligible either -- both of the pass's refusals, an
		// in-flight rebind and the cooldown, lapse on their own.
		MinGain:              cp.defrag.minGain,
		KeepFreePoolNonEmpty: snap.keepFree,
	})
	if err != nil {
		logger.V(2).Info("cannot settle a NUMA node, reporting today's free CPUs as the repacked shape",
			"numaNode", nodeTopo.NUMANodeID(), "reason", err.Error())
		return asIs()
	}

	repacked := make([]int, len(caches))
	for i, cacheID := range caches {
		repacked[i] = nodeTopo.CPUsInCache(cacheID).Intersection(settled).Size()
	}
	return repacked
}

// fitPublisher keeps the fit annotation on this driver's own Node up to date.
//
// Only the reconcile worker owns one, so nothing here is shared and nothing
// here is locked. Only the snapshot inside buildFitReport takes applyMu; every
// call into the API server happens with it released.
type fitPublisher struct {
	driver *CPUDriver
	// published is the value this publisher last saw on the Node. Cleared
	// whenever a write fails, so the next round writes again.
	published string
}

// publish brings the Node's annotation up to date with the node's shape.
//
// Without force, an unchanged shape costs nothing at all: no read, no write.
// With it, the live Node is read and compared even so, which is what restores
// an annotation something else overwrote.
func (p *fitPublisher) publish(ctx context.Context, force bool) {
	cp := p.driver
	logger := ctxlog.FromContext(ctx)
	if !cp.fit.enabled || cp.kubeClient == nil {
		return
	}

	report, ok, err := cp.buildFitReport(logger)
	if err != nil {
		logger.Error(err, "cannot describe this node's free CPU shape")
		return
	}
	if !ok {
		return
	}
	payload, err := json.Marshal(report)
	if err != nil {
		logger.Error(err, "cannot encode the fit annotation")
		return
	}

	value := string(payload)
	if value == p.published && !force {
		return
	}
	node, err := cp.kubeClient.CoreV1().Nodes().Get(ctx, cp.nodeName, metav1.GetOptions{})
	if err != nil {
		logger.Error(err, "cannot read this node to compare its fit annotation")
		return
	}
	if node.Annotations[fitAnnotation] == value {
		p.published = value
		return
	}
	if err := cp.patchFitAnnotation(ctx, &value); err != nil {
		p.published = ""
		logger.Error(err, "cannot publish the fit annotation")
		return
	}
	p.published = value
	logger.V(4).Info("published the fit annotation", "fit", value)
}

// clearFitAnnotation removes an annotation left behind by a driver that used to
// publish one.
//
// Nothing else will: the annotation describes a node whose driver has stopped
// describing it, so it would sit there for as long as the node lives, steering
// a scheduler by a shape that stopped being true the moment publishing was
// turned off.
func (cp *CPUDriver) clearFitAnnotation(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	if cp.kubeClient == nil {
		return
	}
	node, err := cp.kubeClient.CoreV1().Nodes().Get(ctx, cp.nodeName, metav1.GetOptions{})
	if err != nil {
		logger.V(2).Info("cannot check this node for a stale fit annotation", "error", err.Error())
		return
	}
	if _, stale := node.Annotations[fitAnnotation]; !stale {
		return
	}
	if err := cp.patchFitAnnotation(ctx, nil); err != nil {
		// Removing it needs the same permission publishing does, which an
		// operator who has turned publishing off may well have taken away in
		// the same breath.
		logger.Info("cannot remove the stale fit annotation, remove it by hand",
			"command", "kubectl annotate node "+cp.nodeName+" "+fitAnnotation+"-", "error", err.Error())
		return
	}
	logger.Info("removed a stale fit annotation left by an earlier configuration")
}

// patchFitAnnotation sets the annotation to value, or removes it when value is
// nil: a JSON merge patch deletes the key a null stands against.
func (cp *CPUDriver) patchFitAnnotation(ctx context.Context, value *string) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]*string{fitAnnotation: value},
		},
	})
	if err != nil {
		return err
	}
	_, err = cp.kubeClient.CoreV1().Nodes().Patch(ctx, cp.nodeName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}
