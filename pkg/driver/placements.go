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
	"encoding/json"
	"net/http"
	"sort"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// placementsReport is which CPUs back each claim on this node, and how well
// placed they are.
//
// Placement is not published to the API. It is the driver's own answer to
// "which", where a claim's quantity is the scheduler's, so this endpoint is the
// only way to see it -- and it cannot be stale, since it is computed on request.
type placementsReport struct {
	NodeName      string `json:"nodeName"`
	DefragEnabled bool   `json:"defragEnabled"`
	ReservedCPUs  string `json:"reservedCPUs"`
	// SharedCPUs is what a container holding no claim is confined to, which
	// once the node's cores are described is what the claims left unclaimed
	// inside the partitions such a container may run in.
	SharedCPUs   string             `json:"sharedCPUs"`
	Claims       []claimReport      `json:"claims"`
	NUMANodes    []numaNodeReport   `json:"numaNodes"`
	Unmeasurable []unmeasurableNode `json:"unmeasurableNUMANodes,omitempty"`
}

type claimReport struct {
	ClaimUID string `json:"claimUID"`
	CPUs     string `json:"cpus"`
	// MovingFrom is the CPUs a claim is moving away from and still holds, set only
	// while a move is in flight.
	MovingFrom    string `json:"movingFrom,omitempty"`
	PodUID        string `json:"podUID,omitempty"`
	ContainerName string `json:"containerName,omitempty"`
	ContainerID   string `json:"containerID,omitempty"`
}

type numaNodeReport struct {
	NUMANodeID int    `json:"numaNodeID"`
	FreeCPUs   string `json:"freeCPUs"`
	// ExcessUncoreCaches is how many caches this node's claims span beyond the
	// fewest their sizes allow, summed over its partitions: the whole node's to
	// repair, however its cores are divided.
	ExcessUncoreCaches int `json:"excessUncoreCaches"`
	// LargestAlignableFreeCPUs is the largest claim this node could still take
	// inside a single cache, which is the best of its partitions: a claim lands
	// inside one of them.
	LargestAlignableFreeCPUs int           `json:"largestAlignableFreeCPUs"`
	Caches                   []cacheReport `json:"caches"`
	// Plans is what a pass would do to each of this node's partitions, one round
	// each, and is present only for a dry run.
	Plans []partitionPlan `json:"plans,omitempty"`
}

// partitionPlan is what a pass would do to one partition of one NUMA node,
// which is the region one round covers and the whole of where its moves may
// land.
type partitionPlan struct {
	Partition string `json:"partition"`
	CPUs      string `json:"cpus"`
	FreeCPUs  string `json:"freeCPUs"`
	planReport
}

type cacheReport struct {
	CacheID  int    `json:"cacheID"`
	CPUs     string `json:"cpus"`
	FreeCPUs string `json:"freeCPUs"`
}

type planReport struct {
	Moves       []moveReport `json:"moves"`
	CurrentCost int          `json:"currentCost"`
	IdealCost   int          `json:"idealCost"`
	// Blocked counts the moves a better placement calls for that this pass could
	// not make.
	Blocked int `json:"blocked"`
	// Reason says what kept the plan from going further, which is the answer to
	// "why is this claim still split?".
	Reason string `json:"reason,omitempty"`
}

type moveReport struct {
	ClaimUID string `json:"claimUID"`
	From     string `json:"from"`
	To       string `json:"to"`
}

// unmeasurableNode is a NUMA node placement cannot be reasoned about on, which
// is why nothing is reported for it.
type unmeasurableNode struct {
	NUMANodeID int    `json:"numaNodeID"`
	Reason     string `json:"reason"`
}

// ServePlacements answers GET /placements. With dryrun=1 it also returns the
// moves a pass would make right now and, when it would make none, which gate
// stopped it.
//
// A dry run changes nothing: planning is a pure function of the same snapshot
// this reports.
func (cp *CPUDriver) ServePlacements(w http.ResponseWriter, r *http.Request) {
	logger := ctxlog.FromContext(r.Context())
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "only GET is supported", http.StatusMethodNotAllowed)
		return
	}
	dryRun := r.URL.Query().Get("dryrun") == "1"

	report, err := cp.placements(logger, dryRun)
	if err != nil {
		logger.Error(err, "cannot report placements")
		http.Error(w, "cannot report placements: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Encoded with the lock released: a slow reader must not hold up the driver.
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		logger.Error(err, "cannot encode placements")
		http.Error(w, "cannot encode placements", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(append(body, '\n')); err != nil {
		logger.V(2).Info("could not write placements response", "error", err.Error())
	}
}

// dryRunInput is one scope's planning input, captured under applyMu so the plan
// itself can be computed without it: the same view a pass plans from, plus where
// to write the answer and a mobility test bound to the store that was current.
type dryRunInput struct {
	defragScopeView
	report  *partitionPlan
	movable func(types.UID) bool
}

// placements builds the report from one consistent snapshot.
//
// A dry run's planning happens after applyMu is released. Planning is a pure
// function of the snapshot it was handed, so the answer is the same either way,
// and holding the lock for it would let a remote poller of this endpoint stall
// Prepare and CreateContainer for as long as it takes.
func (cp *CPUDriver) placements(logger logr.Logger, dryRun bool) (*placementsReport, error) {
	online, err := cp.currentOnlineCPUs(logger)
	if err != nil {
		return nil, err
	}

	report, planning := cp.placementsSnapshot(logger, online, dryRun)
	for _, input := range planning {
		cp.planPartitionReport(logger, input)
	}
	return report, nil
}

// placementsSnapshot reads everything the report and any plan need, under the
// one lock, and returns the planning inputs for the caller to use without it.
//
// Every per-partition number comes from the same defragView a pass plans from,
// so the endpoint cannot describe a pass this driver would not run.
func (cp *CPUDriver) placementsSnapshot(logger logr.Logger, online cpuset.CPUSet, dryRun bool) (*placementsReport, []dryRunInput) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	topo := cp.topology.cpuTopology
	allocatable := cp.defragAllocatable(online)
	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	if cp.fullPhysicalCPUsOnly {
		free = topo.CPUDetails.CompleteCores(free)
	}
	allocations := cp.cpuAllocationStore.ExclusiveClaimAllocations()
	// Captured rather than read through cp: Synchronize replaces the store
	// wholesale, and the plan below runs with the lock released.
	snapshot := cp.cpuAllocationStore

	report := &placementsReport{
		NodeName:      cp.nodeName,
		DefragEnabled: cp.defrag.enabled,
		ReservedCPUs:  cp.topology.reservedCPUs.String(),
		SharedCPUs:    cp.sharedContainerCPUs().String(),
		Claims:        cp.claimReports(allocations),
	}

	numaNodeIDs := topo.CPUDetails.NUMANodes().List()
	report.NUMANodes = make([]numaNodeReport, 0, len(numaNodeIDs))
	var planning []dryRunInput
	for _, numaNodeID := range numaNodeIDs {
		nodeTopo, err := defrag.NewTopology(topo, numaNodeID, allocatable)
		if err != nil {
			report.Unmeasurable = append(report.Unmeasurable, unmeasurableNode{
				NUMANodeID: numaNodeID,
				Reason:     err.Error(),
			})
			continue
		}
		views, shape := cp.defragNodeViews(logger, nodeTopo, online)
		report.NUMANodes = append(report.NUMANodes, cp.numaNodeReport(nodeTopo, free, shape))
		if !dryRun {
			continue
		}
		nodeReport := &report.NUMANodes[len(report.NUMANodes)-1]
		nodeReport.Plans = make([]partitionPlan, 0, len(views))
		for _, view := range views {
			nodeReport.Plans = append(nodeReport.Plans, partitionPlan{
				Partition: view.scope.partition,
				CPUs:      view.topology.CPUs().String(),
				FreeCPUs:  view.free.String(),
			})
			planning = append(planning, dryRunInput{
				defragScopeView: view,
				report:          &nodeReport.Plans[len(nodeReport.Plans)-1],
				movable:         func(claimUID types.UID) bool { return claimMovableIn(snapshot, claimUID) },
			})
		}
	}
	return report, planning
}

func (cp *CPUDriver) claimReports(allocations map[types.UID]cpuset.CPUSet) []claimReport {
	claimUIDs := make([]types.UID, 0, len(allocations))
	for claimUID := range allocations {
		claimUIDs = append(claimUIDs, claimUID)
	}
	sort.Slice(claimUIDs, func(i, j int) bool { return claimUIDs[i] < claimUIDs[j] })

	reports := make([]claimReport, 0, len(claimUIDs))
	for _, claimUID := range claimUIDs {
		report := claimReport{
			ClaimUID: string(claimUID),
			CPUs:     allocations[claimUID].String(),
		}
		if origin, inFlight := cp.cpuAllocationStore.GetRebindOrigin(claimUID); inFlight {
			report.MovingFrom = origin.String()
		}
		if owner, ok := cp.claimTracker.Owner(claimUID); ok {
			report.PodUID = string(owner.PodUID)
			report.ContainerName = owner.ContainerName
			if state := cp.podConfigStore.GetContainerState(owner.PodUID, owner.ContainerName); state != nil {
				report.ContainerID = string(state.ContainerUID())
			}
		}
		reports = append(reports, report)
	}
	return reports
}

// numaNodeReport is the node's own shape: its caches and what is free in each,
// whatever partition holds them, plus the two counts its partitions add up to.
func (cp *CPUDriver) numaNodeReport(nodeTopo *defrag.Topology, free cpuset.CPUSet, shape defragNodeShape) numaNodeReport {
	nodeFree := free.Intersection(nodeTopo.CPUs())
	report := numaNodeReport{
		NUMANodeID:               nodeTopo.NUMANodeID(),
		FreeCPUs:                 nodeFree.String(),
		ExcessUncoreCaches:       shape.excessUncoreCaches,
		LargestAlignableFreeCPUs: shape.largestAlignableFreeCPUs,
	}
	for _, cacheID := range nodeTopo.Caches() {
		inCache := nodeTopo.CPUsInCache(cacheID)
		report.Caches = append(report.Caches, cacheReport{
			CacheID:  cacheID,
			CPUs:     inCache.String(),
			FreeCPUs: inCache.Intersection(nodeFree).String(),
		})
	}
	return report
}

// planPartitionReport fills in one partition's dry-run plan. Called with applyMu
// released.
func (cp *CPUDriver) planPartitionReport(logger logr.Logger, input dryRunInput) {
	plan, err := defrag.PlanNode(input.topology, input.placements, input.free, cp.defragSelector(logger, input.threadsPerCore), defrag.Options{
		Eligible:             input.movable,
		AllowSwaps:           cp.defrag.allowTransientOverlap,
		KeepFreePoolNonEmpty: input.keepFreePoolNonEmpty,
	})
	if err != nil {
		input.report.planReport = planReport{Reason: "cannot plan: " + err.Error()}
		return
	}
	input.report.planReport = planReport{
		Moves:       make([]moveReport, 0, len(plan.Moves)),
		CurrentCost: plan.CurrentCost,
		IdealCost:   plan.IdealCost,
		Blocked:     plan.Blocked,
		Reason:      plan.Reason,
	}
	for _, move := range plan.Moves {
		input.report.Moves = append(input.report.Moves, moveReport{
			ClaimUID: string(move.ClaimUID),
			From:     move.From.String(),
			To:       move.To.String(),
		})
	}
}
