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
	// fewest their sizes allow.
	ExcessUncoreCaches int `json:"excessUncoreCaches"`
	// LargestAlignableFreeCPUs is the largest claim this node could still take
	// inside a single cache.
	LargestAlignableFreeCPUs int           `json:"largestAlignableFreeCPUs"`
	Caches                   []cacheReport `json:"caches"`
	// Plan is what a pass would do to this node, present only for a dry run.
	Plan *planReport `json:"plan,omitempty"`
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

// dryRunInput is one NUMA node's planning input, captured under applyMu so the
// plan itself can be computed without it.
type dryRunInput struct {
	report         *numaNodeReport
	topology       *defrag.Topology
	placements     []defrag.Placement
	free           cpuset.CPUSet
	threadsPerCore int
	movable        func(types.UID) bool
	keepFreePool   bool
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

	report, planning := cp.placementsSnapshot(online, dryRun)
	for _, input := range planning {
		cp.planNodeReport(logger, input)
	}
	return report, nil
}

// placementsSnapshot reads everything the report and any plan need, under the
// one lock, and returns the planning inputs for the caller to use without it.
func (cp *CPUDriver) placementsSnapshot(online cpuset.CPUSet, dryRun bool) (*placementsReport, []dryRunInput) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	topo := cp.topology.cpuTopology
	allocatable := online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)
	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	if cp.fullPhysicalCPUsOnly {
		free = topo.CPUDetails.CompleteCores(free)
	}
	allocations := cp.cpuAllocationStore.ExclusiveClaimAllocations()
	// Captured rather than read through cp: Synchronize replaces the store
	// wholesale, and the plan below runs with the lock released.
	snapshot := cp.cpuAllocationStore
	keepFreePool := len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0

	report := &placementsReport{
		NodeName:      cp.nodeName,
		DefragEnabled: cp.defrag.enabled,
		ReservedCPUs:  cp.topology.reservedCPUs.String(),
		SharedCPUs:    cp.sharedContainerCPUs().String(),
		Claims:        cp.claimReports(allocations),
	}

	placements := defrag.PlacementsByNUMANode(topo, allocations)
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
		report.NUMANodes = append(report.NUMANodes, cp.numaNodeReport(nodeTopo, placements[numaNodeID], free))
		if !dryRun {
			continue
		}
		planning = append(planning, dryRunInput{
			report:         &report.NUMANodes[len(report.NUMANodes)-1],
			topology:       nodeTopo,
			placements:     placements[numaNodeID],
			free:           free.Intersection(nodeTopo.CPUs()),
			threadsPerCore: cp.topology.numaNodeThreadsPerCore[numaNodeID],
			movable:        func(claimUID types.UID) bool { return claimMovableIn(snapshot, claimUID) },
			keepFreePool:   keepFreePool,
		})
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

func (cp *CPUDriver) numaNodeReport(nodeTopo *defrag.Topology, placements []defrag.Placement, free cpuset.CPUSet) numaNodeReport {
	nodeFree := free.Intersection(nodeTopo.CPUs())
	report := numaNodeReport{
		NUMANodeID:               nodeTopo.NUMANodeID(),
		FreeCPUs:                 nodeFree.String(),
		ExcessUncoreCaches:       nodeTopo.Cost(placements),
		LargestAlignableFreeCPUs: largestAlignableFreeCPUs(nodeTopo, nodeFree),
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

// planNodeReport fills in one node's dry-run plan. Called with applyMu released.
func (cp *CPUDriver) planNodeReport(logger logr.Logger, input dryRunInput) {
	plan, err := defrag.PlanNode(input.topology, input.placements, input.free, cp.defragSelector(logger, input.threadsPerCore), defrag.Options{
		Eligible:             input.movable,
		KeepFreePoolNonEmpty: input.keepFreePool,
	})
	if err != nil {
		input.report.Plan = &planReport{Reason: "cannot plan: " + err.Error()}
		return
	}
	input.report.Plan = &planReport{
		Moves:       make([]moveReport, 0, len(plan.Moves)),
		CurrentCost: plan.CurrentCost,
		IdealCost:   plan.IdealCost,
		Blocked:     plan.Blocked,
		Reason:      plan.Reason,
	}
	for _, move := range plan.Moves {
		input.report.Plan.Moves = append(input.report.Plan.Moves, moveReport{
			ClaimUID: string(move.ClaimUID),
			From:     move.From.String(),
			To:       move.To.String(),
		})
	}
}
