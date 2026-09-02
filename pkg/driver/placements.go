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
	"time"

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
	// SharedPoolCPUs is the static shared pool, empty when the pool is dynamic.
	SharedPoolCPUs string `json:"sharedPoolCPUs,omitempty"`
	// SharedCPUs is what a container without a claim is confined to: the static
	// pool when one is configured, otherwise whatever the claims have left over.
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
	// MovableIn is how long until the claim's cooldown expires, omitted when it
	// may be moved now.
	MovableIn string `json:"movableIn,omitempty"`
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

// placements builds the report from one consistent snapshot.
func (cp *CPUDriver) placements(logger logr.Logger, dryRun bool) (*placementsReport, error) {
	online, err := cp.currentOnlineCPUs(logger)
	if err != nil {
		return nil, err
	}

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	topo := cp.topology.cpuTopology
	allocatable := online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)
	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	if cp.wholeCoreStep > 1 {
		free = topo.CPUDetails.CompleteCores(free)
	}
	allocations := cp.cpuAllocationStore.ResourceClaimAllocations()

	report := &placementsReport{
		NodeName:      cp.nodeName,
		DefragEnabled: cp.defrag.enabled,
		// The truly reserved CPUs: the pool is folded into the effective
		// reserved set internally, but to a reader they are opposites -- one
		// hosts every shared workload, the other none.
		ReservedCPUs:   cp.topology.reservedCPUs.Difference(cp.sharedPool).String(),
		SharedPoolCPUs: cp.sharedPool.String(),
		SharedCPUs:     cp.sharedContainerCPUs().String(),
		Claims:         cp.claimReports(allocations),
	}

	placements := defrag.PlacementsByNUMANode(topo, allocations)
	for _, numaNodeID := range topo.CPUDetails.NUMANodes().List() {
		nodeTopo, err := defrag.NewTopology(topo, numaNodeID, allocatable)
		if err != nil {
			report.Unmeasurable = append(report.Unmeasurable, unmeasurableNode{
				NUMANodeID: numaNodeID,
				Reason:     err.Error(),
			})
			continue
		}
		report.NUMANodes = append(report.NUMANodes, cp.numaNodeReport(logger, nodeTopo, placements[numaNodeID], free, dryRun))
	}
	return report, nil
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
		if cp.defrag.claimCooldown > 0 {
			if movedAt, ok := cp.lastMoved[claimUID]; ok {
				if remaining := cp.defrag.claimCooldown - time.Since(movedAt); remaining > 0 {
					report.MovableIn = remaining.Round(time.Second).String()
				}
			}
		}
		reports = append(reports, report)
	}
	return reports
}

func (cp *CPUDriver) numaNodeReport(logger logr.Logger, nodeTopo *defrag.Topology, placements []defrag.Placement, free cpuset.CPUSet, dryRun bool) numaNodeReport {
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
	if !dryRun {
		return report
	}

	plan, err := defrag.PlanNode(nodeTopo, placements, nodeFree, cp.defragSelector(logger), defrag.Options{
		MaxMoves:             cp.defrag.maxMoves,
		MinGain:              cp.defrag.minGain,
		Eligible:             cp.claimMovable,
		KeepFreePoolNonEmpty: cp.keepFreePoolNonEmpty(),
	})
	if err != nil {
		report.Plan = &planReport{Reason: "cannot plan: " + err.Error()}
		return report
	}
	report.Plan = &planReport{
		Moves:       make([]moveReport, 0, len(plan.Moves)),
		CurrentCost: plan.CurrentCost,
		IdealCost:   plan.IdealCost,
		Blocked:     plan.Blocked,
		Reason:      plan.Reason,
	}
	for _, move := range plan.Moves {
		report.Plan.Moves = append(report.Plan.Moves, moveReport{
			ClaimUID: string(move.ClaimUID),
			From:     move.From.String(),
			To:       move.To.String(),
		})
	}
	return report
}
