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
	"sort"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// defragOptions is the pass configuration, fixed at startup.
type defragOptions struct {
	enabled       bool
	interval      time.Duration
	maxMoves      int
	minGain       int
	claimCooldown time.Duration
}

// defragRound is one set of moves that has been reserved and written to disk and
// is waiting for the runtime to confirm it.
type defragRound struct {
	moves   []defrag.Move
	updates []*api.ContainerUpdate
	// store is the allocation store the reservations live in. Synchronize
	// replaces it wholesale, which discards them.
	store *store.CPUAllocation
}

// defragPass moves claims towards the best placement the node's topology allows.
//
// The work is split around a single call into the runtime, because applyMu may
// not be held across one: reserve and record under the lock, update the
// containers with it released, then confirm or undo under it again.
func (cp *CPUDriver) defragPass(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	if !cp.defrag.enabled || cp.containerUpdater == nil {
		return
	}

	// A CPU that went offline since startup cannot be moved onto: the kernel
	// refuses a cpuset naming it. The driver reads the online set once in New,
	// so a pass reads it again for itself.
	online, err := cp.currentOnlineCPUs(logger)
	if err != nil {
		logger.Error(err, "skipping defragmentation pass: cannot read online CPUs")
		return
	}

	round := cp.beginDefragRound(logger, online)
	if round == nil {
		return
	}

	logger.V(2).Info("applying defragmentation moves", "numMoves", len(round.moves), "numUpdates", len(round.updates))
	if len(round.updates) == 0 {
		// Nothing is running on the CPUs involved, so the store and the specs are
		// the whole of the move and there is nothing to ask the runtime.
		cp.finishDefragRound(logger, round, nil, nil)
		return
	}
	failed, err := cp.containerUpdater.UpdateContainers(round.updates)
	cp.finishDefragRound(logger, round, failed, err)
}

func (cp *CPUDriver) currentOnlineCPUs(logger logr.Logger) (cpuset.CPUSet, error) {
	if cp.sysfs == nil {
		return cpuset.New(), fmt.Errorf("no sysfs to read them from")
	}
	return cpuinfo.OnlineCPUs(logger, cp.sysfs)
}

// beginDefragRound plans the moves, reserves them, and records each claim's new
// placement in its CDI spec. It returns nil when there is nothing to do.
func (cp *CPUDriver) beginDefragRound(logger logr.Logger, online cpuset.CPUSet) *defragRound {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	// A cooldown entry says nothing once its window has lapsed, and claim UIDs
	// never repeat, so entries left behind would accumulate for every claim ever
	// moved for as long as the driver runs. Prune here, under the same lock that
	// guards every other reader.
	for claimUID, movedAt := range cp.lastMoved {
		if time.Since(movedAt) >= cp.defrag.claimCooldown {
			delete(cp.lastMoved, claimUID)
		}
	}

	if round := cp.takePendingRound(logger); round != nil {
		return round
	}

	moves := cp.planDefragMoves(logger, online)
	if len(moves) == 0 {
		return nil
	}

	round := &defragRound{store: cp.cpuAllocationStore}
	for _, move := range moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		if err := cp.cpuAllocationStore.BeginRebind(mLogger, move.ClaimUID, move.To); err != nil {
			mLogger.Error(err, "cannot start moving claim")
			continue
		}
		// The spec on disk is the desired placement, and it is what a driver
		// restart rebuilds the store from, so it has to name the target before
		// the container is told about it.
		if err := cp.writeClaimPlacement(mLogger, move.ClaimUID, move.To); err != nil {
			mLogger.Error(err, "cannot record new placement, leaving claim where it is", "to", move.To.String())
			if abortErr := cp.cpuAllocationStore.AbortRebind(mLogger, move.ClaimUID); abortErr != nil {
				mLogger.Error(abortErr, "cannot undo the reservation either")
			}
			continue
		}
		round.moves = append(round.moves, move)
	}
	if len(round.moves) == 0 {
		return nil
	}

	updates, err := cp.roundUpdates(logger, round.moves)
	if err != nil {
		// Most likely the shared pool cannot be narrowed any further. Undo
		// everything rather than move a claim onto CPUs a shared container still
		// holds.
		logger.Error(err, "abandoning defragmentation round", "numMoves", len(round.moves))
		cp.abortMoves(logger, round.moves)
		return nil
	}
	round.updates = updates
	return round
}

// takePendingRound returns the round a previous pass could not confirm, so it can
// be sent again. Cpuset updates are idempotent, and until one is confirmed its
// claims hold both their old and their new CPUs, so re-sending is the only way to
// find out what happened without guessing.
func (cp *CPUDriver) takePendingRound(logger logr.Logger) *defragRound {
	if cp.pendingRound == nil {
		return nil
	}
	if cp.pendingRound.store != cp.cpuAllocationStore {
		// Synchronize rebuilt the stores from the specs on disk, which already
		// name the targets, so this round's reservations are gone and its claims
		// are recorded where it was taking them.
		logger.V(2).Info("dropping an unconfirmed defragmentation round: the stores were rebuilt")
		cp.pendingRound = nil
		return nil
	}
	logger.V(2).Info("retrying an unconfirmed defragmentation round", "numMoves", len(cp.pendingRound.moves))
	return cp.pendingRound
}

// planDefragMoves plans each NUMA node in turn, sharing one move budget between
// them. Called with applyMu held.
func (cp *CPUDriver) planDefragMoves(logger logr.Logger, online cpuset.CPUSet) []defrag.Move {
	topo := cp.topology.cpuTopology
	allocatable := online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)

	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	if cp.fullPhysicalCPUsOnly {
		// A half-free core cannot take a whole-core claim, so it is not free for
		// this purpose. A promise about the free pool as a whole, so this only
		// needs to know the option is requested, not any one device's step.
		free = topo.CPUDetails.CompleteCores(free)
	}

	// While a move is in flight its claim holds both its old and its new CPUs, so
	// a round that took every free CPU would leave the shared pool momentarily
	// empty, which NRI cannot express.
	keepFree := len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0

	placements := defrag.PlacementsByNUMANode(topo, cp.cpuAllocationStore.ResourceClaimAllocations())
	numaNodeIDs := make([]int, 0, len(placements))
	for numaNodeID := range placements {
		numaNodeIDs = append(numaNodeIDs, numaNodeID)
	}
	sort.Ints(numaNodeIDs)

	budget := cp.defrag.maxMoves
	var moves []defrag.Move
	for _, numaNodeID := range numaNodeIDs {
		if budget <= 0 {
			break
		}
		nLogger := logger.WithValues("numaNode", numaNodeID)
		nodeTopo, err := defrag.NewTopology(topo, numaNodeID, allocatable)
		if err != nil {
			nLogger.V(2).Info("node cannot be defragmented", "reason", err.Error())
			continue
		}
		plan, err := defrag.PlanNode(nodeTopo, placements[numaNodeID], free, cp.defragSelector(nLogger, cp.topology.numaNodeThreadsPerCore[numaNodeID]), defrag.Options{
			MaxMoves:             budget,
			MinGain:              cp.defrag.minGain,
			Eligible:             cp.claimMovable,
			KeepFreePoolNonEmpty: keepFree,
		})
		if err != nil {
			nLogger.Error(err, "cannot plan defragmentation")
			continue
		}
		nLogger.V(4).Info("planned defragmentation", "numMoves", len(plan.Moves), "blocked", plan.Blocked,
			"currentCost", plan.CurrentCost, "idealCost", plan.IdealCost, "reason", plan.Reason)
		moves = append(moves, plan.Moves...)
		budget -= len(plan.Moves)
	}
	return moves
}

// defragSelector is the allocator Prepare places a new claim with, so a move can
// only ever propose a placement the driver would have chosen itself.
// threadsPerCore is the effective allocation step of the one device this
// selector will be asked about -- the caller resolves it once, since a
// Selector call carries no device identity of its own.
func (cp *CPUDriver) defragSelector(logger logr.Logger, threadsPerCore int) defrag.Selector {
	return func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		return cp.takeCPUsForDevice(logger, cp.topology.cpuTopology, available, numCPUs, threadsPerCore)
	}
}

// claimMovable reports whether a claim may be moved now. Called with applyMu held.
func (cp *CPUDriver) claimMovable(claimUID types.UID) bool {
	if _, inFlight := cp.cpuAllocationStore.GetRebindOrigin(claimUID); inFlight {
		return false
	}
	if cp.defrag.claimCooldown <= 0 {
		return true
	}
	movedAt, ok := cp.lastMoved[claimUID]
	return !ok || time.Since(movedAt) >= cp.defrag.claimCooldown
}

// writeClaimPlacement rewrites a claim's CDI spec to record where it now belongs.
//
// The environment edit is reconstructed rather than read back from the spec. It
// is a pure function of the claim UID and whether placement is mutable, so
// rebuilding it is exact -- and reading it would mean querying the CDI cache,
// which only learns of a spec when it is refreshed and so cannot be relied on to
// know about a claim this driver prepared itself.
func (cp *CPUDriver) writeClaimPlacement(logger logr.Logger, claimUID types.UID, cpus cpuset.CPUSet) error {
	deviceName := getCDIDeviceName(claimUID)
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cp.cdiEnvValue(cpus))
	return cp.cdiMgr.AddDevice(logger, deviceName, envVar, cpus)
}

// roundUpdates builds the one batch of container updates a round consists of: the
// containers holding moved claims, each pinned to the union of all its claims,
// plus the shared containers that have to vacate the CPUs the moves are taking.
//
// A moved claim with no running container needs no update at all; the store and
// its spec are the whole of its state until a container is created from them.
func (cp *CPUDriver) roundUpdates(logger logr.Logger, moves []defrag.Move) ([]*api.ContainerUpdate, error) {
	var updates []*api.ContainerUpdate
	covered := map[types.UID]struct{}{}

	for _, move := range moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		owner, ok := cp.claimTracker.Owner(move.ClaimUID)
		if !ok {
			mLogger.V(2).Info("moved claim has no container yet")
			continue
		}
		state := cp.podConfigStore.GetContainerState(owner.PodUID, owner.ContainerName)
		if state == nil {
			mLogger.V(2).Info("moved claim's container is not running")
			continue
		}
		containerUID := state.ContainerUID()
		if _, done := covered[containerUID]; done {
			continue
		}
		covered[containerUID] = struct{}{}

		// A container holding several claims must be pinned to all of them at
		// once, moved or not.
		cpus, err := cp.cpuAllocationStore.GetResourceClaimAllocationUnion(state.ClaimUIDs()...)
		if err != nil {
			return nil, fmt.Errorf("cannot determine CPUs for container %q: %w", containerUID, err)
		}
		update := &api.ContainerUpdate{ContainerId: string(containerUID)}
		update.SetLinuxCPUSetCPUs(cpus.String())
		updates = append(updates, update)
	}

	// The pool is already narrowed by the reservations, so this moves shared
	// containers off the targets in the same batch. They are widened again once
	// the moves commit and the origins return to the pool.
	shared, err := cp.getSharedContainerUpdates(logger, types.UID(""))
	if err != nil {
		return nil, err
	}
	return append(updates, shared...), nil
}

// finishDefragRound settles every move in a round according to what the runtime
// reported.
func (cp *CPUDriver) finishDefragRound(logger logr.Logger, round *defragRound, failed []*api.ContainerUpdate, updateErr error) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if round.store != cp.cpuAllocationStore {
		// Synchronize rebuilt the stores from the specs while the round was out.
		// Those name the targets, so the claims are already recorded there and
		// Synchronize converges their containers itself.
		logger.V(2).Info("defragmentation round outlived its stores", "numMoves", len(round.moves))
		cp.pendingRound = nil
		return
	}
	if updateErr != nil {
		// Nothing can be concluded from a failed call: some of the batch may have
		// been applied. Keep holding both halves of every move and send the same
		// round again rather than release CPUs a container may now be running on.
		logger.Error(updateErr, "defragmentation round unconfirmed, will retry", "numMoves", len(round.moves))
		cp.pendingRound = round
		return
	}
	cp.pendingRound = nil

	refused := map[types.UID]struct{}{}
	for _, update := range failed {
		refused[types.UID(update.GetContainerId())] = struct{}{}
	}

	committed := 0
	for _, move := range round.moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		if cp.moveWasRefused(move, refused) {
			mLogger.Info("runtime refused a move, leaving the claim where it is",
				"from", move.From.String(), "to", move.To.String())
			if err := cp.cpuAllocationStore.AbortRebind(mLogger, move.ClaimUID); err != nil {
				mLogger.Error(err, "cannot undo the reservation")
				continue
			}
			if err := cp.writeClaimPlacement(mLogger, move.ClaimUID, move.From); err != nil {
				mLogger.Error(err, "cannot restore the recorded placement")
			}
			continue
		}
		if err := cp.cpuAllocationStore.CommitRebind(mLogger, move.ClaimUID); err != nil {
			mLogger.Error(err, "cannot complete the move")
			continue
		}
		cp.lastMoved[move.ClaimUID] = time.Now()
		committed++
	}

	if committed > 0 {
		// The CPUs the moved claims left are back in the pool; the shared
		// containers entitled to them are still on the narrower mask.
		cp.requestReconcile()
	}
}

// moveWasRefused reports whether the runtime declined to move a claim's
// container.
//
// A container that has since gone from the store, or been replaced by one with a
// new runtime ID, counts as converged rather than refused: a container created
// after the reservation is pinned from the store, which already holds the target.
func (cp *CPUDriver) moveWasRefused(move defrag.Move, refused map[types.UID]struct{}) bool {
	owner, ok := cp.claimTracker.Owner(move.ClaimUID)
	if !ok {
		return false
	}
	state := cp.podConfigStore.GetContainerState(owner.PodUID, owner.ContainerName)
	if state == nil {
		return false
	}
	_, refusedIt := refused[state.ContainerUID()]
	return refusedIt
}

// abortMoves undoes reservations and recorded placements for moves that will not
// be attempted.
func (cp *CPUDriver) abortMoves(logger logr.Logger, moves []defrag.Move) {
	for _, move := range moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		if err := cp.cpuAllocationStore.AbortRebind(mLogger, move.ClaimUID); err != nil {
			mLogger.Error(err, "cannot undo the reservation")
			continue
		}
		if err := cp.writeClaimPlacement(mLogger, move.ClaimUID, move.From); err != nil {
			mLogger.Error(err, "cannot restore the recorded placement")
		}
	}
}
