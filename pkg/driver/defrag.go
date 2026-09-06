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
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// defragOptions is the pass configuration, fixed at startup.
type defragOptions struct {
	enabled bool
}

// defragRound is one NUMA node's set of moves that has been reserved and written
// to disk and is waiting for the runtime to confirm it.
type defragRound struct {
	numaNodeID int
	moves      []defrag.Move
	updates    []*api.ContainerUpdate
	// store is the allocation store the reservations live in. Synchronize
	// replaces it wholesale, which discards them.
	store *store.CPUAllocation
}

// defragPass moves claims towards the best placement the node's topology allows,
// one round per NUMA node.
//
// Planning per node is what bounds a batch: a round disturbs the claims of one
// NUMA node and no more, and a node whose round the runtime has not settled
// holds up nothing but itself.
func (cp *CPUDriver) defragPass(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	online, ok := cp.defragOnlineCPUs(logger)
	if !ok {
		return
	}

	start := time.Now()
	cp.observeNodeShape(logger, online)
	result := cpumetrics.ResultSuccess
	for _, numaNodeID := range cp.topology.cpuTopology.CPUDetails.NUMANodes().List() {
		if cp.defragNode(ctx, numaNodeID, online) == cpumetrics.ResultError {
			result = cpumetrics.ResultError
		}
	}
	cp.metricsRecorder().RecordDefragPass(result, time.Since(start))
}

// defragRetryPass runs a round on one NUMA node alone.
func (cp *CPUDriver) defragRetryPass(ctx context.Context, numaNodeID int) {
	logger := ctxlog.FromContext(ctx)
	online, ok := cp.defragOnlineCPUs(logger)
	if !ok {
		return
	}

	start := time.Now()
	result := cp.defragNode(ctx, numaNodeID, online)
	cp.metricsRecorder().RecordDefragPass(result, time.Since(start))
}

// defragOnlineCPUs reports the CPUs a pass may place on, and whether there is
// any point running one at all.
//
// A CPU that went offline since startup cannot be moved onto: the kernel refuses
// a cpuset naming it. The driver reads the online set once in New, so a pass
// reads it again for itself.
func (cp *CPUDriver) defragOnlineCPUs(logger logr.Logger) (cpuset.CPUSet, bool) {
	if !cp.defrag.enabled || cp.containerUpdater == nil {
		return cpuset.New(), false
	}
	online, err := cp.currentOnlineCPUs(logger)
	if err != nil {
		logger.Error(err, "skipping defragmentation pass: cannot read online CPUs")
		return cpuset.New(), false
	}
	return online, true
}

// defragNode plans, applies and settles one NUMA node's moves.
//
// The work is split around a single call into the runtime, because applyMu may
// not be held across one: reserve and record under the lock, update the
// containers with it released, then confirm or undo under it again.
func (cp *CPUDriver) defragNode(ctx context.Context, numaNodeID int, online cpuset.CPUSet) cpumetrics.Result {
	logger := ctxlog.FromContext(ctx).WithValues("numaNode", numaNodeID)
	round := cp.beginDefragRound(logger, numaNodeID, online)
	if round == nil {
		return cpumetrics.ResultSuccess
	}

	logger.V(2).Info("applying defragmentation moves", "numMoves", len(round.moves), "numUpdates", len(round.updates))
	var failed []*api.ContainerUpdate
	var updateErr error
	if len(round.updates) > 0 {
		failed, updateErr = cp.containerUpdater.UpdateContainers(round.updates)
	}
	// An empty round means nothing is running on the CPUs involved, so the store
	// and the specs are the whole of the move.
	return cp.finishDefragRound(logger, round, failed, updateErr)
}

// observeNodeShape republishes how well placed the node's claims are and how
// large a claim each NUMA node could still take unsplit.
//
// It measures rather than plans, so every NUMA node is reported on every pass,
// including one whose round is still unsettled. The gauges are replaced
// wholesale, which is why they are taken together and not one round at a time.
func (cp *CPUDriver) observeNodeShape(logger logr.Logger, online cpuset.CPUSet) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	// Every node the topology has, not only those holding a claim. A node with no
	// claims has nothing to move, but it still has a shape worth reporting: how
	// large a claim it could take aligned is most interesting precisely when it is
	// empty, and a gauge that disappears as a node drains cannot be alerted on.
	state := cpumetrics.DefragState{LargestAlignableFreeCPUs: map[int]int{}}
	for _, numaNodeID := range cp.topology.cpuTopology.CPUDetails.NUMANodes().List() {
		view, ok := cp.defragView(logger.WithValues("numaNode", numaNodeID), numaNodeID, online)
		if !ok {
			continue
		}
		state.ExcessUncoreCaches += view.topology.Cost(view.placements)
		state.LargestAlignableFreeCPUs[numaNodeID] = largestAlignableFreeCPUs(view.topology, view.free)
	}
	cp.metricsRecorder().SetDefragState(state)
}

func (cp *CPUDriver) currentOnlineCPUs(logger logr.Logger) (cpuset.CPUSet, error) {
	if cp.sysfs == nil {
		return cpuset.New(), fmt.Errorf("no sysfs to read them from")
	}
	return cpuinfo.OnlineCPUs(logger, cp.sysfs)
}

// beginDefragRound plans one NUMA node's moves, reserves them, and records each
// claim's new placement in its CDI spec. It returns nil when there is nothing to
// do.
func (cp *CPUDriver) beginDefragRound(logger logr.Logger, numaNodeID int, online cpuset.CPUSet) *defragRound {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if round := cp.takePendingRound(logger, numaNodeID); round != nil {
		return round
	}

	moves := cp.planNodeMoves(logger, numaNodeID, online)
	if len(moves) == 0 {
		return nil
	}

	round := &defragRound{numaNodeID: numaNodeID, store: cp.cpuAllocationStore}
	for _, move := range moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		if err := cp.cpuAllocationStore.BeginRebind(mLogger, move.ClaimUID, move.To); err != nil {
			mLogger.Error(err, "cannot start moving claim")
			continue
		}
		// The spec on disk is the desired placement, and it is what a driver
		// restart rebuilds the store from, so it has to name the target before
		// the container is told about it.
		if err := cp.writeClaimPlacement(mLogger, move.ClaimUID); err != nil {
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

// takePendingRound returns the round a previous attempt could not confirm, so it
// can be sent again. Cpuset updates are idempotent, and until one is confirmed
// its claims hold both their old and their new CPUs, so re-sending is the only
// way to find out what happened without guessing.
func (cp *CPUDriver) takePendingRound(logger logr.Logger, numaNodeID int) *defragRound {
	round := cp.pendingRounds[numaNodeID]
	if round == nil {
		return nil
	}
	if round.store != cp.cpuAllocationStore {
		// Synchronize rebuilt the stores from the specs on disk, which already
		// name the targets, so this round's reservations are gone and its claims
		// are recorded where it was taking them.
		logger.V(2).Info("dropping an unconfirmed defragmentation round: the stores were rebuilt")
		delete(cp.pendingRounds, numaNodeID)
		return nil
	}
	logger.V(2).Info("retrying an unconfirmed defragmentation round", "numMoves", len(round.moves))
	return round
}

// defragNodeView is one NUMA node as both planning and measuring see it.
type defragNodeView struct {
	topology   *defrag.Topology
	free       cpuset.CPUSet
	placements []defrag.Placement
}

// defragView builds that view, or reports that the node cannot be reasoned
// about. Called with applyMu held.
func (cp *CPUDriver) defragView(logger logr.Logger, numaNodeID int, online cpuset.CPUSet) (defragNodeView, bool) {
	topo := cp.topology.cpuTopology
	allocatable := online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)

	nodeTopo, err := defrag.NewTopology(topo, numaNodeID, allocatable)
	if err != nil {
		logger.V(2).Info("node cannot be defragmented", "reason", err.Error())
		return defragNodeView{}, false
	}

	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(allocatable)
	if cp.fullPhysicalCPUsOnly {
		// A half-free core cannot take a whole-core claim, so it is not free for
		// this purpose. A promise about the free pool as a whole, so this only
		// needs to know the option is requested, not any one device's step.
		free = topo.CPUDetails.CompleteCores(free)
	}
	placements := defrag.PlacementsByNUMANode(topo, cp.cpuAllocationStore.ExclusiveClaimAllocations())
	return defragNodeView{topology: nodeTopo, free: free, placements: placements[numaNodeID]}, true
}

// planNodeMoves plans one NUMA node. Called with applyMu held.
func (cp *CPUDriver) planNodeMoves(logger logr.Logger, numaNodeID int, online cpuset.CPUSet) []defrag.Move {
	view, ok := cp.defragView(logger, numaNodeID, online)
	if !ok {
		return nil
	}

	plan, err := defrag.PlanNode(view.topology, view.placements, view.free, cp.defragSelector(logger, cp.topology.numaNodeThreadsPerCore[numaNodeID]), defrag.Options{
		Eligible: cp.claimMovable,
		// While a move is in flight its claim holds both its old and its new
		// CPUs, so a round that took every free CPU would leave the shared pool
		// momentarily empty, which NRI cannot express.
		KeepFreePoolNonEmpty: len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0,
	})
	if err != nil {
		logger.Error(err, "cannot plan defragmentation")
		return nil
	}
	logger.V(4).Info("planned defragmentation", "numMoves", len(plan.Moves), "blocked", plan.Blocked,
		"currentCost", plan.CurrentCost, "idealCost", plan.IdealCost, "reason", plan.Reason)
	cp.metricsRecorder().RecordDefragBlockedMoves(plan.Blocked)
	return plan.Moves
}

// largestAlignableFreeCPUs is the most CPUs still free inside a single uncore
// cache of a node, which is the largest claim it could take without splitting it.
func largestAlignableFreeCPUs(nodeTopo *defrag.Topology, free cpuset.CPUSet) int {
	largest := 0
	for _, cacheID := range nodeTopo.Caches() {
		if inCache := nodeTopo.CPUsInCache(cacheID).Intersection(free).Size(); inCache > largest {
			largest = inCache
		}
	}
	return largest
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
	// A move changes the CPUs under a running workload, and only the workload
	// knows whether it survives that, so nothing is moved that has not said so.
	if !cp.cpuAllocationStore.IsRelocatable(claimUID) {
		return false
	}
	_, inFlight := cp.cpuAllocationStore.GetRebindOrigin(claimUID)
	return !inFlight
}

// writeClaimPlacement rewrites a claim's CDI spec to record where each of its
// requests now belongs, as the store has it.
//
// The environment edit is reconstructed rather than read back from the spec. It
// is a pure function of the claim UID and whether placement is mutable, so
// rebuilding it is exact -- and reading it would mean querying the CDI cache,
// which only learns of a spec when it is refreshed and so cannot be relied on to
// know about a claim this driver prepared itself.
func (cp *CPUDriver) writeClaimPlacement(logger logr.Logger, claimUID types.UID) error {
	record, ok := cp.cpuAllocationStore.GetClaimRecord(claimUID)
	if !ok {
		return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
	}
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cp.cdiEnvValue(record))
	return cp.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID), envVar, record)
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
func (cp *CPUDriver) finishDefragRound(logger logr.Logger, round *defragRound, failed []*api.ContainerUpdate, updateErr error) cpumetrics.Result {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if round.store != cp.cpuAllocationStore {
		// Synchronize rebuilt the stores from the specs while the round was out.
		// Those name the targets, so the claims are already recorded there and
		// Synchronize converges their containers itself.
		logger.V(2).Info("defragmentation round outlived its stores", "numMoves", len(round.moves))
		delete(cp.pendingRounds, round.numaNodeID)
		cp.forgetDefragRetry(round.numaNodeID)
		return cpumetrics.ResultSuccess
	}
	if updateErr != nil {
		// Nothing can be concluded from a failed call: some of the batch may have
		// been applied. Keep holding both halves of every move and send the same
		// round again rather than release CPUs a container may now be running on.
		logger.Error(updateErr, "defragmentation round unconfirmed, will retry", "numMoves", len(round.moves))
		cp.pendingRounds[round.numaNodeID] = round
		cp.retryDefragNode(round.numaNodeID)
		return cpumetrics.ResultError
	}
	delete(cp.pendingRounds, round.numaNodeID)

	refused := map[types.UID]struct{}{}
	for _, update := range failed {
		refused[types.UID(update.GetContainerId())] = struct{}{}
	}

	committed, reverted := 0, 0
	for _, move := range round.moves {
		mLogger := logger.WithValues("claimUID", move.ClaimUID)
		if cp.moveWasRefused(move, refused) {
			mLogger.Info("runtime refused a move, leaving the claim where it is",
				"from", move.From.String(), "to", move.To.String())
			reverted++
			if err := cp.cpuAllocationStore.AbortRebind(mLogger, move.ClaimUID); err != nil {
				mLogger.Error(err, "cannot undo the reservation")
				continue
			}
			if err := cp.writeClaimPlacement(mLogger, move.ClaimUID); err != nil {
				mLogger.Error(err, "cannot restore the recorded placement")
			}
			continue
		}
		if err := cp.cpuAllocationStore.CommitRebind(mLogger, move.ClaimUID); err != nil {
			mLogger.Error(err, "cannot complete the move")
			reverted++
			continue
		}
		committed++
	}
	cp.metricsRecorder().RecordDefragMoves(cpumetrics.ResultSuccess, committed)
	cp.metricsRecorder().RecordDefragMoves(cpumetrics.ResultError, reverted)

	if committed > 0 {
		// Two jobs at once. The CPUs the moved claims left are back in the pool
		// and the shared containers entitled to them are still on the narrower
		// mask; and a target vacated by this round is not available until it
		// commits, so the moves it made possible are the next pass's to make.
		cp.requestReconcile()
	}
	if reverted > 0 {
		// The runtime declined a move the plan still wants, so the node is not
		// settled and nothing else is going to look at it.
		cp.retryDefragNode(round.numaNodeID)
		return cpumetrics.ResultError
	}
	cp.forgetDefragRetry(round.numaNodeID)
	return cpumetrics.ResultSuccess
}

// retryDefragNode asks for another attempt at a NUMA node the runtime left
// unsettled, after however long the rate limiter has decided this node's failures
// are worth.
func (cp *CPUDriver) retryDefragNode(numaNodeID int) {
	cp.defragRetries.AddRateLimited(numaNodeID)
}

// forgetDefragRetry clears a NUMA node's accumulated backoff, so a node that
// settles now starts from the shortest delay if it ever fails again. It does not
// withdraw an attempt the rate limiter has already scheduled.
func (cp *CPUDriver) forgetDefragRetry(numaNodeID int) {
	cp.defragRetries.Forget(numaNodeID)
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
		if err := cp.writeClaimPlacement(mLogger, move.ClaimUID); err != nil {
			mLogger.Error(err, "cannot restore the recorded placement")
		}
	}
}
