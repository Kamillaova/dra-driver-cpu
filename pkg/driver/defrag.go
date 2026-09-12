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
	"slices"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpumanager"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// defaultDefragBatchTimeout is how long a round waits for the runtime to answer
// one batch of container updates.
//
// The NRI stub calls the runtime with a background context and no timeout of its
// own (UpdateContainers in github.com/containerd/nri/pkg/stub), so a runtime that
// never answers would otherwise hold the one goroutine that runs passes for the
// life of the process.
const defaultDefragBatchTimeout = 30 * time.Second

// defaultDefragPublishTimeout is how long a round waits for the capacity it
// shrank to reach the API server before giving up on itself.
//
// The publishing controller retries a rejected or failed write under its own
// rate limiter and resyncs on its informer's events, so a write that has not
// landed in a window this size is not about to. Waiting longer would hold a
// scope's reservations against a control plane that is not answering, which the
// retry queue does better: the round is abandoned and the scope is tried again
// with the backoff its own failures have earned.
const defaultDefragPublishTimeout = 30 * time.Second

// storedSlicePollInterval is how often a round asks whether the capacity it
// published has been stored. The informer answers from its cache, so this is
// the cost of a comparison rather than of a request.
const storedSlicePollInterval = 100 * time.Millisecond

// storedSliceSyncTimeout bounds the wait for the driver's own slice informer to
// fill at startup, the same order as the wait for the kubelet to register the
// plugin.
const storedSliceSyncTimeout = 30 * time.Second

// defragOptions is the pass configuration, fixed at startup.
type defragOptions struct {
	enabled bool
	// allowTransientOverlap permits exchanging the CPUs of two claims, and with
	// it the instant between the two container updates in which both of them sit
	// on the CPUs one is leaving.
	allowTransientOverlap bool
	batchTimeout          time.Duration
	publishTimeout        time.Duration
}

// defragScope is the region one round covers: one NUMA node of one CPU
// partition claims take CPUs of their own from.
//
// A move can never leave it, because nothing outside it is ever offered to the
// planner: the free CPUs it may take and the claims it may shuffle are both cut
// to the scope before planning starts. That is what keeps a dataplane
// partition's cores and a virtual machine partition's cores from being mixed by
// a repair, whichever of them a claim happens to need.
type defragScope struct {
	numaNodeID int
	partition  string
}

func (s defragScope) logValues() []any {
	return []any{"numaNode", s.numaNodeID, "partition", s.partition}
}

// exchangeOutcome is what became of one exchange once the runtime answered.
type exchangeOutcome int

const (
	// exchangeUnsettled is the state a round starts in and the one it stays in
	// when the driver cannot say where the exchange's containers are running.
	exchangeUnsettled exchangeOutcome = iota
	// exchangeApplied means every container of the exchange took its new cpuset.
	exchangeApplied
	// exchangeUndone means the half the runtime accepted was put back.
	exchangeUndone
)

// defragRound is one scope's set of moves that has been reserved and written
// to disk and is waiting for the runtime to confirm it.
type defragRound struct {
	id      string
	scope   defragScope
	moves   []defrag.Move
	updates []*api.ContainerUpdate
	// exchangeContainers is the containers each exchange's claims run in, so a
	// reply naming some of them can be attributed to the exchange it belongs to.
	exchangeContainers map[int][]types.UID
	// updateByContainer is this round's own update for each container it touches,
	// so a retry sends what the driver sent rather than what the runtime echoed.
	updateByContainer map[types.UID]*api.ContainerUpdate
	// claimsByContainer is every claim each of those containers holds, which is
	// what a rollback has to pin it back to.
	claimsByContainer map[types.UID][]types.UID
	// outcomes is what became of each exchange. An exchange missing from it, or
	// recorded as unsettled, is one the round cannot close.
	outcomes map[int]exchangeOutcome
	// replayed marks a round an earlier attempt left unsettled, whose holds are
	// therefore not this attempt's to release: its claims may be on either of the
	// two cpusets they hold and only a read-back may say which.
	replayed bool
	// store is the allocation store the reservations live in. Synchronize
	// replaces it wholesale, which discards them.
	store *store.CPUAllocation
}

func newDefragRound(scope defragScope, allocations *store.CPUAllocation) *defragRound {
	return &defragRound{
		id:                 generateShortID(opIDLen),
		scope:              scope,
		exchangeContainers: map[int][]types.UID{},
		updateByContainer:  map[types.UID]*api.ContainerUpdate{},
		claimsByContainer:  map[types.UID][]types.UID{},
		outcomes:           map[int]exchangeOutcome{},
		store:              allocations,
	}
}

// hasExchanges reports whether any of the round's moves is part of an exchange,
// which is what makes a partial reply something to act on rather than something
// to leave to the next pass.
func (r *defragRound) hasExchanges() bool {
	for _, move := range r.moves {
		if move.Exchange != 0 {
			return true
		}
	}
	return false
}

// defragSteps groups moves into the units that are applied and settled together:
// one move into free CPUs, or all the moves of one exchange.
func defragSteps(moves []defrag.Move) [][]defrag.Move {
	var steps [][]defrag.Move
	at := map[int]int{}
	for _, move := range moves {
		if move.Exchange == 0 {
			steps = append(steps, []defrag.Move{move})
			continue
		}
		if index, ok := at[move.Exchange]; ok {
			steps[index] = append(steps[index], move)
			continue
		}
		at[move.Exchange] = len(steps)
		steps = append(steps, []defrag.Move{move})
	}
	return steps
}

// stepClaims is the claims one step moves, in the order the step lists them.
func stepClaims(step []defrag.Move) []types.UID {
	claimUIDs := make([]types.UID, 0, len(step))
	for _, move := range step {
		claimUIDs = append(claimUIDs, move.ClaimUID)
	}
	return claimUIDs
}

// defragPass moves claims towards the best placement the node's topology allows,
// one round per NUMA node and partition.
//
// Planning per scope is what bounds a batch: a round disturbs the claims of one
// NUMA node of one partition and no more, and a scope whose round the runtime
// has not settled holds up nothing but itself.
func (cp *CPUDriver) defragPass(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	online, ok := cp.defragOnlineCPUs(logger)
	if !ok {
		return
	}

	start := time.Now()
	cp.observeNodeShape(logger, online)
	result := cpumetrics.ResultSuccess
	for _, scope := range cp.defragScopes(online) {
		if cp.runDefragRound(ctx, scope, online) == cpumetrics.ResultError {
			result = cpumetrics.ResultError
		}
	}
	cp.metrics.RecordDefragPass(result, time.Since(start))
}

// defragRetryPass runs a round on one scope alone.
func (cp *CPUDriver) defragRetryPass(ctx context.Context, scope defragScope) {
	logger := ctxlog.FromContext(ctx)
	online, ok := cp.defragOnlineCPUs(logger)
	if !ok {
		return
	}

	start := time.Now()
	result := cp.runDefragRound(ctx, scope, online)
	cp.metrics.RecordDefragPass(result, time.Since(start))
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

// runDefragRound plans, applies and settles one scope's moves.
//
// The work is split around a single call into the runtime, because applyMu may
// not be held across one: reserve and record under the lock, update the
// containers with it released, then confirm or undo under it again.
func (cp *CPUDriver) runDefragRound(ctx context.Context, scope defragScope, online cpuset.CPUSet) cpumetrics.Result {
	logger := ctxlog.FromContext(ctx).WithValues(scope.logValues()...)
	// A fenced node is asked first whether it can be reopened: the read-back is
	// what settles the exchange that fenced it, and until it does there is
	// nothing to plan there.
	cp.liftPoison(logger, scope.numaNodeID)
	round := cp.beginDefragRound(logger, scope, online)
	if round == nil {
		cp.republishStaleSlicesLocking(ctx)
		return cpumetrics.ResultSuccess
	}
	// The reservation already shrank the capacity of every device this round
	// moves a claim onto. Nothing may be told to take those CPUs until a
	// scheduler can see that they are gone.
	if err := cp.awaitStoredShrink(ctx, round); err != nil {
		return cp.abandonDefragRound(ctx, logger, round, err)
	}

	logger.V(2).Info("applying defragmentation moves", "numMoves", len(round.moves), "numUpdates", len(round.updates))
	// An empty round means nothing is running on the CPUs involved, so the store
	// and the specs are the whole of the move.
	start := time.Now()
	failed, updateErr := cp.sendDefragBatch(logger, round.updates)
	if updateErr == nil {
		if round.hasExchanges() {
			// The window in which the exchanged claims share CPUs is inside this
			// call, between the two writes the runtime applies in order, so its
			// duration is the tightest bound on the window a plugin can measure.
			// Only a call that answered bounds anything: one that ran out of time
			// would contribute the deadline, which is not a measurement of the
			// window but of the giving up.
			cp.metrics.RecordDefragSwapOverlap(time.Since(start))
		}
		cp.settleExchanges(logger, round, failed)
	}
	result := cp.finishDefragRound(logger, round, failed, updateErr)
	// The published devices carry which NUMA nodes are fenced, and this round may
	// have fenced one or reopened one.
	cp.republishStaleSlicesLocking(ctx)
	return result
}

// abandonDefragRound gives a round up because the capacity it depends on was
// not stored, and reports it as a failure so the scope is tried again.
//
// A round this attempt planned releases its holds: nothing has been asked of the
// runtime, so its claims are where they were and the reservations are the only
// thing to undo. A round replayed from an earlier attempt keeps them, because
// its outcome is unknown -- its claims hold both cpusets and only a read-back
// may settle them.
//
// Either way the controller is handed the current truth again: the capacity
// before the round for one that was released, and the same shrink for one still
// held, which the next attempt waits for again.
func (cp *CPUDriver) abandonDefragRound(ctx context.Context, logger logr.Logger, round *defragRound, cause error) cpumetrics.Result {
	logger.Error(cause, "not moving any claim: the capacity this round shrinks was not stored", "numMoves", len(round.moves))
	cp.metrics.RecordDefragUnpublishedRound()

	cp.applyMu.Lock()
	if !round.replayed && round.store == cp.cpuAllocationStore {
		cp.abortMoves(logger, round.moves)
	}
	cp.applyMu.Unlock()

	cp.retryDefragScope(round.scope)
	cp.republishStaleSlicesLocking(ctx)
	return cpumetrics.ResultError
}

// republishStaleSlicesLocking is republishStaleSlices for a caller that holds no
// lock. What it reads -- the fenced nodes, and what the last publication carried
// -- is guarded by applyMu, and a pass runs on its own goroutine beside the
// kubelet hooks that write them.
func (cp *CPUDriver) republishStaleSlicesLocking(ctx context.Context) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()
	cp.republishStaleSlices(ctx)
}

// sendDefragBatch pushes one batch of container updates and gives the runtime a
// deadline of its own to answer it.
//
// A batch that runs out of time is reported as an error rather than as a refusal,
// because a call that has not answered may still be applied: the round keeps both
// cpusets of everything it touches and is sent again, which is the only thing
// that can be concluded without guessing.
//
// Called with applyMu released.
func (cp *CPUDriver) sendDefragBatch(logger logr.Logger, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	if len(updates) == 0 {
		return nil, nil
	}
	type reply struct {
		failed []*api.ContainerUpdate
		err    error
	}
	// Buffered, so the goroutine finishes and is collected however long the
	// runtime takes to answer a call this one has already given up on.
	answered := make(chan reply, 1)
	go func() {
		failed, err := cp.containerUpdater.UpdateContainers(updates)
		answered <- reply{failed: failed, err: err}
	}()

	timer := time.NewTimer(cp.defrag.batchTimeout)
	defer timer.Stop()
	select {
	case r := <-answered:
		return r.failed, r.err
	case <-timer.C:
		logger.Error(nil, "the runtime did not answer a batch of container updates", "timeout", cp.defrag.batchTimeout, "numUpdates", len(updates))
		return nil, fmt.Errorf("the runtime did not answer within %s", cp.defrag.batchTimeout)
	}
}

// settleExchanges completes or undoes every exchange the runtime applied only in
// part, and records what became of each.
//
// A refused half is sent again first: finishing the exchange is better than
// undoing it, and the reservation still holds every CPU involved, so nothing has
// to be planned again. Only when the runtime refuses it a second time is the half
// it did apply put back. An exchange that ends neither way is left unsettled --
// two claims are then sharing CPUs and the driver cannot say which -- and only a
// read-back can close it.
//
// Called with applyMu released, because every attempt here is a call into the
// runtime.
func (cp *CPUDriver) settleExchanges(logger logr.Logger, round *defragRound, failed []*api.ContainerUpdate) {
	refused := map[types.UID]struct{}{}
	for _, update := range failed {
		refused[types.UID(update.GetContainerId())] = struct{}{}
	}

	for _, step := range defragSteps(round.moves) {
		exchange := step[0].Exchange
		if exchange == 0 {
			continue
		}
		eLogger := logger.WithValues("exchange", exchange, "claimUIDs", stepClaims(step))
		containers := round.exchangeContainers[exchange]
		notApplied := map[types.UID]struct{}{}
		refusedHere := make([]*api.ContainerUpdate, 0, len(containers))
		for _, containerUID := range containers {
			if _, ok := refused[containerUID]; !ok {
				continue
			}
			if !cp.containerIsCurrent(round, containerUID) {
				// Gone, or replaced by one with a new runtime ID. It is refusing
				// nothing: a container created after the reservation is pinned
				// from the store, which already holds the target.
				eLogger.V(2).Info("a refused container is no longer the one its pod runs", "containerID", containerUID)
				continue
			}
			notApplied[containerUID] = struct{}{}
			refusedHere = append(refusedHere, round.updateByContainer[containerUID])
		}
		switch len(refusedHere) {
		case 0:
			round.outcomes[exchange] = exchangeApplied
			continue
		case len(containers):
			// Not one of the exchange's containers moved, so there is nothing to
			// put back and the claims can go straight back to where they are.
			eLogger.Info("runtime refused an exchange outright, leaving both claims where they are")
			round.outcomes[exchange] = exchangeUndone
			continue
		}

		cp.metrics.RecordDefragPartialBatch()
		eLogger.Info("runtime applied an exchange in part, sending the rest again", "numRefused", len(refusedHere))
		stillRefused, err := cp.sendDefragBatch(eLogger, refusedHere)
		switch {
		case err != nil:
			// The retry may have been applied before it stopped answering, so the
			// exchange is in neither state as far as this driver knows and undoing
			// the other half could be undoing a completed exchange. Only a
			// read-back can say.
			eLogger.Error(err, "the retry of a half-applied exchange was not answered")
			round.outcomes[exchange] = exchangeUnsettled
		case len(stillRefused) == 0:
			round.outcomes[exchange] = exchangeApplied
		default:
			round.outcomes[exchange] = cp.rollBackExchange(eLogger, round, exchange, notApplied)
		}
	}
}

// containerIsCurrent reports whether a container this round addressed is still
// the one its pod runs, which is what tells a refusal from a container that has
// simply been replaced since the batch went out.
//
// Called with applyMu released: the two stores it reads take their own locks, and
// the answer is a snapshot either way.
func (cp *CPUDriver) containerIsCurrent(round *defragRound, containerUID types.UID) bool {
	claimUIDs := round.claimsByContainer[containerUID]
	if len(claimUIDs) == 0 {
		return false
	}
	owner, ok := cp.claimTracker.Owner(claimUIDs[0])
	if !ok {
		return false
	}
	state := cp.podConfigStore.GetContainerState(owner.PodUID, owner.ContainerName)
	return state != nil && state.ContainerUID() == containerUID
}

// rollBackExchange puts the containers the runtime did move back on the CPUs
// they were running on before the exchange, and reports whether that leaves the
// exchange undone or unsettled. notApplied is the containers that refused it, so
// everything else in the exchange is what has to go back.
func (cp *CPUDriver) rollBackExchange(logger logr.Logger, round *defragRound, exchange int, notApplied map[types.UID]struct{}) exchangeOutcome {
	updates, err := cp.rollbackUpdates(round, exchange, notApplied)
	if err != nil {
		logger.Error(err, "cannot build the batch that would undo a half-applied exchange")
		cp.metrics.RecordDefragRollback(cpumetrics.ResultError)
		return exchangeUnsettled
	}
	stillRefused, err := cp.sendDefragBatch(logger, updates)
	if err != nil || len(stillRefused) > 0 {
		if err == nil {
			err = fmt.Errorf("the runtime refused %d of %d updates", len(stillRefused), len(updates))
		}
		logger.Error(err, "cannot undo a half-applied exchange: two claims now share CPUs")
		cp.metrics.RecordDefragRollback(cpumetrics.ResultError)
		return exchangeUnsettled
	}
	logger.Info("undid a half-applied exchange", "numContainers", len(updates))
	cp.metrics.RecordDefragRollback(cpumetrics.ResultSuccess)
	return exchangeUndone
}

// rollbackUpdates is the batch that puts one exchange's containers back where
// they were running before it. Only the ones the runtime accepted need it; the
// rest never moved.
//
// Built under applyMu because it reads the store, and sent with the lock
// released like every other batch. The CPUs it names are still reserved for those
// claims, since a mover never releases what it came from.
func (cp *CPUDriver) rollbackUpdates(round *defragRound, exchange int, notApplied map[types.UID]struct{}) ([]*api.ContainerUpdate, error) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if round.store != cp.cpuAllocationStore {
		return nil, fmt.Errorf("the allocation store was rebuilt while the exchange was out")
	}
	var updates []*api.ContainerUpdate
	for _, containerUID := range round.exchangeContainers[exchange] {
		if _, refused := notApplied[containerUID]; refused {
			continue
		}
		cpus, err := round.store.GetResourceClaimOriginUnion(round.claimsByContainer[containerUID]...)
		if err != nil {
			return nil, fmt.Errorf("cannot determine the CPUs container %q came from: %w", containerUID, err)
		}
		update := &api.ContainerUpdate{ContainerId: string(containerUID)}
		update.SetLinuxCPUSetCPUs(cpus.String())
		updates = append(updates, update)
	}
	return updates, nil
}

// observeNodeShape republishes how well placed the node's claims are, how large
// a claim each NUMA node could still take unsplit, and which NUMA nodes the
// driver has stopped vouching for.
//
// It measures rather than plans, so every scope is reported on every pass,
// including one whose round is still unsettled. The two fragmentation gauges are
// replaced wholesale, which is why they are taken together and not one round at
// a time; the fence gauge is per node, because a fence is raised and lifted
// between passes and must not wait for one.
//
// Both keep the shape they had before the node's cores could be divided, so the
// per-partition rounds are folded back into one number per node: the avoidable
// spread is summed, because it is the whole node's to repair however its cores
// are divided, and the largest alignable free block is the largest over the
// node's partitions, because a claim lands inside one of them and so the best
// any single claim can get is the best partition's answer.
func (cp *CPUDriver) observeNodeShape(logger logr.Logger, online cpuset.CPUSet) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	// Every NUMA node the topology has, not only those holding a claim or a
	// partition a pass can plan in. A node with no claims has nothing to move,
	// but it still has a shape worth reporting: how large a claim it could take
	// aligned is most interesting precisely when it is empty, and a gauge that
	// disappears as a node drains cannot be alerted on. A node whose every
	// partition the machine contradicts reports a zero it can be alerted on
	// rather than no series at all.
	topo := cp.topology.cpuTopology
	allocatable := cp.defragAllocatable(online)
	state := cpumetrics.DefragState{LargestAlignableFreeCPUs: map[int]int{}}
	for _, numaNodeID := range topo.CPUDetails.NUMANodes().List() {
		cp.metrics.SetDefragNodePoisoned(numaNodeID, cp.nodeIsPoisoned(numaNodeID))
		nodeTopo, err := defrag.NewTopology(topo, numaNodeID, allocatable)
		if err != nil {
			logger.V(2).Info("node cannot be measured", "numaNode", numaNodeID, "reason", err.Error())
			continue
		}
		_, shape := cp.defragNodeViews(logger, nodeTopo, online)
		state.ExcessUncoreCaches += shape.excessUncoreCaches
		state.LargestAlignableFreeCPUs[numaNodeID] = shape.largestAlignableFreeCPUs
	}
	cp.metrics.SetDefragState(state)
}

func (cp *CPUDriver) currentOnlineCPUs(logger logr.Logger) (cpuset.CPUSet, error) {
	if cp.sysfs == nil {
		return cpuset.New(), fmt.Errorf("no sysfs to read them from")
	}
	return cpuinfo.OnlineCPUs(logger, cp.sysfs)
}

// beginDefragRound plans one scope's moves, reserves them, and records each
// claim's new placement in its CDI spec. It returns nil when there is nothing to
// do.
func (cp *CPUDriver) beginDefragRound(logger logr.Logger, scope defragScope, online cpuset.CPUSet) *defragRound {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	if round := cp.takePendingRound(logger, scope); round != nil {
		return round
	}
	if cp.nodeIsPoisoned(scope.numaNodeID) {
		// The record a plan would be built from may be wrong about which claim
		// holds which CPUs, which is the one thing a plan may not be wrong about.
		logger.V(2).Info("not planning on a fenced NUMA node")
		return nil
	}

	if plan := cp.getActiveExactPlan(scope.numaNodeID); plan != nil {
		view, ok := cp.defragView(logger, scope, online)
		if ok && !plan.MatchesLedger(view.placements) {
			logger.Info("active exact plan no longer matches ledger; aborting and clearing exact plan", "numaNode", scope.numaNodeID)
			cp.clearActiveExactPlan(scope.numaNodeID)
		}
	}

	moves := cp.planScopeMoves(logger, scope, online)
	if len(moves) == 0 {
		return nil
	}

	round := newDefragRound(scope, cp.cpuAllocationStore)
	for _, step := range defragSteps(moves) {
		if !cp.beginDefragStep(logger, round, step) {
			continue
		}
		round.moves = append(round.moves, step...)
	}
	if len(round.moves) == 0 {
		return nil
	}

	if err := cp.roundUpdates(logger, round); err != nil {
		// Most likely the shared pool cannot be narrowed any further. Undo
		// everything rather than move a claim onto CPUs a shared container still
		// holds.
		logger.Error(err, "abandoning defragmentation round", "numMoves", len(round.moves))
		cp.abortMoves(logger, round.moves)
		return nil
	}
	return round
}

// beginDefragStep reserves one step and records the new placement of every claim
// in it, and reports whether the step can be attempted at all.
//
// An exchange is reserved as a unit and abandoned as a unit: each of its moves
// takes CPUs another is still running on, so keeping one of them would put two
// claims on the same CPUs with nothing left to undo it.
//
// Called with applyMu held.
func (cp *CPUDriver) beginDefragStep(logger logr.Logger, round *defragRound, step []defrag.Move) bool {
	logger = logger.WithValues("claimUIDs", stepClaims(step))
	if step[0].Exchange == 0 {
		move := step[0]
		if err := cp.cpuAllocationStore.BeginRebind(logger, move.ClaimUID, move.To); err != nil {
			logger.Error(err, "cannot start moving claim")
			return false
		}
	} else {
		targets := make(map[types.UID]cpuset.CPUSet, len(step))
		for _, move := range step {
			targets[move.ClaimUID] = move.To
		}
		if err := cp.cpuAllocationStore.BeginSwap(logger, targets); err != nil {
			logger.Error(err, "cannot start exchanging claims")
			return false
		}
	}

	// The spec on disk is the desired placement, and it is what a driver restart
	// rebuilds the store from, so it has to name the target before the container
	// is told about it.
	for _, move := range step {
		partners := make([]types.UID, 0, len(step)-1)
		for _, other := range step {
			if other.ClaimUID != move.ClaimUID {
				partners = append(partners, other.ClaimUID)
			}
		}
		roundProv := &store.RoundProvenance{
			RoundID:  round.id,
			Origin:   move.From,
			Target:   move.To,
			Partners: partners,
		}
		if err := cp.writeClaimPlacementWithRound(logger.WithValues("claimUID", move.ClaimUID), move.ClaimUID, roundProv); err != nil {
			logger.Error(err, "cannot record new placement, leaving the claims where they are", "claimUID", move.ClaimUID, "to", move.To.String())
			cp.abortMoves(logger, step)
			return false
		}
	}
	return true
}

// takePendingRound returns the round a previous attempt could not confirm, so it
// can be sent again. Cpuset updates are idempotent, and until one is confirmed
// its claims hold both their old and their new CPUs, so re-sending is the only
// way to find out what happened without guessing.
func (cp *CPUDriver) takePendingRound(logger logr.Logger, scope defragScope) *defragRound {
	round := cp.pendingRounds[scope]
	if round == nil {
		return nil
	}
	if round.store != cp.cpuAllocationStore {
		// Synchronize rebuilt the stores from the specs on disk, which already
		// name the targets, so this round's reservations are gone and its claims
		// are recorded where it was taking them.
		logger.V(2).Info("dropping an unconfirmed defragmentation round: the stores were rebuilt")
		delete(cp.pendingRounds, scope)
		return nil
	}
	logger.V(2).Info("retrying an unconfirmed defragmentation round", "numMoves", len(round.moves))
	round.replayed = true
	return round
}

// defragAllocatable is the CPUs a pass may place a claim on at all: online now,
// known to the topology, and not withheld by configuration. The driver reads the
// online set once in New, so a pass is handed a fresh one.
func (cp *CPUDriver) defragAllocatable(online cpuset.CPUSet) cpuset.CPUSet {
	topo := cp.topology.cpuTopology
	return online.Intersection(topo.CPUDetails.CPUs()).Difference(cp.topology.reservedCPUs)
}

// defragPartitions is the partitions a round may run inside: those a claim takes
// CPUs of its own from, minus the ones the machine contradicts. A degraded
// partition publishes no device, so no claim was ever allocated there and its
// CPUs are not free space for a move to take.
//
// A driver whose cores have not been resolved into partitions plans inside the
// implicit partition alone, which is every allocatable CPU: on a node whose
// cores nobody described that is what the resolution comes to anyway, so the
// scope is then the NUMA node it has always been.
func (cp *CPUDriver) defragPartitions(allocatable cpuset.CPUSet) []device.Partition {
	partitions := cp.partitions
	if len(partitions) == 0 {
		partitions = device.WithImplicitDefault(nil, allocatable)
	}
	planned := make([]device.Partition, 0, len(partitions))
	for _, partition := range partitions {
		if !partition.PublishesExclusiveDevices() {
			continue
		}
		if _, degraded := cp.degradedPartitions[partition.Name]; degraded {
			continue
		}
		planned = append(planned, partition)
	}
	return planned
}

// defragPartition resolves a scope's partition, and reports whether the driver
// still plans inside one of that name.
func (cp *CPUDriver) defragPartition(name string, allocatable cpuset.CPUSet) (device.Partition, bool) {
	for _, partition := range cp.defragPartitions(allocatable) {
		if partition.Name == name {
			return partition, true
		}
	}
	return device.Partition{}, false
}

// defragScopes is every region a pass plans over, NUMA node by NUMA node and,
// inside each, partition by partition, so that a pass walks the machine in a
// fixed order however the maps behind it are iterated.
//
// A partition that reaches into no other NUMA node is the ordinary case rather
// than a region that cannot be defragmented, so a pair with no CPUs between them
// is left out here instead of failing to build a topology later.
func (cp *CPUDriver) defragScopes(online cpuset.CPUSet) []defragScope {
	topo := cp.topology.cpuTopology
	allocatable := cp.defragAllocatable(online)
	partitions := cp.defragPartitions(allocatable)

	var scopes []defragScope
	for _, numaNodeID := range topo.CPUDetails.NUMANodes().List() {
		inNode := topo.CPUDetails.CPUsInNUMANodes(numaNodeID).Intersection(allocatable)
		for _, partition := range partitions {
			if partition.CPUs.Intersection(inNode).IsEmpty() {
				continue
			}
			scopes = append(scopes, defragScope{numaNodeID: numaNodeID, partition: partition.Name})
		}
	}
	return scopes
}

// defragScopeView is one scope as both planning and measuring see it.
type defragScopeView struct {
	scope          defragScope
	topology       *defrag.Topology
	free           cpuset.CPUSet
	placements     []defrag.Placement
	threadsPerCore int
	// keepFreePoolNonEmpty is set only for the partition the containers holding
	// no claim run in: a round anywhere else cannot empty their pool however many
	// CPUs it takes, since none of the CPUs it takes were theirs to run on.
	keepFreePoolNonEmpty bool
}

// defragView builds that view, or reports that the scope cannot be reasoned
// about. Called with applyMu held.
func (cp *CPUDriver) defragView(logger logr.Logger, scope defragScope, online cpuset.CPUSet) (defragScopeView, bool) {
	topo := cp.topology.cpuTopology
	allocatable := cp.defragAllocatable(online)
	partition, ok := cp.defragPartition(scope.partition, allocatable)
	if !ok {
		logger.V(2).Info("scope cannot be defragmented", "reason", "the driver plans inside no partition of that name")
		return defragScopeView{}, false
	}

	// The whole of where this scope's moves may land. Every target a plan emits
	// comes out of this set, which is what makes a move across a partition
	// impossible rather than merely unwanted.
	scopeCPUs := partition.CPUs.Intersection(allocatable)
	nodeTopo, err := defrag.NewTopology(topo, scope.numaNodeID, scopeCPUs)
	if err != nil {
		logger.V(2).Info("scope cannot be defragmented", "reason", err.Error())
		return defragScopeView{}, false
	}

	free := cp.cpuAllocationStore.GetSharedCPUs().Intersection(nodeTopo.CPUs())
	if cp.fullPhysicalCPUsOnly {
		// A half-free core cannot take a whole-core claim, so it is not free for
		// this purpose. A promise about the free pool as a whole, so this only
		// needs to know the option is requested, not any one device's step.
		free = topo.CPUDetails.CompleteCores(free)
	}
	placements := defrag.PlacementsByNUMANode(topo, cp.cpuAllocationStore.ExclusiveClaimAllocations())
	return defragScopeView{
		scope:          scope,
		topology:       nodeTopo,
		free:           free,
		placements:     placementsWithin(placements[scope.numaNodeID], nodeTopo.CPUs()),
		threadsPerCore: cp.scopeThreadsPerCore(nodeTopo.CPUs()),
		// The containers holding no claim run on the default partitions alone,
		// so only a round there has to leave one of their CPUs behind.
		keepFreePoolNonEmpty: partition.Role == device.PARTITION_ROLE_DEFAULT &&
			len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0,
	}, true
}

// defragNodeShape is one NUMA node's fragmentation as both the gauges and the
// /placements report state it.
type defragNodeShape struct {
	excessUncoreCaches       int
	largestAlignableFreeCPUs int
}

// defragNodeViews builds every planning region of one NUMA node and folds them
// into that one shape, so the gauges and the endpoint cannot drift apart.
//
// The spread is summed because it is the whole node's to repair however its
// cores are divided, and the largest alignable block is the best of the node's
// partitions, because a claim lands inside one of them.
//
// A claim no region holds is added from the node's own topology. Such a claim
// straddles two partitions -- a partition list edited under a running node,
// which dra_cpu_misplaced_claims_total counts -- so no pass can repair it; but
// neither can a claim that never asked to move, and that one is reported, which
// is what docs/user/defragmentation.md promises. An unrepairable spread the
// operator cannot see is worse than one they can.
func (cp *CPUDriver) defragNodeViews(logger logr.Logger, nodeTopo *defrag.Topology, online cpuset.CPUSet) ([]defragScopeView, defragNodeShape) {
	var views []defragScopeView
	var shape defragNodeShape
	counted := map[types.UID]struct{}{}

	for _, scope := range cp.defragScopes(online) {
		if scope.numaNodeID != nodeTopo.NUMANodeID() {
			continue
		}
		view, ok := cp.defragView(logger.WithValues(scope.logValues()...), scope, online)
		if !ok {
			continue
		}
		views = append(views, view)
		shape.excessUncoreCaches += view.topology.Cost(view.placements)
		if largest := largestAlignableFreeCPUs(view.topology, view.free); largest > shape.largestAlignableFreeCPUs {
			shape.largestAlignableFreeCPUs = largest
		}
		for _, placement := range view.placements {
			counted[placement.ClaimUID] = struct{}{}
		}
	}

	onNode := defrag.PlacementsByNUMANode(cp.topology.cpuTopology, cp.cpuAllocationStore.ExclusiveClaimAllocations())
	for _, placement := range onNode[nodeTopo.NUMANodeID()] {
		if _, ok := counted[placement.ClaimUID]; ok {
			continue
		}
		shape.excessUncoreCaches += nodeTopo.ExcessSpread(placement.CPUs)
	}
	return views, shape
}

// placementsWithin keeps the placements that lie wholly inside cpus.
//
// A claim's CPUs within one NUMA node are taken from one device and so lie in
// one partition. One that straddles two cannot be moved as a whole anyway, since
// a rebind replaces a claim's entire exclusive set and refuses a target of a
// different size, so leaving it out of the region makes the ideal packing treat
// it as the fixed obstacle it is rather than keep demanding a move nothing can
// make. Its spread is still measured -- see defragNodeViews.
func placementsWithin(placements []defrag.Placement, cpus cpuset.CPUSet) []defrag.Placement {
	within := make([]defrag.Placement, 0, len(placements))
	for _, placement := range placements {
		if placement.CPUs.IsSubsetOf(cpus) {
			within = append(within, placement)
		}
	}
	return within
}

// scopeThreadsPerCore is the whole-core allocation step a plan for these CPUs
// must respect: the count their own cores agree on, and zero where they do not
// or where whole-core allocation was never asked for, which is what the selector
// reads as "no whole-core promise".
//
// Computed from the scope's own cores rather than the NUMA node's, which is the
// same number pkg/device computes for the devices of this partition in this NUMA
// node. A node holding an SMT partition beside one whose siblings the platform
// took offline has no single answer, and neither partition should be given the
// other's.
func (cp *CPUDriver) scopeThreadsPerCore(cpus cpuset.CPUSet) int {
	if !cp.fullPhysicalCPUsOnly {
		return 0
	}
	if threads := cp.topology.cpuTopology.CPUDetails.UniformThreadsPerCore(cpus); threads > 1 {
		return threads
	}
	return 0
}

// planScopeMoves plans one scope. Called with applyMu held.
func (cp *CPUDriver) planScopeMoves(logger logr.Logger, scope defragScope, online cpuset.CPUSet) []defrag.Move {
	if cp.hasActiveExactPlan(scope.numaNodeID) {
		logger.V(4).Info("skipping greedy pass on NUMA node with active exact plan", "numaNode", scope.numaNodeID)
		return nil
	}

	view, ok := cp.defragView(logger, scope, online)
	if !ok {
		return nil
	}

	plan, err := defrag.PlanNode(view.topology, view.placements, view.free, cp.defragSelector(logger, view.threadsPerCore), defrag.Options{
		Eligible:   cp.claimMovable,
		AllowSwaps: cp.defrag.allowTransientOverlap,
		// While a move is in flight its claim holds both its old and its new
		// CPUs, so a round that took every free CPU would leave the shared pool
		// momentarily empty, which NRI cannot express.
		KeepFreePoolNonEmpty: view.keepFreePoolNonEmpty,
	})
	if err != nil {
		logger.Error(err, "cannot plan defragmentation")
		return nil
	}
	logger.V(4).Info("planned defragmentation", "numMoves", len(plan.Moves), "blocked", plan.Blocked,
		"currentCost", plan.CurrentCost, "idealCost", plan.IdealCost, "reason", plan.Reason)
	cp.metrics.RecordDefragBlockedMoves(plan.Blocked)
	return plan.Moves
}

// hasActiveExactPlan reports whether the NUMA node has an active exact plan.
// Called with applyMu held.
func (cp *CPUDriver) hasActiveExactPlan(numaNodeID int) bool {
	if cp.activeExactPlans == nil {
		return false
	}
	return cp.activeExactPlans[numaNodeID] != nil
}

// setActiveExactPlan stores an active exact plan for the NUMA node.
// Called with applyMu held.
func (cp *CPUDriver) setActiveExactPlan(numaNodeID int, plan *defrag.ExactPlan) {
	if cp.activeExactPlans == nil {
		cp.activeExactPlans = make(map[int]*defrag.ExactPlan)
	}
	cp.activeExactPlans[numaNodeID] = plan
}

// clearActiveExactPlan removes the active exact plan for the NUMA node.
// Called with applyMu held.
func (cp *CPUDriver) clearActiveExactPlan(numaNodeID int) {
	if cp.activeExactPlans != nil {
		delete(cp.activeExactPlans, numaNodeID)
	}
}

// getActiveExactPlan returns the active exact plan for the NUMA node.
// Called with applyMu held.
func (cp *CPUDriver) getActiveExactPlan(numaNodeID int) *defrag.ExactPlan {
	if cp.activeExactPlans == nil {
		return nil
	}
	return cp.activeExactPlans[numaNodeID]
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
		return cp.selectMoveCPUs(logger, cp.topology.cpuTopology, available, numCPUs, threadsPerCore)
	}
}

// selectMoveCPUs picks a move's destination. It deliberately does not go through
// the configured CPUAllocator, which takeCPUsForDevice uses for a claim: that
// interface is the claim-allocation path, where the caller first asks the
// allocator for the claim's own hint. A move has no claim asking for anything --
// the destination is whatever the planner found free -- and the external
// allocator refuses an allocation whose hint is empty, so routing moves through
// it would fail every move on a node configured that way.
func (cp *CPUDriver) selectMoveCPUs(logger logr.Logger, topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs, threadsPerCore int) (cpuset.CPUSet, error) {
	if got, ok, err := cp.placedCPUs(topo, available, numCPUs, threadsPerCore); ok {
		return got, err
	}
	return cpumanager.TakeByTopologyNUMAPacked(logger, topo, available, numCPUs, cpumanager.CPUSortingStrategyPacked, true)
}

// claimMovable reports whether a claim may be moved now. Called with applyMu held.
func (cp *CPUDriver) claimMovable(claimUID types.UID) bool {
	return claimMovableIn(cp.cpuAllocationStore, claimUID)
}

// claimMovableIn answers the same question against a given store, so a caller
// that captured one under applyMu can ask after releasing it.
func claimMovableIn(allocations *store.CPUAllocation, claimUID types.UID) bool {
	// A move changes the CPUs under a running workload, and only the workload
	// knows whether it survives that, so nothing is moved that has not said so.
	if !allocations.IsRelocatable(claimUID) {
		return false
	}
	_, inFlight := allocations.GetRebindOrigin(claimUID)
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
	return cp.writeClaimPlacementWithRound(logger, claimUID, nil)
}

func (cp *CPUDriver) writeClaimPlacementWithRound(logger logr.Logger, claimUID types.UID, round *store.RoundProvenance) error {
	record, ok := cp.cpuAllocationStore.GetClaimRecord(claimUID)
	if !ok {
		return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
	}
	record.Round = round
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claimUID, cp.cdiEnvValue(record))
	return cp.cdiMgr.AddDevice(logger, getCDIDeviceName(claimUID), envVar, record)
}

// roundUpdates builds the one batch of container updates a round consists of: the
// containers holding moved claims, each pinned to the union of all its claims,
// plus the shared containers that have to vacate the CPUs the moves are taking.
//
// A moved claim with no running container needs no update at all; the store and
// its spec are the whole of its state until a container is created from them.
func (cp *CPUDriver) roundUpdates(logger logr.Logger, round *defragRound) error {
	for _, move := range round.moves {
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
		if move.Exchange != 0 && !slices.Contains(round.exchangeContainers[move.Exchange], containerUID) {
			round.exchangeContainers[move.Exchange] = append(round.exchangeContainers[move.Exchange], containerUID)
		}
		if _, done := round.updateByContainer[containerUID]; done {
			continue
		}

		// A container holding several claims must be pinned to all of them at
		// once, moved or not.
		claimUIDs := state.ClaimUIDs()
		cpus, err := cp.cpuAllocationStore.GetResourceClaimAllocationUnion(claimUIDs...)
		if err != nil {
			return fmt.Errorf("cannot determine CPUs for container %q: %w", containerUID, err)
		}
		update := &api.ContainerUpdate{ContainerId: string(containerUID)}
		update.SetLinuxCPUSetCPUs(cpus.String())
		round.updates = append(round.updates, update)
		round.updateByContainer[containerUID] = update
		round.claimsByContainer[containerUID] = claimUIDs
	}

	// The pool is already narrowed by the reservations, so this moves shared
	// containers off the targets in the same batch. They are widened again once
	// the moves commit and the origins return to the pool.
	shared, err := cp.getSharedContainerUpdates(logger, types.UID(""))
	if err != nil {
		return err
	}
	round.updates = append(round.updates, shared...)
	return nil
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
		delete(cp.pendingRounds, round.scope)
		cp.forgetDefragRetry(round.scope)
		return cpumetrics.ResultSuccess
	}
	if updateErr != nil {
		// Nothing can be concluded from a failed call: some of the batch may have
		// been applied. Keep holding both halves of every move and send the same
		// round again rather than release CPUs a container may now be running on.
		logger.Error(updateErr, "defragmentation round unconfirmed, will retry", "numMoves", len(round.moves))
		cp.pendingRounds[round.scope] = round
		cp.retryDefragScope(round.scope)
		return cpumetrics.ResultError
	}
	refused := map[types.UID]struct{}{}
	for _, update := range failed {
		refused[types.UID(update.GetContainerId())] = struct{}{}
	}

	committed, reverted, unsettled := 0, 0, 0
	for _, step := range defragSteps(round.moves) {
		sLogger := logger.WithValues("claimUIDs", stepClaims(step))
		if step[0].Exchange == 0 {
			move := step[0]
			if cp.moveWasRefused(move, refused) {
				sLogger.Info("runtime refused a move, leaving the claim where it is",
					"from", move.From.String(), "to", move.To.String())
				reverted++
				cp.abortStep(sLogger, step)
				continue
			}
			if err := cp.cpuAllocationStore.CommitRebind(sLogger, move.ClaimUID); err != nil {
				sLogger.Error(err, "cannot complete the move")
				reverted++
				continue
			}
			if err := cp.writeClaimPlacement(sLogger, move.ClaimUID); err != nil {
				sLogger.Error(err, "cannot clear round provenance from recorded placement", "claimUID", move.ClaimUID)
			}
			committed++
			continue
		}

		wanted := round.outcomes[step[0].Exchange]
		if !cp.settleExchangeStep(sLogger, round, step) {
			// Neither applied nor undone: the claims still hold both cpusets, the
			// round is sent again rather than settled on a guess, and the NUMA
			// node is fenced until a read-back says where they are.
			sLogger.Info("an exchange is unsettled, keeping both cpusets reserved")
			unsettled += len(step)
			cp.poisonNode(sLogger, round.scope)
			continue
		}
		if wanted == exchangeApplied {
			committed += len(step)
			continue
		}
		reverted += len(step)
	}
	cp.metrics.RecordDefragMoves(cpumetrics.ResultSuccess, committed)
	cp.metrics.RecordDefragMoves(cpumetrics.ResultError, reverted)

	if unsettled > 0 {
		// Only the exchanges nobody can place are sent again: re-sending a move
		// this round has already settled would apply it a second time, and for a
		// move that was put back it would apply it against the ledger.
		cp.pendingRounds[round.scope] = round.retainUnsettled()
		cp.retryDefragScope(round.scope)
		return cpumetrics.ResultError
	}
	delete(cp.pendingRounds, round.scope)

	if committed > 0 {
		// Two jobs at once. The CPUs the moved claims left are back in the pool
		// and the shared containers entitled to them are still on the narrower
		// mask; and a target vacated by this round is not available until it
		// commits, so the moves it made possible are the next pass's to make.
		cp.requestReconcile()
	}
	if reverted > 0 {
		// The runtime declined a move the plan still wants, so the scope is not
		// settled and nothing else is going to look at it.
		cp.retryDefragScope(round.scope)
		return cpumetrics.ResultError
	}
	cp.forgetDefragRetry(round.scope)
	return cpumetrics.ResultSuccess
}

// retryDefragScope asks for another attempt at a scope the runtime left
// unsettled, after however long the rate limiter has decided this scope's
// failures are worth.
func (cp *CPUDriver) retryDefragScope(scope defragScope) {
	cp.defragRetries.AddRateLimited(scope)
}

// forgetDefragRetry clears a scope's accumulated backoff, so a scope that
// settles now starts from the shortest delay if it ever fails again. It does not
// withdraw an attempt the rate limiter has already scheduled.
func (cp *CPUDriver) forgetDefragRetry(scope defragScope) {
	cp.defragRetries.Forget(scope)
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
	for _, step := range defragSteps(moves) {
		cp.abortStep(logger.WithValues("claimUIDs", stepClaims(step)), step)
	}
}

// settleExchangeStep applies one exchange's recorded outcome to the store and
// the specs on disk, and reports whether it is settled. An outcome nobody could
// determine, and one the store refuses, both leave it unsettled.
//
// Called with applyMu held.
func (cp *CPUDriver) settleExchangeStep(logger logr.Logger, round *defragRound, step []defrag.Move) bool {
	exchange := step[0].Exchange
	switch round.outcomes[exchange] {
	case exchangeApplied:
		if err := cp.cpuAllocationStore.CommitSwap(logger, stepClaims(step)...); err != nil {
			logger.Error(err, "cannot complete the exchange")
			round.outcomes[exchange] = exchangeUnsettled
			return false
		}
		for _, move := range step {
			if err := cp.writeClaimPlacement(logger, move.ClaimUID); err != nil {
				logger.Error(err, "cannot clear round provenance from recorded placement", "claimUID", move.ClaimUID)
			}
		}
		return true
	case exchangeUndone:
		if !cp.abortStep(logger, step) {
			round.outcomes[exchange] = exchangeUnsettled
			return false
		}
		return true
	default:
		return false
	}
}

// abortStep puts one step's claims back where they came from, in the store and
// in the specs on disk, and reports whether the store took it. An exchange is
// undone as a unit for the reason it is reserved as one.
//
// Called with applyMu held.
func (cp *CPUDriver) abortStep(logger logr.Logger, step []defrag.Move) bool {
	if step[0].Exchange == 0 {
		if err := cp.cpuAllocationStore.AbortRebind(logger, step[0].ClaimUID); err != nil {
			logger.Error(err, "cannot undo the reservation")
			return false
		}
	} else if err := cp.cpuAllocationStore.AbortSwap(logger, stepClaims(step)...); err != nil {
		logger.Error(err, "cannot undo the exchange's reservation")
		return false
	}
	for _, move := range step {
		if err := cp.writeClaimPlacement(logger, move.ClaimUID); err != nil {
			logger.Error(err, "cannot restore the recorded placement", "claimUID", move.ClaimUID)
		}
	}
	return true
}

// retainUnsettled narrows a round to the exchanges the runtime left in a state
// nobody can name, which are the only part of it worth sending again: a move
// this round has already settled would be applied a second time, and one that
// was put back would be applied against the ledger.
func (r *defragRound) retainUnsettled() *defragRound {
	next := newDefragRound(r.scope, r.store)
	for _, step := range defragSteps(r.moves) {
		exchange := step[0].Exchange
		if exchange == 0 || r.outcomes[exchange] != exchangeUnsettled {
			continue
		}
		next.moves = append(next.moves, step...)
		next.exchangeContainers[exchange] = r.exchangeContainers[exchange]
		for _, containerUID := range r.exchangeContainers[exchange] {
			next.updates = append(next.updates, r.updateByContainer[containerUID])
			next.updateByContainer[containerUID] = r.updateByContainer[containerUID]
			next.claimsByContainer[containerUID] = r.claimsByContainer[containerUID]
		}
	}
	return next
}
