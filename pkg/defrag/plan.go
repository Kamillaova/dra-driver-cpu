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

package defrag

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// Selector picks numCPUs out of available. Pass the same allocator the driver
// uses to place a new claim, so a plan can never propose a placement the driver
// would not have chosen itself.
type Selector func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error)

// Move relocates one claim's CPUs within a NUMA node. From and To always have
// the same size, and may overlap.
type Move struct {
	ClaimUID types.UID
	From     cpuset.CPUSet
	To       cpuset.CPUSet
}

// Plan is what one pass would do to one NUMA node.
type Plan struct {
	NUMANodeID int
	Moves      []Move
	// CurrentCost and IdealCost are the node's alignment cost now and under the
	// ideal packing. Their difference is what the pass is worth in total, which
	// is not what these moves alone achieve: a pass that cannot reach the ideal
	// yet still emits the moves that get closer to it.
	CurrentCost int
	IdealCost   int
	// Blocked counts moves the ideal calls for that could not be made this pass,
	// either because another claim is in the way or because they would leave the
	// moved claim worse placed than it is.
	Blocked int
	// Reason says what kept the plan from being fuller. Empty when the ideal was
	// reached in one pass.
	Reason string
}

// Options tunes a plan.
type Options struct {
	// Eligible reports whether a claim may be moved at all, which is how an
	// opt-out is applied. Nil means every claim may move.
	//
	// An ineligible claim is not merely left out of the plan: the ideal packing
	// is built around it, so it stays a fixed obstacle rather than a claim the
	// ideal keeps calling to be moved and the plan can never move.
	Eligible func(types.UID) bool
	// KeepFreePoolNonEmpty drops moves until at least one free CPU is left over.
	// Set it whenever a container without a claim is running on the node: while
	// a move is in flight its claim holds both its old and its new CPUs, so a
	// plan taking every free CPU at once would momentarily leave the shared pool
	// empty, and NRI cannot express an empty cpuset.
	KeepFreePoolNonEmpty bool
}

func (o Options) eligible(claimUID types.UID) bool {
	return o.Eligible == nil || o.Eligible(claimUID)
}

// PlanNode plans the moves that bring one NUMA node closer to the best packing
// its claims allow.
//
// free is the node's unallocated CPUs, which under whole-core allocation the
// caller restricts to complete cores. It is the free set as of the start of the
// pass and is never revised while planning, which is what makes the result safe
// to apply in any order and in part: every target lies within the planned claim's
// own CPUs plus that same free set, and targets never overlap, so a move that
// fails to apply cannot invalidate one that succeeded.
//
// The plan aims at the packing the allocator would have produced had every claim
// arrived on an empty node, largest first. That ideal depends only on the claim
// sizes and the topology and never on where the claims currently sit, so it is
// the same on every pass: a claim takes its ideal CPUs when they are free and a
// step towards them when they are not, and only when that strictly improves the
// claim by the measure moveScore describes. Because that measure cannot rise, a
// node converges in a bounded number of passes instead of churning.
//
// The ideal is the destination only for claims that need one. A claim already
// spread as little as its size allows is left where it is unless it occupies
// CPUs the ideal owes to a claim that is actually misaligned, so a repair
// relocates the straddling claim and the bystanders in its way, and nothing
// else: the node ends aligned, not necessarily identical to the ideal layout.
func PlanNode(topo *Topology, placements []Placement, free cpuset.CPUSet, sel Selector, opts Options) (Plan, error) {
	plan := Plan{NUMANodeID: topo.NUMANodeID()}
	for _, p := range placements {
		if !p.CPUs.IsSubsetOf(topo.CPUs()) {
			return Plan{}, fmt.Errorf("claim %q holds CPUs %q which are not allocatable in NUMA node %d (%q)",
				p.ClaimUID, p.CPUs.String(), topo.NUMANodeID(), topo.CPUs().String())
		}
	}
	free = free.Intersection(topo.CPUs())

	ideal, err := idealPacking(topo, placements, free, sel, opts)
	if err != nil {
		return Plan{}, err
	}

	plan.CurrentCost = topo.Cost(placements)
	idealPlacements := make([]Placement, 0, len(placements))
	for _, p := range placements {
		idealPlacements = append(idealPlacements, Placement{ClaimUID: p.ClaimUID, CPUs: ideal[p.ClaimUID]})
	}
	plan.IdealCost = topo.Cost(idealPlacements)

	if plan.CurrentCost <= plan.IdealCost {
		plan.Reason = "node is already packed as well as its claims allow"
		return plan, nil
	}

	moves, blocked, reason := plannedMoves(topo, placements, ideal, free, sel, opts)
	plan.Moves = moves
	plan.Blocked = blocked
	plan.Reason = reason
	if len(moves) == 0 && reason == "" {
		plan.Reason = fmt.Sprintf("all %d moves towards the ideal are blocked", blocked)
	}
	if len(moves) > 0 && reason == "" && blocked > 0 {
		plan.Reason = fmt.Sprintf("%d further moves towards the ideal are blocked", blocked)
	}
	return plan, nil
}

// idealPacking assigns every claim the CPUs it would have received had all of
// them arrived on an empty node, largest first, with ties broken by claim UID so
// the result is stable. Claims that may not move keep the CPUs they hold and the
// rest are packed around them.
func idealPacking(topo *Topology, placements []Placement, free cpuset.CPUSet, sel Selector, opts Options) (map[types.UID]cpuset.CPUSet, error) {
	ideal := make(map[types.UID]cpuset.CPUSet, len(placements))
	movable := make([]Placement, 0, len(placements))
	// Only the free CPUs and the movable claims' own CPUs may be redistributed.
	// Taking the node's whole allocatable set instead would let the ideal use
	// CPUs the caller has excluded, such as the odd thread of a core split by
	// the reserved set.
	available := free
	for _, p := range placements {
		if !opts.eligible(p.ClaimUID) {
			ideal[p.ClaimUID] = p.CPUs
			continue
		}
		movable = append(movable, p)
		available = available.Union(p.CPUs)
	}

	for _, p := range packingOrder(movable) {
		target, err := sel(available, p.CPUs.Size())
		if err != nil {
			return nil, fmt.Errorf("no ideal packing for NUMA node %d: placing claim %q (%d CPUs) in %q: %w",
				topo.NUMANodeID(), p.ClaimUID, p.CPUs.Size(), available.String(), err)
		}
		ideal[p.ClaimUID] = target
		available = available.Difference(target)
	}
	return ideal, nil
}

// moveScore ranks a placement for one claim: first how many uncore caches it
// wastes, then how many CPUs it occupies that the ideal packing owes to some
// other claim. A move is made only when it strictly lowers this.
//
// The second term is what breaks the standing-in-the-way deadlock. A small claim
// sitting alone in the cache a large claim needs is already as well placed as its
// own size allows, so nothing about its own spread will ever ask it to move,
// while the large claim cannot move until it does. Counting the CPUs it is
// squatting on gives it a reason to step aside, and gives it nothing to gain from
// stepping into someone else's place instead.
//
// Ordering the terms this way keeps a move from ever leaving its own claim worse
// placed: no amount of stepping aside pays for one more wasted cache. And the
// ideal is the same on every pass, so the total over all claims is a
// non-negative integer that strictly falls whenever anything moves, which bounds
// the passes.
func moveScore(topo *Topology, cpus, othersIdeal cpuset.CPUSet) int {
	return topo.ExcessSpread(cpus)*(topo.CPUs().Size()+1) + cpus.Intersection(othersIdeal).Size()
}

// plannedMoves walks the claims in the order the ideal packing placed them and
// gives each the best CPUs actually available, largest claim first.
//
// A claim whose ideal CPUs are free takes them outright. One whose ideal is still
// occupied is not simply skipped: it takes the best placement available now,
// which moves it closer to the ideal and lets the claims behind it move next
// pass. Skipping instead would deadlock a node whose free space is scattered one
// CPU per cache, where no claim can reach its ideal in a single step and none of
// them would ever move.
//
// Availability only ever shrinks as moves are added, and never grows by the CPUs
// a move vacates: those belong to a container that is still running on them until
// the move is applied. That is what makes any subset of the result safe to apply
// in any order.
func plannedMoves(topo *Topology, placements []Placement, ideal map[types.UID]cpuset.CPUSet, free cpuset.CPUSet, sel Selector, opts Options) ([]Move, int, string) {
	allIdeal := cpuset.New()
	for _, cpus := range ideal {
		allIdeal = allIdeal.Union(cpus)
	}
	// The ideal slots of the claims that are actually misaligned. A claim
	// already spread as little as its size allows moves only to vacate these:
	// the ideal is a demand for alignment, not for tidiness, and every move
	// beyond that is churn some running workload pays for nothing. Without
	// this, repairing one straddling claim would also herd every well-placed
	// bystander into the ideal's preferred slots.
	needy := cpuset.New()
	for _, p := range placements {
		if topo.ExcessSpread(p.CPUs) > 0 {
			needy = needy.Union(ideal[p.ClaimUID])
		}
	}
	// CPUs the ideal leaves over. A claim that cannot reach its own ideal slot is
	// steered here rather than into another claim's, which is what makes stepping
	// aside progress instead of passing the problem on.
	spare := topo.CPUs().Difference(allIdeal)

	var moves []Move
	blocked := 0
	reason := ""
	avail := free

	for _, p := range packingOrder(placements) {
		wanted := ideal[p.ClaimUID]
		if wanted.Equals(p.CPUs) {
			continue
		}
		if topo.ExcessSpread(p.CPUs) == 0 && p.CPUs.Intersection(needy).IsEmpty() {
			continue
		}
		othersIdeal := allIdeal.Difference(wanted)
		target, ok := reachableTarget(topo, p.CPUs, wanted, spare, avail, othersIdeal, sel)
		if !ok {
			blocked++
			continue
		}
		if moveScore(topo, target, othersIdeal) >= moveScore(topo, p.CPUs, othersIdeal) {
			blocked++
			continue
		}
		if opts.KeepFreePoolNonEmpty && avail.Difference(target).IsEmpty() {
			blocked++
			reason = "held back a move to keep a CPU in the shared pool"
			continue
		}

		moves = append(moves, Move{ClaimUID: p.ClaimUID, From: p.CPUs, To: target})
		avail = avail.Difference(target)
	}
	return moves, blocked, reason
}

// reachableTarget returns the best CPUs a claim can take right now: its ideal
// slot when that is free, otherwise whichever of two candidates scores better.
//
// The first candidate comes from the ideal's leftovers plus the claim's own
// slot, so a claim that has to move somewhere provisional does not displace
// another one. The second comes from everything within reach. Both are offered
// rather than the first winning outright: when a claim cannot reach its own
// ideal slot, the only placement that stops it straddling caches may lie inside
// a slot the ideal promised to someone else, and if that someone cannot move
// there either, refusing it strands them both. moveScore decides, and wasted
// caches dominate it, so keeping out of the way still wins every tie that does
// not cost a cache.
func reachableTarget(topo *Topology, own, wanted, spare, avail, othersIdeal cpuset.CPUSet, sel Selector) (cpuset.CPUSet, bool) {
	reachable := avail.Union(own)
	if wanted.IsSubsetOf(reachable) {
		return wanted, true
	}

	var best cpuset.CPUSet
	found := false
	consider := func(pool cpuset.CPUSet) {
		if pool.Size() < own.Size() {
			return
		}
		target, err := sel(pool, own.Size())
		if err != nil {
			return
		}
		if !found || moveScore(topo, target, othersIdeal) < moveScore(topo, best, othersIdeal) {
			best, found = target, true
		}
	}
	consider(reachable.Intersection(wanted.Union(spare)))
	consider(reachable)
	return best, found
}

// packingOrder is largest claim first, ties broken by UID: the order the ideal
// packing itself uses, so a large claim gets first refusal on real availability
// exactly as it did on virtual availability.
func packingOrder(placements []Placement) []Placement {
	order := append([]Placement(nil), placements...)
	sort.SliceStable(order, func(i, j int) bool {
		if order[i].CPUs.Size() != order[j].CPUs.Size() {
			return order[i].CPUs.Size() > order[j].CPUs.Size()
		}
		return order[i].ClaimUID < order[j].ClaimUID
	})
	return order
}
