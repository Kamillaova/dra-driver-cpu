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
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// SearchStatus is the tri-state outcome of an exact bounded search.
type SearchStatus int

const (
	// SearchReachable indicates the search found the shortest legal move sequence to the goal.
	SearchReachable SearchStatus = iota
	// SearchNotProven indicates the work budget or depth limit was reached before proving reachability.
	// Callers treat this as unreachable (safe false negative).
	SearchNotProven
	// SearchUnreachable indicates the reachable state space was exhausted within budget and no path to goal exists.
	SearchUnreachable
)

func (s SearchStatus) String() string {
	switch s {
	case SearchReachable:
		return "reachable"
	case SearchNotProven:
		return "not-proven"
	case SearchUnreachable:
		return "unreachable"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// Feasible reports whether the goal was proven reachable with a witness move sequence.
func (s SearchStatus) Feasible() bool {
	return s == SearchReachable
}

// Goal defines the target condition for the exact search.
type Goal interface {
	// IsSatisfied reports whether the current placements and free CPUs satisfy the goal.
	IsSatisfied(topo *Topology, placements map[types.UID]cpuset.CPUSet, free cpuset.CPUSet) bool
	// TargetClaim returns the specific claim UID targeted by the goal, if any.
	TargetClaim() (types.UID, bool)
	// TargetCache returns the specific cache ID targeted by the goal, if any.
	TargetCache() (int, bool)
	// Description returns a readable summary of the goal.
	Description() string
}

// GoalMakeClaimWhole aims to align one claim so that it spans minimal caches.
type GoalMakeClaimWhole struct {
	ClaimUID types.UID
}

func (g GoalMakeClaimWhole) IsSatisfied(topo *Topology, placements map[types.UID]cpuset.CPUSet, free cpuset.CPUSet) bool {
	cpus, ok := placements[g.ClaimUID]
	if !ok || cpus.IsEmpty() {
		return false
	}
	return topo.ExcessSpread(cpus) == 0
}

func (g GoalMakeClaimWhole) TargetClaim() (types.UID, bool) { return g.ClaimUID, true }
func (g GoalMakeClaimWhole) TargetCache() (int, bool)        { return 0, false }
func (g GoalMakeClaimWhole) Description() string {
	return fmt.Sprintf("make claim %q whole", g.ClaimUID)
}

// GoalFreeCache aims to free all allocatable CPUs in a specific cache.
type GoalFreeCache struct {
	CacheID int
}

func (g GoalFreeCache) IsSatisfied(topo *Topology, placements map[types.UID]cpuset.CPUSet, free cpuset.CPUSet) bool {
	cacheCPUs := topo.CPUsInCache(g.CacheID)
	if cacheCPUs.IsEmpty() {
		return false
	}
	return cacheCPUs.IsSubsetOf(free)
}

func (g GoalFreeCache) TargetClaim() (types.UID, bool) { return "", false }
func (g GoalFreeCache) TargetCache() (int, bool)        { return g.CacheID, true }
func (g GoalFreeCache) Description() string {
	return fmt.Sprintf("free cache %d", g.CacheID)
}

// GoalKCacheHome aims to produce K whole free caches.
type GoalKCacheHome struct {
	K int
}

func (g GoalKCacheHome) IsSatisfied(topo *Topology, placements map[types.UID]cpuset.CPUSet, free cpuset.CPUSet) bool {
	if g.K <= 0 {
		return true
	}
	freeCaches := 0
	for _, cacheID := range topo.Caches() {
		if topo.CPUsInCache(cacheID).IsSubsetOf(free) {
			freeCaches++
			if freeCaches >= g.K {
				return true
			}
		}
	}
	return false
}

func (g GoalKCacheHome) TargetClaim() (types.UID, bool) { return "", false }
func (g GoalKCacheHome) TargetCache() (int, bool)        { return 0, false }
func (g GoalKCacheHome) Description() string {
	return fmt.Sprintf("produce a %d-cache home", g.K)
}

// ExactOptions tunes an exact bounded search.
type ExactOptions struct {
	// Eligible reports whether a claim may move. Nil means all claims are eligible.
	Eligible func(types.UID) bool
	// AllowSwaps permits swap moves (exchanges between two claims).
	AllowSwaps bool
	// MaxDepth is the maximum move depth. If <= 0, defaults to 3.
	MaxDepth int
	// Budget is the maximum state expansions. If <= 0, derived from geometry.
	Budget int
	// CanonicaliseCaches controls whether states are canonicalised under cache permutations.
	// Defaults to true. Setting false enables the uncanonicalised oracle comparison.
	CanonicaliseCaches bool
}

func (o ExactOptions) eligible(claimUID types.UID) bool {
	return o.Eligible == nil || o.Eligible(claimUID)
}

// ExactPlan represents the result of an exact bounded search.
type ExactPlan struct {
	NUMANodeID    int
	Status        SearchStatus
	Moves         []Move
	Closure       cpuset.CPUSet
	Goal          Goal
	WorkCount     int
	Budget        int
	DisplacedCPUs int
}

// MatchesLedger reports whether each move's source CPUs still match the live placement.
func (p *ExactPlan) MatchesLedger(placements []Placement) bool {
	curr := make(map[types.UID]cpuset.CPUSet, len(placements))
	for _, pl := range placements {
		curr[pl.ClaimUID] = pl.CPUs
	}
	for _, m := range p.Moves {
		c, ok := curr[m.ClaimUID]
		if !ok || !c.Equals(m.From) {
			return false
		}
	}
	return true
}

// ComputeClosure calculates and stores the union of all From and To CPU sets across all moves.
func (p *ExactPlan) ComputeClosure() cpuset.CPUSet {
	closure := cpuset.New()
	for _, m := range p.Moves {
		closure = closure.Union(m.From).Union(m.To)
	}
	p.Closure = closure
	return closure
}

// Combination computes C(n, k).
func Combination(n, k int) int {
	if k < 0 || k > n {
		return 0
	}
	if k == 0 || k == n {
		return 1
	}
	if k > n/2 {
		k = n - k
	}
	result := 1
	for i := 1; i <= k; i++ {
		result = result * (n - i + 1) / i
	}
	return result
}

// TransferVectorsV computes V(C, 2m) = \sum_{a+b<=C} C(C,a)*C(C-a,b)*C(m-1,a-1)*C(m-1,b-1).
// For C=8 at m=1..4, it yields 56, 812, 5768, 26474.
func TransferVectorsV(C, twoM int) int {
	m := twoM / 2
	if m < 1 || C < 1 {
		return 0
	}
	total := 0
	for a := 1; a <= C; a++ {
		cCa := Combination(C, a)
		cMa := Combination(m-1, a-1)
		if cCa == 0 || cMa == 0 {
			continue
		}
		for b := 1; a+b <= C; b++ {
			cCb := Combination(C-a, b)
			cMb := Combination(m-1, b-1)
			if cCb == 0 || cMb == 0 {
				continue
			}
			total += cCa * cCb * cMa * cMb
		}
	}
	return total
}

// DerivedBudget computes B(m) = T * V(C, 2m) * C^2.
func DerivedBudget(topo *Topology, placements []Placement, m int, eligible func(types.UID) bool) int {
	C := len(topo.Caches())
	if C < 1 {
		return 0
	}
	if m <= 0 {
		m = 3
	}
	typeSizes := map[int]struct{}{}
	for _, p := range placements {
		if eligible == nil || eligible(p.ClaimUID) {
			typeSizes[p.CPUs.Size()] = struct{}{}
		}
	}
	T := len(typeSizes)
	if T < 1 {
		T = 1
	}
	v := TransferVectorsV(C, 2*m)
	return T * v * C * C
}

type claimOccupant struct {
	size        int
	relocatable bool
	isTarget    bool
}

type cacheSignature struct {
	cacheID       int
	capacity      int
	freeCPUs      int
	isTargetCache bool
	occupants     []claimOccupant
}

func (cs cacheSignature) string(canonical bool) string {
	var b strings.Builder
	if !canonical {
		fmt.Fprintf(&b, "id:%d;", cs.cacheID)
	}
	fmt.Fprintf(&b, "cap:%d;free:%d;target:%t;occ:", cs.capacity, cs.freeCPUs, cs.isTargetCache)
	for i, occ := range cs.occupants {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d:%t:%t", occ.size, occ.relocatable, occ.isTarget)
	}
	return b.String()
}

func stateKey(topo *Topology, placements map[types.UID]cpuset.CPUSet, free cpuset.CPUSet, goal Goal, opts ExactOptions, canonical bool) string {
	targetClaim, hasTargetClaim := goal.TargetClaim()
	targetCache, hasTargetCache := goal.TargetCache()

	cacheSignatures := make([]cacheSignature, 0, len(topo.Caches()))
	for _, cacheID := range topo.Caches() {
		cacheCPUs := topo.CPUsInCache(cacheID)
		freeInC := free.Intersection(cacheCPUs).Size()

		var occupants []claimOccupant
		for uid, cpus := range placements {
			inC := cpus.Intersection(cacheCPUs)
			if inC.IsEmpty() {
				continue
			}
			isTarget := hasTargetClaim && uid == targetClaim
			occupants = append(occupants, claimOccupant{
				size:        inC.Size(),
				relocatable: opts.eligible(uid),
				isTarget:    isTarget,
			})
		}
		sort.Slice(occupants, func(i, j int) bool {
			if occupants[i].size != occupants[j].size {
				return occupants[i].size < occupants[j].size
			}
			if occupants[i].relocatable != occupants[j].relocatable {
				return !occupants[i].relocatable && occupants[j].relocatable
			}
			return !occupants[i].isTarget && occupants[j].isTarget
		})

		cacheSignatures = append(cacheSignatures, cacheSignature{
			cacheID:       cacheID,
			capacity:      cacheCPUs.Size(),
			freeCPUs:      freeInC,
			isTargetCache: hasTargetCache && cacheID == targetCache,
			occupants:     occupants,
		})
	}

	if canonical {
		sort.Slice(cacheSignatures, func(i, j int) bool {
			return cacheSignatures[i].string(true) < cacheSignatures[j].string(true)
		})
	}

	var sb strings.Builder
	for i, cs := range cacheSignatures {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(cs.string(canonical))
	}
	return sb.String()
}

type searchState struct {
	placements    map[types.UID]cpuset.CPUSet
	free          cpuset.CPUSet
	moves         []Move
	displacedCPUs int
	depth         int
	exchangeCount int
}

// ExactSearch finds the shortest legal move sequence to satisfy the goal within budget.
func ExactSearch(topo *Topology, placements []Placement, free, inFlight cpuset.CPUSet, goal Goal, sel Selector, opts ExactOptions) (ExactPlan, error) {
	return exactSearchInternal(topo, placements, free, inFlight, goal, sel, opts, true)
}

// ExactSearchUncanonicalised runs the exact search without cache permutation canonicalisation (oracle).
func ExactSearchUncanonicalised(topo *Topology, placements []Placement, free, inFlight cpuset.CPUSet, goal Goal, sel Selector, opts ExactOptions) (ExactPlan, error) {
	return exactSearchInternal(topo, placements, free, inFlight, goal, sel, opts, false)
}

func exactSearchInternal(topo *Topology, placements []Placement, free, inFlight cpuset.CPUSet, goal Goal, sel Selector, opts ExactOptions, canonical bool) (ExactPlan, error) {
	for _, p := range placements {
		if !p.CPUs.IsSubsetOf(topo.CPUs()) {
			return ExactPlan{}, fmt.Errorf("claim %q holds CPUs %q not in NUMA node %d (%q)",
				p.ClaimUID, p.CPUs.String(), topo.NUMANodeID(), topo.CPUs().String())
		}
	}

	effectiveFree := free.Intersection(topo.CPUs()).Difference(inFlight)
	maxDepth := opts.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 3
	}
	budget := opts.Budget
	if budget <= 0 {
		budget = DerivedBudget(topo, placements, maxDepth, opts.Eligible)
	}

	initialPlacements := make(map[types.UID]cpuset.CPUSet, len(placements))
	for _, p := range placements {
		initialPlacements[p.ClaimUID] = p.CPUs
	}

	plan := ExactPlan{
		NUMANodeID: topo.NUMANodeID(),
		Goal:       goal,
		Budget:     budget,
	}

	if goal.IsSatisfied(topo, initialPlacements, effectiveFree) {
		plan.Status = SearchReachable
		plan.ComputeClosure()
		return plan, nil
	}

	type queueItem struct {
		state *searchState
	}

	queue := []*searchState{
		{
			placements:    initialPlacements,
			free:          effectiveFree,
			moves:         nil,
			displacedCPUs: 0,
			depth:         0,
			exchangeCount: 0,
		},
	}

	visited := map[string]int{}
	initKey := stateKey(topo, initialPlacements, effectiveFree, goal, opts, canonical)
	visited[initKey] = 0

	workCount := 0
	var bestSolution *searchState

	// Deterministic sorted list of claim UIDs
	claimUIDs := make([]types.UID, 0, len(initialPlacements))
	for uid := range initialPlacements {
		claimUIDs = append(claimUIDs, uid)
	}
	sort.Slice(claimUIDs, func(i, j int) bool { return claimUIDs[i] < claimUIDs[j] })

	caches := topo.Caches()
	sort.Ints(caches)

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		// If a best solution was found at an earlier depth than curr.depth, we are done
		// because BFS guarantees non-decreasing depth.
		if bestSolution != nil && curr.depth > bestSolution.depth {
			break
		}

		if curr.depth >= maxDepth {
			continue
		}

		workCount++
		if workCount > budget {
			plan.Status = SearchNotProven
			plan.WorkCount = workCount
			return plan, nil
		}

		// 1. Single claim relocations into free CPUs
		for _, uid := range claimUIDs {
			if !opts.eligible(uid) {
				continue
			}
			currentCPUs := curr.placements[uid]
			currSpread := topo.Spread(currentCPUs)

			for _, cacheID := range caches {
				cacheCPUs := topo.CPUsInCache(cacheID)
				inC := currentCPUs.Intersection(cacheCPUs)
				if inC.Size() == currentCPUs.Size() {
					// Already wholly in cacheID
					continue
				}
				needed := currentCPUs.Size() - inC.Size()
				freeInC := curr.free.Intersection(cacheCPUs)
				if freeInC.Size() < needed {
					continue
				}

				var picked cpuset.CPUSet
				if sel != nil {
					p, err := sel(freeInC, needed)
					if err != nil {
						continue
					}
					picked = p
				} else {
					picked = cpuset.New(freeInC.List()[:needed]...)
				}

				target := inC.Union(picked)
				if target.Size() != currentCPUs.Size() {
					continue
				}
				// Move must not increase spread
				if topo.Spread(target) > currSpread {
					continue
				}

				newPlacements := make(map[types.UID]cpuset.CPUSet, len(curr.placements))
				for k, v := range curr.placements {
					newPlacements[k] = v
				}
				newPlacements[uid] = target
				newFree := curr.free.Difference(picked).Union(currentCPUs.Difference(inC))

				move := Move{
					ClaimUID: uid,
					From:     currentCPUs,
					To:       target,
					Exchange: 0,
				}
				newMoves := make([]Move, len(curr.moves), len(curr.moves)+1)
				copy(newMoves, curr.moves)
				newMoves = append(newMoves, move)

				newState := &searchState{
					placements:    newPlacements,
					free:          newFree,
					moves:         newMoves,
					displacedCPUs: curr.displacedCPUs + needed,
					depth:         curr.depth + 1,
					exchangeCount: curr.exchangeCount,
				}

				if goal.IsSatisfied(topo, newPlacements, newFree) {
					if bestSolution == nil || isBetterSolution(newState, bestSolution) {
						bestSolution = newState
					}
					continue
				}

				key := stateKey(topo, newPlacements, newFree, goal, opts, canonical)
				if prevDepth, seen := visited[key]; !seen || newState.depth < prevDepth {
					visited[key] = newState.depth
					queue = append(queue, newState)
				}
			}
		}

		// 2. Swaps between two eligible claims
		if opts.AllowSwaps {
			for i := 0; i < len(claimUIDs); i++ {
				u1 := claimUIDs[i]
				if !opts.eligible(u1) {
					continue
				}
				for j := i + 1; j < len(claimUIDs); j++ {
					u2 := claimUIDs[j]
					if !opts.eligible(u2) {
						continue
					}
					p1 := Placement{ClaimUID: u1, CPUs: curr.placements[u1]}
					p2 := Placement{ClaimUID: u2, CPUs: curr.placements[u2]}
					pair := []Placement{p1, p2}

					var targets map[types.UID]cpuset.CPUSet
					var ok bool
					if sel != nil {
						targets, ok = recut(pair, sel)
					} else {
						// Simple fallback if sel is nil
						remaining := p1.CPUs.Union(p2.CPUs)
						t1 := cpuset.New(remaining.List()[:p1.CPUs.Size()]...)
						t2 := remaining.Difference(t1)
						targets = map[types.UID]cpuset.CPUSet{u1: t1, u2: t2}
						ok = true
					}
					if !ok {
						continue
					}

					t1, t2 := targets[u1], targets[u2]
					// Reject no-op
					if t1.Equals(p1.CPUs) && t2.Equals(p2.CPUs) {
						continue
					}
					// Neither participant's spread may worsen
					if topo.ExcessSpread(t1) > topo.ExcessSpread(p1.CPUs) ||
						topo.ExcessSpread(t2) > topo.ExcessSpread(p2.CPUs) {
						continue
					}
					// Total excess spread must strictly improve
					if topo.ExcessSpread(t1)+topo.ExcessSpread(t2) >=
						topo.ExcessSpread(p1.CPUs)+topo.ExcessSpread(p2.CPUs) {
						continue
					}

					newPlacements := make(map[types.UID]cpuset.CPUSet, len(curr.placements))
					for k, v := range curr.placements {
						newPlacements[k] = v
					}
					newPlacements[u1] = t1
					newPlacements[u2] = t2

					nextEx := curr.exchangeCount + 1
					m1 := Move{ClaimUID: u1, From: p1.CPUs, To: t1, Exchange: nextEx}
					m2 := Move{ClaimUID: u2, From: p2.CPUs, To: t2, Exchange: nextEx}

					newMoves := make([]Move, len(curr.moves), len(curr.moves)+2)
					copy(newMoves, curr.moves)
					newMoves = append(newMoves, m1, m2)

					newState := &searchState{
						placements:    newPlacements,
						free:          curr.free,
						moves:         newMoves,
						displacedCPUs: curr.displacedCPUs + p1.CPUs.Difference(t1).Size() + p2.CPUs.Difference(t2).Size(),
						depth:         curr.depth + 1,
						exchangeCount: nextEx,
					}

					if goal.IsSatisfied(topo, newPlacements, curr.free) {
						if bestSolution == nil || isBetterSolution(newState, bestSolution) {
							bestSolution = newState
						}
						continue
					}

					key := stateKey(topo, newPlacements, curr.free, goal, opts, canonical)
					if prevDepth, seen := visited[key]; !seen || newState.depth < prevDepth {
						visited[key] = newState.depth
						queue = append(queue, newState)
					}
				}
			}
		}
	}

	plan.WorkCount = workCount
	if bestSolution != nil {
		plan.Status = SearchReachable
		plan.Moves = bestSolution.moves
		plan.DisplacedCPUs = bestSolution.displacedCPUs
		plan.ComputeClosure()
		return plan, nil
	}

	plan.Status = SearchUnreachable
	return plan, nil
}

func isBetterSolution(a, b *searchState) bool {
	if a.depth != b.depth {
		return a.depth < b.depth
	}
	tenantsA := tenantsMoved(a.moves)
	tenantsB := tenantsMoved(b.moves)
	if tenantsA != tenantsB {
		return tenantsA < tenantsB
	}
	return a.displacedCPUs < b.displacedCPUs
}

func tenantsMoved(moves []Move) int {
	seen := map[types.UID]struct{}{}
	for _, m := range moves {
		seen[m.ClaimUID] = struct{}{}
	}
	return len(seen)
}
