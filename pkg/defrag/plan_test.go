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

package defrag_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// selectorFor is the allocator the driver itself places claims with, so a plan
// is tested against real target selection rather than a stand-in.
func selectorFor(topo *cpuinfo.CPUTopology) defrag.Selector {
	return func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		return coreselect.TakeWholeCores(topo, available, numCPUs)
	}
}

func placement(claimUID types.UID, cpus ...int) defrag.Placement {
	return defrag.Placement{ClaimUID: claimUID, CPUs: cpuset.New(cpus...)}
}

func totalCPUs(placements []defrag.Placement) cpuset.CPUSet {
	all := cpuset.New()
	for _, p := range placements {
		all = all.Union(p.CPUs)
	}
	return all
}

// requireValidPlan checks the guarantees every plan must offer, whatever the
// node looks like: sizes preserved, targets independent of each other and of the
// claims left alone, and no claim moved to a worse place than it already had.
func requireValidPlan(t *testing.T, dtopo *defrag.Topology, placements []defrag.Placement, free cpuset.CPUSet, plan defrag.Plan) {
	t.Helper()

	current := map[types.UID]cpuset.CPUSet{}
	for _, p := range placements {
		current[p.ClaimUID] = p.CPUs
	}

	seenTargets := cpuset.New()
	for _, move := range plan.Moves {
		from, ok := current[move.ClaimUID]
		require.True(t, ok, "plan moves unknown claim %q", move.ClaimUID)
		require.True(t, from.Equals(move.From), "move of %q starts from %q, not its %q",
			move.ClaimUID, move.From.String(), from.String())
		require.Equal(t, move.From.Size(), move.To.Size(),
			"move of %q changes its CPU count", move.ClaimUID)
		require.True(t, move.To.IsSubsetOf(dtopo.CPUs()),
			"move of %q targets CPUs outside the node", move.ClaimUID)
		require.True(t, move.To.IsSubsetOf(free.Union(from)),
			"move of %q to %q is not independent: free is %q", move.ClaimUID, move.To.String(), free.String())
		require.True(t, move.To.Intersection(seenTargets).IsEmpty(),
			"move of %q to %q overlaps another move's target", move.ClaimUID, move.To.String())
		require.LessOrEqual(t, dtopo.ExcessSpread(move.To), dtopo.ExcessSpread(from),
			"move of %q leaves it worse placed", move.ClaimUID)
		seenTargets = seenTargets.Union(move.To)
	}
}

// applyPlan returns the placements and free CPUs after a plan is carried out.
func applyPlan(placements []defrag.Placement, free cpuset.CPUSet, plan defrag.Plan) ([]defrag.Placement, cpuset.CPUSet) {
	moved := map[types.UID]cpuset.CPUSet{}
	for _, move := range plan.Moves {
		moved[move.ClaimUID] = move.To
		free = free.Union(move.From)
	}
	for _, move := range plan.Moves {
		free = free.Difference(move.To)
	}

	next := make([]defrag.Placement, 0, len(placements))
	for _, p := range placements {
		if to, ok := moved[p.ClaimUID]; ok {
			next = append(next, defrag.Placement{ClaimUID: p.ClaimUID, CPUs: to})
			continue
		}
		next = append(next, p)
	}
	return next, free
}

// converge runs passes until no move is left, checking every plan on the way. It
// gives up well before any correct planner would need the passes, so a planner
// that fails to make progress fails the test instead of looping.
func converge(t *testing.T, topo *cpuinfo.CPUTopology, dtopo *defrag.Topology, placements []defrag.Placement, free cpuset.CPUSet, opts defrag.Options) (passes int, _ []defrag.Placement, _ cpuset.CPUSet) {
	t.Helper()
	const limit = 20
	held := totalCPUs(placements)
	universe := held.Union(free)
	scores := make([]int, 0, limit)

	for passes = range limit {
		plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), opts)
		require.NoError(t, err)
		requireValidPlan(t, dtopo, placements, free, plan)
		if len(plan.Moves) == 0 {
			return passes, placements, free
		}
		placements, free = applyPlan(placements, free, plan)

		// Whatever moved, the node still accounts for exactly the same CPUs, no
		// claim shares one with another, and every claim keeps its size.
		require.Equal(t, held.Size(), totalCPUs(placements).Size())
		require.True(t, totalCPUs(placements).Union(free).Equals(universe))
		require.True(t, totalCPUs(placements).Intersection(free).IsEmpty())

		// Passes must make progress, or this loop would only be bounded by its
		// own limit.
		score := dtopo.Cost(placements)*universe.Size() + free.Size()
		scores = append(scores, score)
		if passes > 0 {
			require.LessOrEqual(t, score, scores[passes-1], "pass %d undid the one before it", passes)
		}
	}
	t.Fatalf("no convergence after %d passes", limit)
	return 0, nil, cpuset.New()
}

func TestPlanNodeLeavesAPackedNodeAlone(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	// Each claim sits inside a single cache, which is the best its size allows.
	placements := []defrag.Placement{
		placement("claim-1", 0, 1, 2, 3),
		placement("claim-2", 4, 5),
		placement("claim-3", 8, 9, 10, 11),
	}
	free := cpuset.New(6, 7, 12, 13, 14, 15)

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.Empty(t, plan.Moves)
	require.Equal(t, 0, plan.CurrentCost)
	require.Equal(t, "node is already packed as well as its claims allow", plan.Reason)
}

func TestPlanNodeLeavesSmallClaimsTheirOwnCachesWhileThereIsSlack(t *testing.T) {
	// Four claims, one per cache, each as well placed as its size allows. A
	// tighter packing exists but is worth nothing yet, so nothing is disturbed:
	// consolidation is what a large misplaced claim buys, not an end in itself.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("claim-1", 0, 1),
		placement("claim-2", 4, 5),
		placement("claim-3", 8, 9),
		placement("claim-4", 12, 13),
	}
	free := cpuset.New(2, 3, 6, 7, 10, 11, 14, 15)

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.Empty(t, plan.Moves)
	require.Equal(t, plan.CurrentCost, plan.IdealCost)
}

func TestPlanNodeConsolidatesSmallClaimsToRepairALargeOne(t *testing.T) {
	// The mixed case: four small claims hold a slice of every cache, and an
	// 8-CPU claim that wanted one cache has been scattered over all four. Only
	// repacking the small claims can give it a cache, so they move even though
	// each is individually well placed.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("big", 1, 2, 5, 6, 9, 10, 13, 14),
		placement("small-1", 0),
		placement("small-2", 4),
		placement("small-3", 8),
		placement("small-4", 12),
	}
	free := cpuset.New(3, 7, 11, 15)

	before := dtopo.Cost(placements)
	require.Equal(t, 2, before, "the big claim spans four caches where two would do")

	passes, final, _ := converge(t, topo, dtopo, placements, free, defrag.Options{})
	require.Positive(t, passes)

	after := dtopo.Cost(final)
	require.Less(t, after, before)
	require.Equal(t, 0, after)
	for _, p := range final {
		if p.ClaimUID == "big" {
			require.Equal(t, 2, dtopo.Spread(p.CPUs), "the big claim ends on the fewest caches it can")
		}
	}
}

func TestPlanNodeEscapesTheBlockedLargeClaimDeadlock(t *testing.T) {
	// Every cache holds a slice of the large claim and a small claim beside it.
	// The large claim's ideal cache is occupied, so its move is not independent;
	// and moving a small claim changes no cost at all, so a planner following
	// cost alone would emit nothing here and stay stuck forever. Counting the
	// CPUs a small claim occupies in the large claim's ideal slot makes those
	// cost-neutral moves worth taking, and they unblock the large one.
	topo := topologyOf([]int{6, 6, 6, 6})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	// Three CPUs of the large claim in each of the four caches, one small claim
	// beside it in every cache, two CPUs spare there. Two caches would hold the
	// large claim, but not even counting its own CPUs can any two of them free
	// twelve while the small claims stay put.
	placements := []defrag.Placement{
		placement("big", 0, 1, 2, 6, 7, 8, 12, 13, 14, 18, 19, 20),
		placement("small-1", 3),
		placement("small-2", 9),
		placement("small-3", 15),
		placement("small-4", 21),
	}
	free := cpuset.New(4, 5, 10, 11, 16, 17, 22, 23)

	require.Equal(t, 2, dtopo.Cost(placements))
	require.Equal(t, 2, dtopo.MinSpread(12))
	require.Equal(t, 4, dtopo.Spread(cpuset.New(0, 1, 2, 6, 7, 8, 12, 13, 14, 18, 19, 20)))

	// The large claim cannot reach the cache it wants on the first pass, and the
	// small claims standing in the way have no spread of their own to gain by
	// moving. The pass must still do something, or nothing ever would.
	first, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.NotEmpty(t, first.Moves, "a pass that cannot reach the ideal is still progress towards it")
	require.Subset(t, claimUIDsOf(first.Moves), []types.UID{"small-1"},
		"a claim in the way steps aside even though its own placement is already as good as it gets")
	for _, move := range first.Moves {
		if move.ClaimUID == "big" {
			require.Greater(t, dtopo.Spread(move.To), 2, "the ideal cache is still occupied")
		}
	}

	passes, final, _ := converge(t, topo, dtopo, placements, free, defrag.Options{})
	require.Equal(t, 0, dtopo.Cost(final))
	require.GreaterOrEqual(t, passes, 2, "the large claim cannot move on the first pass")
	for _, p := range final {
		if p.ClaimUID == "big" {
			require.Equal(t, 2, dtopo.Spread(p.CPUs))
		}
	}
}

func TestPlanNodeSkipsAZeroSlackCycle(t *testing.T) {
	// Two claims each sit exactly where the other belongs and there is no free
	// CPU to stage a swap through, so neither move can be made independently.
	// Nothing is emitted and the impossibility is reported.
	topo := topologyOf([]int{2, 2})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("claim-1", 0, 3),
		placement("claim-2", 1, 2),
	}
	free := cpuset.New()

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	requireValidPlan(t, dtopo, placements, free, plan)
	require.Empty(t, plan.Moves)
	require.Positive(t, plan.Blocked)
	require.Contains(t, plan.Reason, "blocked")
}

func TestPlanNodeConsolidatesCheckerboardedFreeSpace(t *testing.T) {
	// Free CPUs are scattered one per cache, so no cache can be handed out whole
	// until the claims are repacked. The claim that spans more caches than it has
	// to is what makes the repack worth doing.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("claim-1", 0, 4, 8, 12),
		placement("claim-2", 1, 5, 9, 13),
		placement("claim-3", 2, 6, 10, 14),
	}
	free := cpuset.New(3, 7, 11, 15)

	require.Equal(t, 9, dtopo.Cost(placements), "each claim wastes three caches")
	_, final, finalFree := converge(t, topo, dtopo, placements, free, defrag.Options{})
	require.Equal(t, free.Size(), finalFree.Size())
	require.Equal(t, 0, dtopo.Cost(final))

	// A whole cache is free at the end, which is the point of consolidating.
	freeWholeCaches := 0
	for _, cacheID := range dtopo.Caches() {
		if dtopo.CPUsInCache(cacheID).IsSubsetOf(finalFree) {
			freeWholeCaches++
		}
	}
	require.Positive(t, freeWholeCaches, "free CPUs are still scattered across caches")
}

func TestPlanNodeIsDeterministic(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("big", 1, 2, 5, 6, 9, 10, 13, 14),
		placement("small-1", 0),
		placement("small-2", 4),
		placement("small-3", 8),
		placement("small-4", 12),
	}
	free := cpuset.New(3, 7, 11, 15)

	first, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	for range 16 {
		got, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
		require.NoError(t, err)
		require.Equal(t, first, got)
	}

	// Claim order must not change the plan either: it comes from a map.
	shuffled := []defrag.Placement{placements[3], placements[0], placements[4], placements[1], placements[2]}
	got, err := defrag.PlanNode(dtopo, shuffled, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.ElementsMatch(t, first.Moves, got.Moves)
}

func TestPlanNodeMovesEveryClaimARepairNeedsAtOnce(t *testing.T) {
	// Three claims each straddling four caches, all repairable through the one
	// free cache. Nothing bounds a plan but what the node can reach, so a repair
	// that takes several moves is one plan rather than one move per pass.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("claim-1", 0, 4, 8, 12),
		placement("claim-2", 1, 5, 9, 13),
		placement("claim-3", 2, 6, 10, 14),
	}
	free := cpuset.New(3, 7, 11, 15)

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.Greater(t, len(plan.Moves), 1)
	requireValidPlan(t, dtopo, placements, free, plan)

	_, final, _ := converge(t, topo, dtopo, placements, free, defrag.Options{})
	require.Equal(t, 0, dtopo.Cost(final))
}

func TestPlanNodeMovesForAGainOfOneCache(t *testing.T) {
	// One claim straddling two caches where one would hold it. That is the
	// smallest improvement a node can offer, and the only gate on a plan is
	// whether an improvement exists at all, so it is taken.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("split", 0, 4),
		placement("other", 8, 9),
	}
	free := cpuset.New(1, 2, 3, 5, 6, 7, 10, 11, 12, 13, 14, 15)

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.Equal(t, 1, plan.CurrentCost-plan.IdealCost)
	require.NotEmpty(t, plan.Moves)
	requireValidPlan(t, dtopo, placements, free, plan)
}

func TestPlanNodeTreatsIneligibleClaimsAsObstacles(t *testing.T) {
	// A claim that never asked to be moved must not be, and the ideal has to be built
	// around it: an ideal that keeps calling for a move the plan can never make
	// would block every claim behind it forever.
	topo := topologyOf([]int{4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("big", 1, 2, 5, 6, 9, 10, 13, 14),
		placement("pinned", 0),
		placement("small-2", 4),
		placement("small-3", 8),
		placement("small-4", 12),
	}
	free := cpuset.New(3, 7, 11, 15)

	opts := defrag.Options{Eligible: func(claimUID types.UID) bool { return claimUID != "pinned" }}
	passes, final, _ := converge(t, topo, dtopo, placements, free, opts)
	require.Positive(t, passes)

	for _, p := range final {
		if p.ClaimUID == "pinned" {
			require.Equal(t, cpuset.New(0), p.CPUs, "a claim held back must not move")
		}
	}
	require.Less(t, dtopo.Cost(final), dtopo.Cost(placements), "the rest still improves around it")
}

func TestPlanNodeKeepsTheSharedPoolNonEmpty(t *testing.T) {
	// While a move is in flight its claim holds both its old and its new CPUs.
	// If a plan claimed every free CPU at once, the shared pool would momentarily
	// be empty, and NRI has no way to express an empty cpuset.
	topo := topologyOf([]int{4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("split-1", 0, 4),
		placement("split-2", 1, 5),
		placement("split-3", 2, 6),
	}
	free := cpuset.New(3, 7)

	unconstrained, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	require.True(t, free.Difference(targetsIn(unconstrained)).IsEmpty(),
		"this plan is only interesting because it would take every free CPU")

	guarded, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{KeepFreePoolNonEmpty: true})
	require.NoError(t, err)
	requireValidPlan(t, dtopo, placements, free, guarded)
	require.False(t, free.Difference(targetsIn(guarded)).IsEmpty(), "no CPU is left for shared containers")
	require.Contains(t, guarded.Reason, "shared pool")
}

func TestPlanNodeTakesWholeCoresWhenTheSelectorDoes(t *testing.T) {
	// 2 caches x 2 cores x 2 threads: CPUs 0-3 are the first thread of cores
	// 0-3, CPUs 4-7 their siblings; cores 0-1 share cache 0.
	topo := smtTopology(2, 2)
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("split", 0, 4, 2, 6),
	}
	free := cpuset.New(1, 5, 3, 7)

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	requireValidPlan(t, dtopo, placements, free, plan)
	require.NotEmpty(t, plan.Moves)
	for _, move := range plan.Moves {
		require.Equal(t, move.To, topo.CPUDetails.CompleteCores(move.To),
			"move to %q splits a physical core", move.To.String())
	}
}

func TestPlanNodeRejectsPlacementsOutsideTheNode(t *testing.T) {
	topo := topologyOf([]int{4, 4}, []int{4, 4})
	// Only node 0, so CPU 8 belongs to another node; CPU 0 is reserved here.
	dtopo := requireTopology(t, topo, 0, allBut(topo, 0))

	testCases := []struct {
		name  string
		claim defrag.Placement
	}{
		{name: "a CPU from another NUMA node", claim: placement("claim-1", 1, 8)},
		{name: "a reserved CPU", claim: placement("claim-1", 0, 1)},
		{name: "a CPU that does not exist", claim: placement("claim-1", 1, 99)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := defrag.PlanNode(dtopo, []defrag.Placement{tc.claim}, cpuset.New(2, 3), selectorFor(topo), defrag.Options{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "not allocatable in NUMA node 0")
		})
	}
}

func claimUIDsOf(moves []defrag.Move) []types.UID {
	uids := make([]types.UID, 0, len(moves))
	for _, move := range moves {
		uids = append(uids, move.ClaimUID)
	}
	return uids
}

func targetsIn(plan defrag.Plan) cpuset.CPUSet {
	targets := cpuset.New()
	for _, move := range plan.Moves {
		targets = targets.Union(move.To)
	}
	return targets
}

// smtTopology returns caches x coresPerCache cores of two threads on one NUMA
// node. Core c is CPUs c and c+cores.
func smtTopology(caches, coresPerCache int) *cpuinfo.CPUTopology {
	cores := caches * coresPerCache
	details := cpuinfo.CPUDetails{}
	for cpu := range cores * 2 {
		core := cpu % cores
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        core,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: core / coresPerCache,
			SiblingCPUID:  (cpu + cores) % (cores * 2),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: cores * 2, NumCores: cores, NumSockets: 1, NumNUMANodes: 1,
		NumUncoreCache: caches, SMTEnabled: true, CPUDetails: details,
	}
}

func TestPlanNodeAlwaysConvergesFromRandomLayouts(t *testing.T) {
	// The invariants and the convergence bound are checked on every pass by
	// converge, so this looks for the layout that breaks one of them: a plan that
	// churns, oscillates, or proposes CPUs it may not have.
	//
	// The topology is randomised along with the placements, and deliberately
	// includes unequal cache sizes: only where caches differ can the ideal's
	// leftovers straddle a cache boundary and exercise the two-candidate rule,
	// which a fleet of equal 4-CPU caches would never do.
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed seed is the point: failures must be reproducible

	for round := range 300 {
		caches := make([]int, 2+rng.IntN(3))
		for i := range caches {
			caches[i] = 2 + rng.IntN(7)
		}
		topo := topologyOf(caches)
		dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())
		all := topo.CPUDetails.CPUs().List()
		rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

		var placements []defrag.Placement
		taken := 0
		for claim := range 6 {
			size := 1 + rng.IntN(max(2, len(all)/2))
			if taken+size > len(all)-1 {
				break
			}
			placements = append(placements, defrag.Placement{
				ClaimUID: types.UID(fmt.Sprintf("claim-%d", claim)),
				CPUs:     cpuset.New(all[taken : taken+size]...),
			})
			taken += size
		}
		if len(placements) == 0 {
			continue
		}
		free := cpuset.New(all[taken:]...)

		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			_, final, finalFree := converge(t, topo, dtopo, placements, free, defrag.Options{})
			require.LessOrEqual(t, dtopo.Cost(final), dtopo.Cost(placements))
			require.Equal(t, free.Size(), finalFree.Size())
		})
	}
}

// TestPlanNodeTakesACacheItsIdealPromisedToAnother pins how directly a claim
// reaches a whole cache when its own ideal slot is unreachable.
//
// The shape is one a real node produced. The caches are unequal, so the ideal
// packs the larger claim into the smaller cache that still fits it and sends the
// straddling claim to the other one -- and neither can get there, each blocked
// by the other. The ideal's leftovers then hold exactly the straddling claim's
// size, spanning both caches, so preferring them means moving it from one split
// placement to another and waiting for the larger claim to shuffle out of the way
// first.
//
// Offering the wider pool as well lets it take the free cache at once, which it
// can only do by stepping into the slot the ideal promised to the larger claim.
// Both routes reach zero cost; this one disturbs one claim instead of two, and a
// claim moved without improving its own placement is a container re-pinned for
// nothing.
func TestPlanNodeTakesACacheItsIdealPromisedToAnother(t *testing.T) {
	topo := topologyOf([]int{7, 8})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	big := cpuset.New(7, 8, 9, 10, 11, 12)
	straddling := cpuset.New(5, 6, 13)
	placements := []defrag.Placement{
		{ClaimUID: "big", CPUs: big},
		{ClaimUID: "straddling", CPUs: straddling},
	}
	free := topo.CPUDetails.CPUs().Difference(big.Union(straddling))

	require.Equal(t, cpuset.New(0, 1, 2, 3, 4, 14), free)
	require.Equal(t, 1, dtopo.Cost(placements), "only the straddling claim wastes a cache")
	require.Equal(t, 1, dtopo.MinSpread(straddling.Size()), "its size fits in one cache")

	plan, err := defrag.PlanNode(dtopo, placements, free, selectorFor(topo), defrag.Options{})
	require.NoError(t, err)
	requireValidPlan(t, dtopo, placements, free, plan)
	require.Len(t, plan.Moves, 1)
	require.Equal(t, types.UID("straddling"), plan.Moves[0].ClaimUID,
		"the claim that is already whole must not be the one disturbed")
	require.Equal(t, 1, dtopo.Spread(plan.Moves[0].To),
		"the move must land in one cache, not shuffle between two split placements")

	passes, final, _ := converge(t, topo, dtopo, placements, free, defrag.Options{})
	require.Equal(t, 1, passes, "one pass should settle it")
	require.Equal(t, 0, dtopo.Cost(final))
	for _, p := range final {
		require.Equal(t, 1, dtopo.Spread(p.CPUs), "claim %s should end in one cache", p.ClaimUID)
	}
}

// TestPlanNodeMovesOnlyTheClaimsARepairNeeds: the confetti case. Every cache
// holds one well-placed small claim, a straddling claim needs two of those
// caches, and the repair may touch exactly the straddler and the two smalls in
// its way. The ideal layout would also tidy the other smalls into its preferred
// slots; tidiness is not worth a running workload's cpuset changing, so those
// moves must not ride along in the passes the repair keeps the cost gate open.
func TestPlanNodeMovesOnlyTheClaimsARepairNeeds(t *testing.T) {
	topo := topologyOf([]int{8, 8, 8, 8, 8, 8, 8, 8})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	placements := []defrag.Placement{
		placement("big", 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 20, 21, 22, 23),
	}
	for i := range 8 {
		placements = append(placements, placement(types.UID(fmt.Sprintf("small-%d", i)), i*8, i*8+1))
	}
	free := topo.CPUDetails.CPUs().Difference(totalCPUs(placements))
	require.Equal(t, 1, dtopo.Cost(placements), "the big claim spans three caches where two would do")

	moved := map[types.UID]int{}
	current := placements
	for pass := 0; pass < 10; pass++ {
		plan, err := defrag.PlanNode(dtopo, current, free, selectorFor(topo), defrag.Options{})
		require.NoError(t, err)
		requireValidPlan(t, dtopo, current, free, plan)
		if len(plan.Moves) == 0 {
			break
		}
		for _, move := range plan.Moves {
			moved[move.ClaimUID]++
		}
		current, free = applyPlan(current, free, plan)
	}

	require.Equal(t, 0, dtopo.Cost(current), "the node did not end aligned")
	require.Positive(t, moved["big"], "the straddling claim is the repair's whole point")
	for i := 2; i < 8; i++ {
		uid := types.UID(fmt.Sprintf("small-%d", i))
		require.Zero(t, moved[uid], "small-%d was not in the repair's way and must not move", i)
	}
	byUID := map[types.UID]cpuset.CPUSet{}
	for _, p := range current {
		byUID[p.ClaimUID] = p.CPUs
	}
	for i := 2; i < 8; i++ {
		uid := types.UID(fmt.Sprintf("small-%d", i))
		require.True(t, byUID[uid].Equals(cpuset.New(i*8, i*8+1)),
			"small-%d must keep its exact CPUs, has %s", i, byUID[uid].String())
	}
}

// TestPlanNodeDisplacedClaimsFollowTheSpreadPolicy: under the spread selector a
// bystander evicted by a repair lands in the emptiest cache, so the one-tenant-
// per-cache layout survives its own repair wherever there is room for it.
func TestPlanNodeDisplacedClaimsFollowTheSpreadPolicy(t *testing.T) {
	topo := topologyOf([]int{4, 4, 4, 4, 4, 4})
	dtopo := requireTopology(t, topo, 0, topo.CPUDetails.CPUs())

	spreadSel := func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		return coreselect.TakeWholeCoresPolicy(topo, available, numCPUs, coreselect.Spread, cpuset.New())
	}

	placements := []defrag.Placement{
		placement("big", 2, 3, 6, 7, 10, 11, 14, 15),
		placement("small-1", 0, 1),
		placement("small-2", 4, 5),
		placement("small-3", 8, 9),
		placement("small-4", 12, 13),
	}
	free := cpuset.New(16, 17, 18, 19, 20, 21, 22, 23)
	require.Positive(t, dtopo.Cost(placements))

	current := placements
	for pass := 0; pass < 10; pass++ {
		plan, err := defrag.PlanNode(dtopo, current, free, spreadSel, defrag.Options{})
		require.NoError(t, err)
		requireValidPlan(t, dtopo, current, free, plan)
		if len(plan.Moves) == 0 {
			break
		}
		current, free = applyPlan(current, free, plan)
	}

	require.Equal(t, 0, dtopo.Cost(current), "the node did not end aligned")
	tenants := map[int]int{}
	for _, p := range current {
		if p.ClaimUID == "big" {
			continue
		}
		require.Equal(t, 1, dtopo.Spread(p.CPUs), "small %s ended split", p.ClaimUID)
		for _, cpu := range p.CPUs.List() {
			tenants[topo.CPUDetails[cpu].UncoreCacheID]++
			break
		}
	}
	for cache, n := range tenants {
		require.Equal(t, 1, n, "cache %d hosts %d smalls; with six caches and a two-cache claim, spread has room for one each", cache, n)
	}
}
