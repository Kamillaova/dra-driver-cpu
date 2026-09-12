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
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// fakeTopology builds a Topology with numCaches caches, each having cpusPerCache CPUs.
func fakeTopology(numCaches, cpusPerCache int) *Topology {
	t, _ := fakeTopologyWithCPUInfo(numCaches, cpusPerCache)
	return t
}

func fakeTopologyWithCPUInfo(numCaches, cpusPerCache int) (*Topology, *cpuinfo.CPUTopology) {
	details := cpuinfo.CPUDetails{}
	cpuID := 0
	for c := 0; c < numCaches; c++ {
		for i := 0; i < cpusPerCache; i++ {
			details[cpuID] = cpuinfo.CPUInfo{
				NUMANodeID:    0,
				SocketID:      0,
				CoreID:        cpuID,
				UncoreCacheID: c,
			}
			cpuID++
		}
	}
	topo := &cpuinfo.CPUTopology{CPUDetails: details}
	allCPUs := cpuset.New()
	for id := 0; id < cpuID; id++ {
		allCPUs = allCPUs.Union(cpuset.New(id))
	}
	t, err := NewTopology(topo, 0, allCPUs)
	if err != nil {
		panic(err)
	}
	return t, topo
}

// fakeUnequalTopology builds a Topology with specified CPUs per cache.
func fakeUnequalTopology(cacheSizes []int) *Topology {
	details := cpuinfo.CPUDetails{}
	cpuID := 0
	for c, size := range cacheSizes {
		for i := 0; i < size; i++ {
			details[cpuID] = cpuinfo.CPUInfo{
				NUMANodeID:    0,
				SocketID:      0,
				CoreID:        cpuID,
				UncoreCacheID: c,
			}
			cpuID++
		}
	}
	topo := &cpuinfo.CPUTopology{CPUDetails: details}
	allCPUs := cpuset.New()
	for id := 0; id < cpuID; id++ {
		allCPUs = allCPUs.Union(cpuset.New(id))
	}
	t, err := NewTopology(topo, 0, allCPUs)
	if err != nil {
		panic(err)
	}
	return t
}

func parseCPUSet(s string) cpuset.CPUSet {
	set, err := cpuset.Parse(s)
	if err != nil {
		panic(err)
	}
	return set
}

// TestThreeMoveCounterexample verifies that the exact bounded search finds the 3-move staging
// sequence that the greedy planner provably cannot find.
func TestThreeMoveCounterexample(t *testing.T) {
	// Geometry: 4 caches (0:t, 1:s, 2:d, 3:e), each 16 CPUs.
	topo, cpuTopo := fakeTopologyWithCPUInfo(4, 16)
	// Cache 0 (t): CPUs 0..15
	// Cache 1 (s): CPUs 16..31
	// Cache 2 (d): CPUs 32..47
	// Cache 3 (e): CPUs 48..63

	// Claim X (16 CPUs total): 4+12 split (4 in t: 0..3, 12 in s: 16..27)
	claimX := Placement{
		ClaimUID: types.UID("claim-X"),
		CPUs:     parseCPUSet("0-3,16-27"),
	}
	// Blocker B (12 CPUs): wholly in t (4..15). Occupies rest of t!
	claimB := Placement{
		ClaimUID: types.UID("claim-B"),
		CPUs:     parseCPUSet("4-15"),
	}
	// Immobile claim in s occupying the remaining 4 CPUs (28..31):
	claimS := Placement{
		ClaimUID: types.UID("claim-S-immobile"),
		CPUs:     parseCPUSet("28-31"),
	}
	// Bystander Y (8 CPUs): wholly in d (32..39). Leaves 4 free in d (40..43).
	claimY := Placement{
		ClaimUID: types.UID("claim-Y"),
		CPUs:     parseCPUSet("32-39"),
	}
	// Immobile claim in d occupying 4 CPUs (44..47) so cache d has only 12 usable CPUs:
	claimD := Placement{
		ClaimUID: types.UID("claim-D-immobile"),
		CPUs:     parseCPUSet("44-47"),
	}
	// Immobile claim in e occupying 8 CPUs (48..55), leaving 8 free in e (56..63).
	claimE := Placement{
		ClaimUID: types.UID("claim-E-immobile"),
		CPUs:     parseCPUSet("48-55"),
	}

	placements := []Placement{claimX, claimB, claimS, claimY, claimD, claimE}
	// Free CPUs: 40-43 (in d, 4 CPUs) and 56-63 (in e, 8 CPUs)
	free := parseCPUSet("40-43,56-63")

	eligible := func(uid types.UID) bool {
		return uid != "claim-S-immobile" && uid != "claim-D-immobile" && uid != "claim-E-immobile"
	}

	sel := func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		return coreselect.TakeWholeCores(cpuTopo, available, numCPUs)
	}

	// 1. Assert that the greedy planner PlanNode fails to resolve this:
	greedyPlan, err := PlanNode(topo, placements, free, sel, Options{
		Eligible:   eligible,
		AllowSwaps: false,
	})
	if err != nil {
		t.Fatalf("PlanNode error: %v", err)
	}
	if len(greedyPlan.Moves) > 0 {
		t.Fatalf("expected greedy planner to be blocked on counterexample, but it planned %d moves: %+v",
			len(greedyPlan.Moves), greedyPlan.Moves)
	}

	// 2. Assert that ExactSearch finds the 3-move staging solution:
	goal := GoalMakeClaimWhole{ClaimUID: types.UID("claim-X")}
	plan, err := ExactSearch(topo, placements, free, cpuset.New(), goal, sel, ExactOptions{
		Eligible:   eligible,
		AllowSwaps: false,
		MaxDepth:   3,
	})
	if err != nil {
		t.Fatalf("ExactSearch error: %v", err)
	}
	if plan.Status != SearchReachable {
		t.Fatalf("expected SearchReachable, got status %v (workCount=%d, budget=%d)", plan.Status, plan.WorkCount, plan.Budget)
	}
	if len(plan.Moves) != 3 {
		t.Fatalf("expected exactly 3 moves, got %d moves: %+v", len(plan.Moves), plan.Moves)
	}

	// Verify the 3 moves:
	// Move 1: Bystander Y moves from d (2) to e (3)
	if plan.Moves[0].ClaimUID != "claim-Y" || !plan.Moves[0].To.IsSubsetOf(topo.CPUsInCache(3)) {
		t.Errorf("move 0 expected Y -> cache e (3), got: %+v", plan.Moves[0])
	}
	// Move 2: Blocker B moves from t (0) to d (2)
	if plan.Moves[1].ClaimUID != "claim-B" || !plan.Moves[1].To.IsSubsetOf(topo.CPUsInCache(2)) {
		t.Errorf("move 1 expected B -> cache d (2), got: %+v", plan.Moves[1])
	}
	// Move 3: Claim X moves to cache t (0)
	if plan.Moves[2].ClaimUID != "claim-X" || !plan.Moves[2].To.IsSubsetOf(topo.CPUsInCache(0)) {
		t.Errorf("move 2 expected X -> cache t (0), got: %+v", plan.Moves[2])
	}

	// Verify closure includes all touched CPUs
	closure := plan.ComputeClosure()
	if !closure.IsSubsetOf(topo.CPUs()) {
		t.Errorf("closure not subset of topo CPUs")
	}
	if !plan.MatchesLedger(placements) {
		t.Errorf("expected plan to match ledger initially")
	}
}

// TestUncanonicalisedOracleUnequalCaches tests that canonical search and uncanonicalised search
// produce the identical status and move count on unequal cache sizes.
func TestUncanonicalisedOracleUnequalCaches(t *testing.T) {
	// Unequal caches: cache 0 has 16 CPUs, cache 1 has 8 CPUs, cache 2 has 8 CPUs
	topo := fakeUnequalTopology([]int{16, 8, 8})

	// Claim A (8 CPUs): split 4 in cache 0 (0..3) and 4 in cache 1 (16..19)
	claimA := Placement{ClaimUID: "claim-A", CPUs: parseCPUSet("0-3,16-19")}
	// Claim B (8 CPUs): in cache 0 (4..11)
	claimB := Placement{ClaimUID: "claim-B", CPUs: parseCPUSet("4-11")}
	// Free: 4 in cache 0 (12..15), 4 in cache 1 (20..23), 8 in cache 2 (24..31)
	free := parseCPUSet("12-15,20-23,24-31")

	placements := []Placement{claimA, claimB}
	goal := GoalMakeClaimWhole{ClaimUID: "claim-A"}

	sel := func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		if available.Size() < numCPUs {
			return cpuset.New(), fmt.Errorf("insufficient cpus")
		}
		return cpuset.New(available.List()[:numCPUs]...), nil
	}

	// Run canonical search
	planCanonical, err := ExactSearch(topo, placements, free, cpuset.New(), goal, sel, ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	if err != nil {
		t.Fatalf("canonical search error: %v", err)
	}

	// Run uncanonicalised search (oracle)
	planOracle, err := ExactSearchUncanonicalised(topo, placements, free, cpuset.New(), goal, sel, ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	if err != nil {
		t.Fatalf("oracle search error: %v", err)
	}

	if planCanonical.Status != planOracle.Status {
		t.Fatalf("status mismatch: canonical=%v, oracle=%v", planCanonical.Status, planOracle.Status)
	}
	if len(planCanonical.Moves) != len(planOracle.Moves) {
		t.Fatalf("move count mismatch: canonical=%d, oracle=%d", len(planCanonical.Moves), len(planOracle.Moves))
	}
}

// TestBudgetDerivationAndExhaustion verifies the combinatorics formulas and budget limits.
func TestBudgetDerivationAndExhaustion(t *testing.T) {
	// Verify combinatorics formula for V(C, 2m)
	if v := TransferVectorsV(8, 2); v != 56 {
		t.Errorf("expected V(8, 2)=56, got %d", v)
	}
	if v := TransferVectorsV(8, 4); v != 812 {
		t.Errorf("expected V(8, 4)=812, got %d", v)
	}
	if v := TransferVectorsV(8, 6); v != 5768 {
		t.Errorf("expected V(8, 6)=5768, got %d", v)
	}
	if v := TransferVectorsV(8, 8); v != 26474 {
		t.Errorf("expected V(8, 8)=26474, got %d", v)
	}

	topo := fakeTopology(4, 16)
	claimX := Placement{ClaimUID: "claim-X", CPUs: parseCPUSet("0-3,16-27")}
	claimB := Placement{ClaimUID: "claim-B", CPUs: parseCPUSet("4-15")}
	claimY := Placement{ClaimUID: "claim-Y", CPUs: parseCPUSet("32-39")}
	free := parseCPUSet("40-47,56-63")
	placements := []Placement{claimX, claimB, claimY}

	// Derived budget should be positive
	b := DerivedBudget(topo, placements, 3, nil)
	if b <= 0 {
		t.Errorf("expected positive derived budget, got %d", b)
	}

	goal := GoalMakeClaimWhole{ClaimUID: "claim-X"}
	// With budget=1, a 3-move solution cannot be proven within budget:
	plan, err := ExactSearch(topo, placements, free, cpuset.New(), goal, nil, ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
		Budget:     1,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if plan.Status != SearchNotProven {
		t.Fatalf("expected SearchNotProven when budget is 1, got %v", plan.Status)
	}
	if plan.Status.Feasible() {
		t.Errorf("expected Feasible() to be false for SearchNotProven")
	}
}

// TestSearchUnreachable verifies that when a goal cannot be reached, the search reports SearchUnreachable.
func TestSearchUnreachable(t *testing.T) {
	topo := fakeTopology(2, 16)
	// Claim X (16 CPUs) split 8 in cache 0, 8 in cache 1.
	claimX := Placement{ClaimUID: "claim-X", CPUs: parseCPUSet("0-7,16-23")}
	// Immobile claims blocking all remaining CPUs:
	immobile0 := Placement{ClaimUID: "imm-0", CPUs: parseCPUSet("8-15")}
	immobile1 := Placement{ClaimUID: "imm-1", CPUs: parseCPUSet("24-31")}
	placements := []Placement{claimX, immobile0, immobile1}
	free := cpuset.New() // 0 free CPUs

	eligible := func(uid types.UID) bool { return uid == "claim-X" }
	goal := GoalMakeClaimWhole{ClaimUID: "claim-X"}

	plan, err := ExactSearch(topo, placements, free, cpuset.New(), goal, nil, ExactOptions{
		Eligible: eligible,
		MaxDepth: 3,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if plan.Status != SearchUnreachable {
		t.Fatalf("expected SearchUnreachable, got %v", plan.Status)
	}
}

// TestGoalFreeCacheAndKCacheHome tests the other goal types.
func TestGoalFreeCacheAndKCacheHome(t *testing.T) {
	topo := fakeTopology(3, 16)
	// Cache 0 (0..15), Cache 1 (16..31), Cache 2 (32..47)
	// Claim A (8 CPUs) in cache 0 (0..7)
	claimA := Placement{ClaimUID: "claim-A", CPUs: parseCPUSet("0-7")}
	// Claim B (8 CPUs) in cache 1 (16..23)
	claimB := Placement{ClaimUID: "claim-B", CPUs: parseCPUSet("16-23")}
	// Free: 8 in cache 0 (8..15), 8 in cache 1 (24..31), 16 in cache 2 (32..47)
	free := parseCPUSet("8-15,24-31,32-47")
	placements := []Placement{claimA, claimB}

	// Goal: Free cache 0
	goalFree := GoalFreeCache{CacheID: 0}
	plan, err := ExactSearch(topo, placements, free, cpuset.New(), goalFree, nil, ExactOptions{
		MaxDepth: 3,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if plan.Status != SearchReachable {
		t.Fatalf("expected SearchReachable, got %v", plan.Status)
	}
	if len(plan.Moves) != 1 || plan.Moves[0].ClaimUID != "claim-A" {
		t.Fatalf("unexpected moves for GoalFreeCache: %+v", plan.Moves)
	}

	// Goal: Produce a 2-cache home (2 caches completely free).
	// Cache 2 is currently the only completely free cache.
	// Moving Claim B from cache 1 to cache 0 (where 8 CPUs are free) will free cache 1!
	// Then both Cache 1 and Cache 2 will be completely free (2 caches!).
	goalK := GoalKCacheHome{K: 2}
	planK, err := ExactSearch(topo, placements, free, cpuset.New(), goalK, nil, ExactOptions{
		MaxDepth: 3,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if planK.Status != SearchReachable {
		t.Fatalf("expected SearchReachable, got %v", planK.Status)
	}
	if len(planK.Moves) != 1 || (planK.Moves[0].ClaimUID != "claim-B" && planK.Moves[0].ClaimUID != "claim-A") {
		t.Fatalf("expected 1 move of claim A or claim B to free a cache, got %+v", planK.Moves)
	}
}

// TestInFlightAllocationsTreatedAsConsumed verifies inFlight CPUs are avoided.
func TestInFlightAllocationsTreatedAsConsumed(t *testing.T) {
	topo := fakeTopology(2, 16)
	claimA := Placement{ClaimUID: "claim-A", CPUs: parseCPUSet("0-7")}
	placements := []Placement{claimA}
	// Cache 1 (16..31) has 16 free CPUs physically, but 16..23 are in-flight!
	free := parseCPUSet("8-15,16-31")
	inFlight := parseCPUSet("16-23") // 8 CPUs in cache 1 are in flight

	goalFree := GoalFreeCache{CacheID: 0}
	plan, err := ExactSearch(topo, placements, free, inFlight, goalFree, nil, ExactOptions{
		MaxDepth: 3,
	})
	if err != nil {
		t.Fatalf("search error: %v", err)
	}
	if plan.Status != SearchReachable {
		t.Fatalf("expected SearchReachable, got %v", plan.Status)
	}
	// Move should land on 24..31 in cache 1 (avoiding 16..23)
	if !plan.Moves[0].To.Intersection(inFlight).IsEmpty() {
		t.Fatalf("move landed on in-flight CPUs: %+v", plan.Moves[0])
	}
}
