/*
Copyright 2025 The Kubernetes Authors.

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

package store

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	v1alpha1 "github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// Role is what a request was given: CPUs its claim holds alone, or CPUs of a
// pool other claims may hold too.
type Role string

const (
	// RoleExclusive marks CPUs no other claim may be given.
	RoleExclusive Role = "exclusive"
	// RoleShared marks CPUs of a claimed pool, which every claim that asks for
	// that pool holds at the same time.
	RoleShared Role = "shared"
)

// RequestAllocation is the CPUs one request of a claim was given.
type RequestAllocation struct {
	Request string
	CPUs    cpuset.CPUSet
	Role    Role
}

// UnionOf returns the CPUs a set of requests grants together.
func UnionOf(requests []RequestAllocation) cpuset.CPUSet {
	union := cpuset.New()
	for _, request := range requests {
		union = union.Union(request.CPUs)
	}
	return union
}

// RoundProvenance records a defragmentation round in flight when the claim's
// placement was written.
type RoundProvenance struct {
	RoundID  string
	Origin   cpuset.CPUSet
	Target   cpuset.CPUSet
	Partners []types.UID
}

// ClaimRecord is everything the driver records for one prepared claim: what each
// of its requests holds, and whether the claim permits those CPUs to change
// while its containers run.
//
// Mobility is recorded beside the placement because it outlives the claim object
// as far as this driver is concerned: the driver does not watch ResourceClaims,
// so after a restart the CDI specs on disk are all it has, and a claim whose
// mobility it could not recover would stop being movable for the life of its
// pod.
type ClaimRecord struct {
	Requests    []RequestAllocation
	Relocatable bool
	Alignment   v1alpha1.Alignment
	// Recorded is how many CPUs the claim's allocation charged to each device it
	// names, which is what a scheduler subtracts from that device's capacity. It
	// is empty for a claim that was given no CPUs of its own, and for one whose
	// record on disk was written before the driver kept this.
	Recorded map[string]int
	// Round is the defragmentation round in flight when this record was written
	// to disk, or nil when no round is active for the claim.
	Round *RoundProvenance
}

// CPUAllocation is the single source of truth for CPU allocations.
type CPUAllocation struct {
	mu            sync.RWMutex
	availableCPUs cpuset.CPUSet
	reservedCPUs  cpuset.CPUSet
	// CCX-FORK: upstream holds one cpuset per claim, keyed by claim UID alone.
	claims       map[types.UID]*claimAllocation
	preparedCPUs cpuset.CPUSet
	// lastSwapGroup numbers the exchanges this store has begun, so that the
	// participants of one can be told from the participants of another.
	lastSwapGroup int
	// closureReservations holds, per NUMA node, the reserved CPU closure of an
	// in-flight exact repair plan.
	closureReservations map[int]cpuset.CPUSet
}

type claimAllocation struct {
	byRequest map[string]RequestAllocation
	// rebindOrigin is the exclusive CPUs of each request before the move in
	// flight, and is nil when none is.
	rebindOrigin map[string]cpuset.CPUSet
	// swapGroup identifies the exchange this claim is part of, and is zero for a
	// claim moving into free CPUs on its own. The group is recorded rather than
	// inferred from which CPUs the movers hold, because it has to survive one of
	// its members being unprepared while the batch is out.
	swapGroup   int
	relocatable bool
	alignment   v1alpha1.Alignment
	// recorded is what the claim's allocation charged each device it names.
	recorded map[string]int
}

func newClaimAllocation(record ClaimRecord) *claimAllocation {
	byRequest := make(map[string]RequestAllocation, len(record.Requests))
	for _, request := range record.Requests {
		byRequest[request.Request] = request
	}
	return &claimAllocation{
		byRequest:   byRequest,
		relocatable: record.Relocatable,
		alignment:   record.Alignment,
		recorded:    maps.Clone(record.Recorded),
	}
}

// requests returns the claim's allocations ordered by request name, so callers
// and records do not depend on map iteration order.
func (c *claimAllocation) requests() []RequestAllocation {
	requests := make([]RequestAllocation, 0, len(c.byRequest))
	for _, request := range c.byRequest {
		requests = append(requests, request)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].Request < requests[j].Request })
	return requests
}

func (c *claimAllocation) cpus() cpuset.CPUSet {
	cpus := cpuset.New()
	for _, request := range c.byRequest {
		cpus = cpus.Union(request.CPUs)
	}
	return cpus
}

func (c *claimAllocation) exclusiveCPUs() cpuset.CPUSet {
	cpus := cpuset.New()
	for _, request := range c.byRequest {
		if request.Role == RoleExclusive {
			cpus = cpus.Union(request.CPUs)
		}
	}
	return cpus
}

func (c *claimAllocation) originCPUs() cpuset.CPUSet {
	cpus := cpuset.New()
	for _, origin := range c.rebindOrigin {
		cpus = cpus.Union(origin)
	}
	return cpus
}

// exclusiveOverlap is the CPUs more than one exclusive request of the claim was
// given. A claim that holds a CPU twice cannot be moved as a whole, since its
// requests then have more CPUs between them than the claim occupies.
func (c *claimAllocation) exclusiveOverlap() cpuset.CPUSet {
	seen, overlap := cpuset.New(), cpuset.New()
	for _, name := range c.exclusiveRequestNames() {
		cpus := c.byRequest[name].CPUs
		overlap = overlap.Union(seen.Intersection(cpus))
		seen = seen.Union(cpus)
	}
	return overlap
}

func (c *claimAllocation) exclusiveRequestNames() []string {
	var names []string
	for name, request := range c.byRequest {
		if request.Role == RoleExclusive {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// placeExclusive spreads target over the claim's exclusive requests, each
// keeping the number of CPUs it already had. A move is planned for a claim as a
// whole and within one NUMA node, so which of its own requests ends up on which
// of the target's CPUs is not a distinction anything outside the claim can make.
func (c *claimAllocation) placeExclusive(target cpuset.CPUSet) {
	cpuIDs := target.List()
	for _, name := range c.exclusiveRequestNames() {
		request := c.byRequest[name]
		size := request.CPUs.Size()
		request.CPUs = cpuset.New(cpuIDs[:size]...)
		cpuIDs = cpuIDs[size:]
		c.byRequest[name] = request
	}
}

func (c *claimAllocation) restoreExclusive(origin map[string]cpuset.CPUSet) {
	for name, cpus := range origin {
		request := c.byRequest[name]
		request.CPUs = cpus
		c.byRequest[name] = request
	}
}

func (c *claimAllocation) exclusiveByRequest() map[string]cpuset.CPUSet {
	byRequest := make(map[string]cpuset.CPUSet)
	for name, request := range c.byRequest {
		if request.Role == RoleExclusive {
			byRequest[name] = request.CPUs
		}
	}
	return byRequest
}

func (c *claimAllocation) equals(requests []RequestAllocation) bool {
	if len(c.byRequest) != len(requests) {
		return false
	}
	for _, request := range requests {
		existing, ok := c.byRequest[request.Request]
		if !ok || existing.Role != request.Role || !existing.CPUs.Equals(request.CPUs) {
			return false
		}
	}
	return true
}

// AllocationSnapshot is a point-in-time summary of CPU allocation state.
type AllocationSnapshot struct {
	AllocatedCPUs        int
	AvailableCPUs        int
	ReservedCPUs         int
	ActiveResourceClaims int
}

// NewCPUAllocation creates a new CPUAllocation.
func NewCPUAllocation(cpuTopology *cpuinfo.CPUTopology, reservedCPUs cpuset.CPUSet) *CPUAllocation {
	cpuIDs := []int{}
	for cpuID := range cpuTopology.CPUDetails {
		cpuIDs = append(cpuIDs, cpuID)
	}
	allCPUsSet := cpuset.New(cpuIDs...)
	availableCPUs := allCPUsSet.Difference(reservedCPUs)

	return &CPUAllocation{
		availableCPUs:       availableCPUs,
		reservedCPUs:        reservedCPUs,
		claims:              make(map[types.UID]*claimAllocation),
		preparedCPUs:        cpuset.New(),
		closureReservations: make(map[int]cpuset.CPUSet),
	}
}

// ReserveResourceClaimAllocation records what each request of a prepared claim
// was given, and whether the claim allows its CPUs to change. Its exclusive CPUs
// remain unavailable to shared containers and to other claims until Unprepare;
// the CPUs of a request with any other role are recorded for the container's
// cpuset alone. When shared containers are present, the reservation must leave at
// least one CPU in the shared pool because NRI cannot represent an empty CPUSet.
//
// CCX-FORK: upstream records one cpuset per claim, since to it every request
// grants CPUs the claim holds alone and no claim ever moves.
func (s *CPUAllocation) ReserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, record ClaimRecord, hasSharedContainers bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocation, ok := s.claims[claimUID]; ok {
		if allocation.equals(record.Requests) {
			return nil
		}
		return fmt.Errorf("claim %q is already prepared with CPUs %q (requested %q)", claimUID, allocation.cpus().String(), UnionOf(record.Requests).String())
	}
	allocation := newClaimAllocation(record)
	if overlap := allocation.exclusiveOverlap(); !overlap.IsEmpty() {
		return fmt.Errorf("claim %q was given CPUs %q for more than one of its exclusive requests", claimUID, overlap.String())
	}
	exclusive := allocation.exclusiveCPUs()
	sharedCPUs := s.availableCPUs.Difference(s.preparedCPUs)
	if !exclusive.IsSubsetOf(sharedCPUs) {
		return fmt.Errorf("claim %q has overlapping CPU assignment %q", claimUID, exclusive.String())
	}
	if hasSharedContainers && !exclusive.IsEmpty() && sharedCPUs.Difference(exclusive).IsEmpty() {
		return fmt.Errorf("claim %q would exhaust the shared CPU pool while shared containers are running", claimUID)
	}
	s.claims[claimUID] = allocation
	s.preparedCPUs = s.preparedCPUs.Union(exclusive)
	logger.Info("reserved allocation for resource claim", "cpus", allocation.cpus().String())
	return nil
}

// GetResourceClaimAllocationUnion returns the union of the prepared cpusets of
// the given claims, failing if any of them is not prepared by this driver.
//
// CCX-FORK: replaces upstream's ValidateResourceClaimAllocations, which compared
// the store against caller-supplied cpusets instead of supplying them.
func (s *CPUAllocation) GetResourceClaimAllocationUnion(claimUIDs ...types.UID) (cpuset.CPUSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	union := cpuset.New()
	for _, claimUID := range claimUIDs {
		allocation, ok := s.claims[claimUID]
		if !ok {
			return cpuset.New(), fmt.Errorf("claim %q is not prepared by this driver", claimUID)
		}
		union = union.Union(allocation.cpus())
	}
	return union, nil
}

// GetResourceClaimOriginUnion returns the union of the given claims' CPUs as
// they were before the moves in flight, which for a claim that is not moving is
// simply what it holds. It is what a container has to be pinned back to when a
// move it took part in has to be undone, and it stays available for as long as
// the move is in flight, since a mover never releases what it came from.
//
// CCX-FORK: upstream has no move, so a claim's CPUs have only one value.
func (s *CPUAllocation) GetResourceClaimOriginUnion(claimUIDs ...types.UID) (cpuset.CPUSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	union := cpuset.New()
	for _, claimUID := range claimUIDs {
		allocation, ok := s.claims[claimUID]
		if !ok {
			return cpuset.New(), fmt.Errorf("claim %q is not prepared by this driver", claimUID)
		}
		if allocation.rebindOrigin == nil {
			union = union.Union(allocation.cpus())
			continue
		}
		union = union.Union(allocation.cpus().Difference(allocation.exclusiveCPUs())).Union(allocation.originCPUs())
	}
	return union, nil
}

// BeginRebind starts moving a prepared claim's exclusive CPUs onto target,
// holding both its current and its target CPUs until the move is committed or
// aborted. Neither half is offered to a shared container or to another claim in
// the meantime, so an abort always has valid CPUs to fall back to: the ones the
// claim already held were never released.
//
// Only the claim's CPU count is checked. What else makes a target valid, namely
// preserving the claim's per-NUMA-node footprint and taking whole physical cores
// where those are required, is for the caller that chose it from the topology.
func (s *CPUAllocation) BeginRebind(logger logr.Logger, claimUID types.UID, target cpuset.CPUSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	allocation, ok := s.claims[claimUID]
	if !ok {
		return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
	}
	current := allocation.exclusiveCPUs()
	if allocation.rebindOrigin != nil {
		return fmt.Errorf("claim %q is already rebinding from %q to %q", claimUID, allocation.originCPUs().String(), current.String())
	}
	if target.Size() != current.Size() {
		return fmt.Errorf("rebind of claim %q would change its CPU count from %d to %d", claimUID, current.Size(), target.Size())
	}
	free := s.availableCPUs.Difference(s.preparedCPUs).Union(current)
	if !target.IsSubsetOf(free) {
		return fmt.Errorf("rebind target %q for claim %q is not free (claim may move within %q)", target.String(), claimUID, free.String())
	}

	allocation.rebindOrigin = allocation.exclusiveByRequest()
	allocation.placeExclusive(target)
	s.preparedCPUs = s.preparedCPUs.Union(target)
	logger.Info("began rebind of resource claim", "from", current.String(), "to", target.String())
	return nil
}

// BeginSwap starts an exchange between prepared claims: each is placed on the
// CPUs targets names for it, and every one of them holds both its current and
// its target CPUs until the exchange is committed or aborted. That transit set
// is what keeps a Prepare arriving mid-batch from handing a newcomer CPUs a
// half-swapped claim is still running on, and it is what an abort falls back
// to, since no participant ever released anything.
//
// The targets divide up exactly the CPUs the claims already hold between them,
// each keeping its own count, so the set the group occupies is the same before,
// during and after. Nothing here touches the CPUs available to anything else,
// whichever way the exchange ends. An exchange that also took free CPUs would
// be a move and an exchange at once; the caller plans that as two steps.
func (s *CPUAllocation) BeginSwap(logger logr.Logger, targets map[types.UID]cpuset.CPUSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(targets) < 2 {
		return fmt.Errorf("an exchange needs at least two claims, got %d", len(targets))
	}
	claimUIDs := sortedUIDs(targets)
	held, wanted := cpuset.New(), cpuset.New()
	for _, claimUID := range claimUIDs {
		allocation, ok := s.claims[claimUID]
		if !ok {
			return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
		}
		if allocation.rebindOrigin != nil {
			return fmt.Errorf("claim %q is already rebinding from %q to %q", claimUID, allocation.originCPUs().String(), allocation.exclusiveCPUs().String())
		}
		current := allocation.exclusiveCPUs()
		target := targets[claimUID]
		if target.Size() != current.Size() {
			return fmt.Errorf("exchange would change claim %q from %d CPUs to %d", claimUID, current.Size(), target.Size())
		}
		if overlap := wanted.Intersection(target); !overlap.IsEmpty() {
			return fmt.Errorf("exchange gives CPUs %q to more than one claim", overlap.String())
		}
		held, wanted = held.Union(current), wanted.Union(target)
	}
	if !held.Equals(wanted) {
		return fmt.Errorf("exchange of claims %v would move them from %q onto %q, which is not the same set of CPUs", claimUIDs, held.String(), wanted.String())
	}

	s.lastSwapGroup++
	for _, claimUID := range claimUIDs {
		allocation := s.claims[claimUID]
		allocation.rebindOrigin = allocation.exclusiveByRequest()
		allocation.swapGroup = s.lastSwapGroup
		allocation.placeExclusive(targets[claimUID])
	}
	logger.Info("began exchange of resource claims", "claims", claimUIDs, "cpus", held.String())
	return nil
}

// CommitSwap ends an exchange with every participant still prepared on its
// target.
func (s *CPUAllocation) CommitSwap(logger logr.Logger, claimUIDs ...types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	present, err := s.swapInFlight(claimUIDs)
	if err != nil {
		return err
	}
	for _, claimUID := range present {
		allocation := s.claims[claimUID]
		allocation.rebindOrigin, allocation.swapGroup = nil, 0
	}
	s.preparedCPUs = s.heldByClaimsLocked()
	logger.Info("committed exchange of resource claims", "claims", present)
	return nil
}

// AbortSwap ends an exchange with every participant still prepared back where it
// started.
func (s *CPUAllocation) AbortSwap(logger logr.Logger, claimUIDs ...types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	present, err := s.swapInFlight(claimUIDs)
	if err != nil {
		return err
	}
	for _, claimUID := range present {
		allocation := s.claims[claimUID]
		allocation.restoreExclusive(allocation.rebindOrigin)
		allocation.rebindOrigin, allocation.swapGroup = nil, 0
	}
	s.preparedCPUs = s.heldByClaimsLocked()
	logger.Info("aborted exchange of resource claims", "claims", present)
	return nil
}

// swapInFlight returns the participants of one exchange that are still prepared.
//
// A participant unprepared while the batch was out is skipped rather than
// refused. It released nothing its partner is taking -- removing it recomputed
// what the survivors hold -- and refusing the whole group over it would leave the
// rest holding two cpusets with nothing able to settle them, which on a fenced
// NUMA node means fenced for as long as the driver runs.
//
// Everything else is refused, and the group id is what makes that exact: a claim
// moving on its own, a claim not moving at all, two exchanges named together, and
// an exchange named without one of its still-prepared members are each a caller
// settling something it did not begin.
func (s *CPUAllocation) swapInFlight(claimUIDs []types.UID) ([]types.UID, error) {
	if len(claimUIDs) < 2 {
		return nil, fmt.Errorf("an exchange needs at least two claims, got %d", len(claimUIDs))
	}
	group := 0
	present := make([]types.UID, 0, len(claimUIDs))
	for _, claimUID := range claimUIDs {
		allocation, ok := s.claims[claimUID]
		if !ok {
			continue
		}
		if allocation.swapGroup == 0 {
			return nil, fmt.Errorf("claim %q has no exchange in flight", claimUID)
		}
		if group != 0 && allocation.swapGroup != group {
			return nil, fmt.Errorf("claims %v are not one exchange", claimUIDs)
		}
		group = allocation.swapGroup
		present = append(present, claimUID)
	}
	if len(present) == 0 {
		return nil, fmt.Errorf("no claim of the exchange %v is prepared any more", claimUIDs)
	}
	for claimUID, allocation := range s.claims {
		if allocation.swapGroup == group && !slices.Contains(present, claimUID) {
			return nil, fmt.Errorf("claim %q belongs to the same exchange and was not named", claimUID)
		}
	}
	return present, nil
}

func sortedUIDs(targets map[types.UID]cpuset.CPUSet) []types.UID {
	claimUIDs := make([]types.UID, 0, len(targets))
	for claimUID := range targets {
		claimUIDs = append(claimUIDs, claimUID)
	}
	sort.Slice(claimUIDs, func(i, j int) bool { return claimUIDs[i] < claimUIDs[j] })
	return claimUIDs
}

// CommitRebind releases the CPUs a claim moved away from, keeping the target.
func (s *CPUAllocation) CommitRebind(logger logr.Logger, claimUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	allocation, ok := s.claims[claimUID]
	if !ok || allocation.rebindOrigin == nil {
		return fmt.Errorf("claim %q has no rebind in flight", claimUID)
	}
	origin := allocation.originCPUs()
	target := allocation.exclusiveCPUs()
	allocation.rebindOrigin = nil
	s.preparedCPUs = s.preparedCPUs.Difference(origin.Difference(target))
	logger.Info("committed rebind of resource claim", "cpus", target.String())
	return nil
}

// AbortRebind returns a claim to the CPUs it was moving away from and releases
// the target.
func (s *CPUAllocation) AbortRebind(logger logr.Logger, claimUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	allocation, ok := s.claims[claimUID]
	if !ok || allocation.rebindOrigin == nil {
		return fmt.Errorf("claim %q has no rebind in flight", claimUID)
	}
	origin := allocation.originCPUs()
	target := allocation.exclusiveCPUs()
	allocation.restoreExclusive(allocation.rebindOrigin)
	allocation.rebindOrigin = nil
	s.preparedCPUs = s.preparedCPUs.Difference(target.Difference(origin))
	logger.Info("aborted rebind of resource claim", "cpus", origin.String())
	return nil
}

// GetRebindOrigin returns the CPUs a claim is moving away from, and whether a
// rebind is in flight for it at all.
func (s *CPUAllocation) GetRebindOrigin(claimUID types.UID) (cpuset.CPUSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	if !ok || allocation.rebindOrigin == nil {
		return cpuset.CPUSet{}, false
	}
	return allocation.originCPUs(), true
}

// RemoveResourceClaimAllocation removes a resource claim allocation from the store.
func (s *CPUAllocation) RemoveResourceClaimAllocation(logger logr.Logger, claimUID types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.claims[claimUID]; ok {
		s.removeLocked(claimUID)
		logger.Info("removed allocation for resource claim")
	}
}

// CCX-FORK: upstream releases only the claim's own cpuset; here a claim may hold
// two, the CPUs it runs on and the ones it is moving onto.
func (s *CPUAllocation) removeLocked(claimUID types.UID) {
	if _, ok := s.claims[claimUID]; !ok {
		return
	}
	delete(s.claims, claimUID)
	s.preparedCPUs = s.heldByClaimsLocked()
}

// heldByClaimsLocked is every CPU the prepared claims hold between them, both
// halves of a move in flight included.
//
// Recomputed rather than adjusted claim by claim, because a CPU one claim is
// leaving is a CPU another may be taking: the two sides of an exchange hold each
// other's, so subtracting a departing claim's own would release a CPU its partner
// is still running on.
func (s *CPUAllocation) heldByClaimsLocked() cpuset.CPUSet {
	held := cpuset.New()
	for _, allocation := range s.claims {
		held = held.Union(allocation.exclusiveCPUs()).Union(allocation.originCPUs())
	}
	return held
}

// GetSharedCPUs returns CPUs available to shared containers.
func (s *CPUAllocation) GetSharedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.availableCPUs.Difference(s.preparedCPUs)
}

// GetResourceClaimAllocation returns every CPU a claim was given, whatever the
// role of the request that granted it.
//
// CCX-FORK: while a rebind is in flight this is the claim's target, not the CPUs
// its container is still running on. A caller that needs both takes the other
// half from GetRebindOrigin.
func (s *CPUAllocation) GetResourceClaimAllocation(claimUID types.UID) (cpuset.CPUSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	if !ok {
		return cpuset.CPUSet{}, false
	}
	return allocation.cpus(), true
}

// GetClaimRecord returns everything recorded for a claim: what each of its
// requests was given, ordered by request name, and whether its CPUs may change.
func (s *CPUAllocation) GetClaimRecord(claimUID types.UID) (ClaimRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	if !ok {
		return ClaimRecord{}, false
	}
	return ClaimRecord{
		Requests:    allocation.requests(),
		Relocatable: allocation.relocatable,
		Alignment:   allocation.alignment,
		Recorded:    maps.Clone(allocation.recorded),
	}, true
}

// Alignment returns a claim's alignment policy. Defaults to AlignmentBestEffort.
func (s *CPUAllocation) Alignment(claimUID types.UID) v1alpha1.Alignment {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	if !ok || allocation.alignment == "" {
		return v1alpha1.AlignmentBestEffort
	}
	return allocation.alignment
}

// IsRepairable reports whether a claim asked to be made whole by the driver when landing split.
func (s *CPUAllocation) IsRepairable(claimUID types.UID) bool {
	return s.Alignment(claimUID) == v1alpha1.AlignmentRepairable
}

// ReserveClosure holds an exact repair plan's closure for a NUMA node.
func (s *CPUAllocation) ReserveClosure(numaNodeID int, closure cpuset.CPUSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closureReservations == nil {
		s.closureReservations = make(map[int]cpuset.CPUSet)
	}
	s.closureReservations[numaNodeID] = closure
}

// ReleaseClosure clears the reserved closure for a NUMA node.
func (s *CPUAllocation) ReleaseClosure(numaNodeID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closureReservations != nil {
		delete(s.closureReservations, numaNodeID)
	}
}

// ReservedClosures returns the union of all currently reserved plan closures across NUMA nodes.
func (s *CPUAllocation) ReservedClosures() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	all := cpuset.New()
	for _, c := range s.closureReservations {
		all = all.Union(c)
	}
	return all
}

// ReservedClosure returns the reserved closure for a specific NUMA node.
func (s *CPUAllocation) ReservedClosure(numaNodeID int) cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closureReservations == nil {
		return cpuset.New()
	}
	return s.closureReservations[numaNodeID]
}

// SetRecordedDevices records what a claim's allocation charged each device,
// which a caller supplies when the claim's own record could not carry it: a
// spec written before the driver kept this names no device, and the claim
// object a replayed Prepare hands over is where the answer comes back from.
//
// It refuses to overwrite an answer the store already has, since the
// allocation is immutable and two answers about it cannot both be right.
func (s *CPUAllocation) SetRecordedDevices(logger logr.Logger, claimUID types.UID, recorded map[string]int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	allocation, ok := s.claims[claimUID]
	if !ok {
		return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
	}
	if len(allocation.recorded) > 0 {
		return fmt.Errorf("claim %q already records the devices its allocation charged", claimUID)
	}
	allocation.recorded = maps.Clone(recorded)
	logger.V(2).Info("recovered the devices a claim's allocation charged", "recorded", recorded)
	return nil
}

// IsRelocatable reports whether a claim permits the driver to change its CPUs
// while its containers run.
func (s *CPUAllocation) IsRelocatable(claimUID types.UID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	return ok && allocation.relocatable
}

// HoldsExclusiveCPUs reports whether a claim was given CPUs it holds alone.
// A claim that was not is bound to no single container: nothing it grants is
// taken away from anything else, so several containers and pods may reference it.
func (s *CPUAllocation) HoldsExclusiveCPUs(claimUID types.UID) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	return ok && !allocation.exclusiveCPUs().IsEmpty()
}

// ClaimHolding is one prepared claim as the capacity published for a device is
// computed from.
//
// Held is the claim's exclusive CPUs together with the ones it is moving away
// from, because a claim with a move in flight occupies both: its container is on
// one of the two and the driver does not know which until the runtime answers.
// A device the claim is moving onto is therefore shrunk from the moment the move
// is reserved, and the one it is leaving grows only once the move is committed.
type ClaimHolding struct {
	Held     cpuset.CPUSet
	Recorded map[string]int
}

// ClaimHoldings returns every prepared claim that holds CPUs of its own, in one
// snapshot.
//
// One snapshot rather than a reader per quantity: a caller that took the CPUs
// and the charged amounts in two calls could catch a move between them and
// publish a device as both emptied and never filled.
func (s *CPUAllocation) ClaimHoldings() map[types.UID]ClaimHolding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	holdings := make(map[types.UID]ClaimHolding, len(s.claims))
	for claimUID, allocation := range s.claims {
		held := allocation.exclusiveCPUs().Union(allocation.originCPUs())
		if held.IsEmpty() {
			continue
		}
		holdings[claimUID] = ClaimHolding{Held: held, Recorded: maps.Clone(allocation.recorded)}
	}
	return holdings
}

// ExclusiveClaimAllocations returns every prepared claim that holds exclusive
// CPUs, and which CPUs those are. A claim with a rebind in flight reads as being
// on its target, as it does through GetResourceClaimAllocation.
func (s *CPUAllocation) ExclusiveClaimAllocations() map[types.UID]cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocations := make(map[types.UID]cpuset.CPUSet, len(s.claims))
	for claimUID, allocation := range s.claims {
		if cpus := allocation.exclusiveCPUs(); !cpus.IsEmpty() {
			allocations[claimUID] = cpus
		}
	}
	return allocations
}

// GetReservedCPUs returns the set of reserved CPUs.
func (s *CPUAllocation) GetReservedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedCPUs
}

// GetPreparedCPUs returns the CPUs reserved for prepared claims.
func (s *CPUAllocation) GetPreparedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.preparedCPUs
}

// Snapshot returns a point-in-time summary of CPU allocation state.
func (s *CPUAllocation) Snapshot() AllocationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return AllocationSnapshot{
		AllocatedCPUs:        s.preparedCPUs.Size(),
		AvailableCPUs:        s.availableCPUs.Difference(s.preparedCPUs).Size(),
		ReservedCPUs:         s.reservedCPUs.Size(),
		ActiveResourceClaims: len(s.claims),
	}
}
