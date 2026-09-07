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
	"sort"
	"sync"

	"github.com/go-logr/logr"
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

// CPUAllocation is the single source of truth for CPU allocations.
type CPUAllocation struct {
	mu            sync.RWMutex
	availableCPUs cpuset.CPUSet
	reservedCPUs  cpuset.CPUSet
	// CCX-FORK: upstream holds one cpuset per claim, keyed by claim UID alone.
	claims       map[types.UID]*claimAllocation
	preparedCPUs cpuset.CPUSet
}

type claimAllocation struct {
	byRequest map[string]RequestAllocation
	// rebindOrigin is the exclusive CPUs of each request before the move in
	// flight, and is nil when none is.
	rebindOrigin map[string]cpuset.CPUSet
}

func newClaimAllocation(requests []RequestAllocation) *claimAllocation {
	byRequest := make(map[string]RequestAllocation, len(requests))
	for _, request := range requests {
		byRequest[request.Request] = request
	}
	return &claimAllocation{byRequest: byRequest}
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
		availableCPUs: availableCPUs,
		reservedCPUs:  reservedCPUs,
		claims:        make(map[types.UID]*claimAllocation),
		preparedCPUs:  cpuset.New(),
	}
}

// ReserveResourceClaimAllocation records what each request of a prepared claim
// was given. Its exclusive CPUs remain unavailable to shared containers and to
// other claims until Unprepare; the CPUs of a request with any other role are
// recorded for the container's cpuset alone. When shared containers are present,
// the reservation must leave at least one CPU in the shared pool because NRI
// cannot represent an empty CPUSet.
//
// CCX-FORK: upstream records one cpuset per claim, since to it every request
// grants CPUs the claim holds alone.
func (s *CPUAllocation) ReserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, requests []RequestAllocation, hasSharedContainers bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocation, ok := s.claims[claimUID]; ok {
		if allocation.equals(requests) {
			return nil
		}
		return fmt.Errorf("claim %q is already prepared with CPUs %q (requested %q)", claimUID, allocation.cpus().String(), UnionOf(requests).String())
	}
	allocation := newClaimAllocation(requests)
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

// CCX-FORK: upstream releases only the claim's own cpuset; a claim removed
// mid-rebind also holds the CPUs it was moving away from.
func (s *CPUAllocation) removeLocked(claimUID types.UID) {
	allocation, ok := s.claims[claimUID]
	if !ok {
		return
	}
	delete(s.claims, claimUID)
	s.preparedCPUs = s.preparedCPUs.Difference(allocation.exclusiveCPUs().Union(allocation.originCPUs()))
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

// GetResourceClaimRequests returns what each request of a claim was given,
// ordered by request name.
func (s *CPUAllocation) GetResourceClaimRequests(claimUID types.UID) ([]RequestAllocation, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.claims[claimUID]
	if !ok {
		return nil, false
	}
	return allocation.requests(), true
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
