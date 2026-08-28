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
	"sync"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// CPUAllocation is the single source of truth for CPU allocations.
type CPUAllocation struct {
	mu                       sync.RWMutex
	availableCPUs            cpuset.CPUSet
	reservedCPUs             cpuset.CPUSet
	resourceClaimAllocations map[types.UID]cpuset.CPUSet
	rebindOrigins            map[types.UID]cpuset.CPUSet
	preparedCPUs             cpuset.CPUSet
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
		availableCPUs:            availableCPUs,
		reservedCPUs:             reservedCPUs,
		resourceClaimAllocations: make(map[types.UID]cpuset.CPUSet),
		rebindOrigins:            make(map[types.UID]cpuset.CPUSet),
		preparedCPUs:             cpuset.New(),
	}
}

// ReserveResourceClaimAllocation records a prepared claim. Its CPUs remain unavailable
// to shared containers and other exclusive claims until Unprepare. When shared
// containers are present, the reservation must leave at least one CPU in the
// shared pool because NRI cannot represent an empty CPUSet.
func (s *CPUAllocation) ReserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, cpus cpuset.CPUSet, hasSharedContainers bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocation, ok := s.resourceClaimAllocations[claimUID]; ok {
		if allocation.Equals(cpus) {
			return nil
		}
		return fmt.Errorf("claim %q is already prepared with CPUs %q (requested %q)", claimUID, allocation.String(), cpus.String())
	}
	sharedCPUs := s.availableCPUs.Difference(s.preparedCPUs)
	if !cpus.IsSubsetOf(sharedCPUs) {
		return fmt.Errorf("claim %q has overlapping CPU assignment %q", claimUID, cpus.String())
	}
	if hasSharedContainers && !cpus.IsEmpty() && sharedCPUs.Difference(cpus).IsEmpty() {
		return fmt.Errorf("claim %q would exhaust the shared CPU pool while shared containers are running", claimUID)
	}
	s.resourceClaimAllocations[claimUID] = cpus
	s.preparedCPUs = s.preparedCPUs.Union(cpus)
	logger.Info("reserved allocation for resource claim", "cpus", cpus.String())
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
		allocation, ok := s.resourceClaimAllocations[claimUID]
		if !ok {
			return cpuset.New(), fmt.Errorf("claim %q is not prepared by this driver", claimUID)
		}
		union = union.Union(allocation)
	}
	return union, nil
}

// BeginRebind starts moving a prepared claim onto target, holding both its
// current and its target CPUs until the move is committed or aborted. Neither
// half is offered to a shared container or to another claim in the meantime, so
// an abort always has valid CPUs to fall back to: the ones the claim already
// held were never released.
//
// Only the claim's CPU count is checked. What else makes a target valid, namely
// preserving the claim's per-NUMA-node footprint and taking whole physical cores
// where those are required, is for the caller that chose it from the topology.
func (s *CPUAllocation) BeginRebind(logger logr.Logger, claimUID types.UID, target cpuset.CPUSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, ok := s.resourceClaimAllocations[claimUID]
	if !ok {
		return fmt.Errorf("claim %q is not prepared by this driver", claimUID)
	}
	if origin, ok := s.rebindOrigins[claimUID]; ok {
		return fmt.Errorf("claim %q is already rebinding from %q to %q", claimUID, origin.String(), current.String())
	}
	if target.Size() != current.Size() {
		return fmt.Errorf("rebind of claim %q would change its CPU count from %d to %d", claimUID, current.Size(), target.Size())
	}
	free := s.availableCPUs.Difference(s.preparedCPUs).Union(current)
	if !target.IsSubsetOf(free) {
		return fmt.Errorf("rebind target %q for claim %q is not free (claim may move within %q)", target.String(), claimUID, free.String())
	}

	s.rebindOrigins[claimUID] = current
	s.resourceClaimAllocations[claimUID] = target
	s.preparedCPUs = s.preparedCPUs.Union(target)
	logger.Info("began rebind of resource claim", "from", current.String(), "to", target.String())
	return nil
}

// CommitRebind releases the CPUs a claim moved away from, keeping the target.
func (s *CPUAllocation) CommitRebind(logger logr.Logger, claimUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	origin, ok := s.rebindOrigins[claimUID]
	if !ok {
		return fmt.Errorf("claim %q has no rebind in flight", claimUID)
	}
	target := s.resourceClaimAllocations[claimUID]
	delete(s.rebindOrigins, claimUID)
	s.preparedCPUs = s.preparedCPUs.Difference(origin.Difference(target))
	logger.Info("committed rebind of resource claim", "cpus", target.String())
	return nil
}

// AbortRebind returns a claim to the CPUs it was moving away from and releases
// the target.
func (s *CPUAllocation) AbortRebind(logger logr.Logger, claimUID types.UID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	origin, ok := s.rebindOrigins[claimUID]
	if !ok {
		return fmt.Errorf("claim %q has no rebind in flight", claimUID)
	}
	target := s.resourceClaimAllocations[claimUID]
	delete(s.rebindOrigins, claimUID)
	s.resourceClaimAllocations[claimUID] = origin
	s.preparedCPUs = s.preparedCPUs.Difference(target.Difference(origin))
	logger.Info("aborted rebind of resource claim", "cpus", origin.String())
	return nil
}

// GetRebindOrigin returns the CPUs a claim is moving away from, and whether a
// rebind is in flight for it at all.
func (s *CPUAllocation) GetRebindOrigin(claimUID types.UID) (cpuset.CPUSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	origin, ok := s.rebindOrigins[claimUID]
	return origin, ok
}

// RemoveResourceClaimAllocation removes a resource claim allocation from the store.
func (s *CPUAllocation) RemoveResourceClaimAllocation(logger logr.Logger, claimUID types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.resourceClaimAllocations[claimUID]; ok {
		s.removeLocked(claimUID)
		logger.Info("removed allocation for resource claim")
	}
}

// CCX-FORK: upstream releases only resourceClaimAllocations[claimUID]; a claim
// removed mid-rebind also holds the CPUs it was moving away from.
func (s *CPUAllocation) removeLocked(claimUID types.UID) {
	allocation, ok := s.resourceClaimAllocations[claimUID]
	if !ok {
		return
	}
	delete(s.resourceClaimAllocations, claimUID)
	origin := s.rebindOrigins[claimUID]
	delete(s.rebindOrigins, claimUID)
	s.preparedCPUs = s.preparedCPUs.Difference(allocation.Union(origin))
}

// GetSharedCPUs returns CPUs available to shared containers.
func (s *CPUAllocation) GetSharedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.availableCPUs.Difference(s.preparedCPUs)
}

// GetResourceClaimAllocation returns the cpuset for a given resource claim.
//
// CCX-FORK: while a rebind is in flight this is the claim's target, not the CPUs
// its container is still running on. A caller that needs both takes the other
// half from GetRebindOrigin.
func (s *CPUAllocation) GetResourceClaimAllocation(claimUID types.UID) (cpuset.CPUSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.resourceClaimAllocations[claimUID]
	return allocation, ok
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
		ActiveResourceClaims: len(s.resourceClaimAllocations),
	}
}
