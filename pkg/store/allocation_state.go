/*
Copyright The Kubernetes Authors.

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
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

var (
	ErrClaimAlreadyBound = errors.New("claim is already bound to another container")
	ErrClaimCPUMismatch  = errors.New("claim cpuset does not match the prepared allocation")
	ErrUnknownClaim      = errors.New("claim is not prepared by this driver")
	ErrUnknownRequest    = errors.New("claim was prepared without the request")
	ErrClaimOverlap      = errors.New("claim shares CPUs with another prepared claim")
	ErrEmptyClaimList    = errors.New("claim list is empty")
)

type ClaimAlreadyBoundError struct {
	ClaimUID types.UID
	Owner    OwnerIdent
}

func (err ClaimAlreadyBoundError) Error() string {
	return fmt.Sprintf("claimUID %q is already bound to pod %q container %q", err.ClaimUID, err.Owner.PodUID, err.Owner.ContainerName)
}

func (err ClaimAlreadyBoundError) Unwrap() error {
	return ErrClaimAlreadyBound
}

type ClaimCPUMismatchError struct {
	ClaimUID types.UID
	Request  string
	Expected cpuset.CPUSet
	Got      cpuset.CPUSet
}

func (err ClaimCPUMismatchError) Error() string {
	if err.Request != "" {
		return fmt.Sprintf("validation failed for claim %q request %q: cpuset mismatch (expected %q, got %q)", err.ClaimUID, err.Request, err.Expected.String(), err.Got.String())
	}
	return fmt.Sprintf("validation failed for claim %q: cpuset mismatch (expected %q, got %q)", err.ClaimUID, err.Expected.String(), err.Got.String())
}

func (err ClaimCPUMismatchError) Unwrap() error {
	return ErrClaimCPUMismatch
}

type UnknownClaimError struct {
	ClaimUID types.UID
}

func (err UnknownClaimError) Error() string {
	return fmt.Sprintf("claim %q is not prepared by this driver", err.ClaimUID)
}

func (err UnknownClaimError) Unwrap() error {
	return ErrUnknownClaim
}

type UnknownRequestError struct {
	ClaimUID types.UID
	Request  string
}

func (err UnknownRequestError) Error() string {
	return fmt.Sprintf("claim %q was prepared without request %q", err.ClaimUID, err.Request)
}

func (err UnknownRequestError) Unwrap() error {
	return ErrUnknownRequest
}

type ClaimOverlapError struct {
	ClaimUID types.UID
	Other    types.UID
	CPUs     cpuset.CPUSet
}

func (err ClaimOverlapError) Error() string {
	return fmt.Sprintf("claim %q shares CPUs %q with prepared claim %q", err.ClaimUID, err.CPUs.String(), err.Other)
}

func (err ClaimOverlapError) Unwrap() error {
	return ErrClaimOverlap
}

// AssignmentOverlapError refuses to reserve CPUs that prepared claims hold.
type AssignmentOverlapError struct {
	ClaimUID types.UID
	CPUs     cpuset.CPUSet
	Holders  ClaimHolders
}

func (err AssignmentOverlapError) Error() string {
	msg := fmt.Sprintf("claim %q has overlapping CPU assignment %q", err.ClaimUID, err.CPUs.String())
	if len(err.Holders) == 0 {
		return msg
	}
	return msg + ": " + err.Holders.String()
}

func (err AssignmentOverlapError) Unwrap() error {
	return ErrClaimOverlap
}

// ClaimHolder is a prepared claim holding some CPUs of a set.
type ClaimHolder struct {
	ClaimUID  types.UID
	Namespace string
	Name      string
	CPUs      cpuset.CPUSet
	Owner     OwnerIdent
}

func (h ClaimHolder) String() string {
	claim := fmt.Sprintf("claim %s/%s (UID %q)", h.Namespace, h.Name, h.ClaimUID)
	if h.Name == "" {
		claim = fmt.Sprintf("claim with UID %q", h.ClaimUID)
	}
	owner := "bound to no container"
	if !h.Owner.Vacant() {
		owner = fmt.Sprintf("bound to container %q of pod %q", h.Owner.ContainerName, h.Owner.PodUID)
	}
	return fmt.Sprintf("%s holds CPUs %s, %s", claim, h.CPUs.String(), owner)
}

// ClaimHolders are the prepared claims holding some CPUs of a set, in order of
// claim UID.
type ClaimHolders []ClaimHolder

func (hs ClaimHolders) String() string {
	held := make([]string, 0, len(hs)+1)
	for _, h := range hs {
		held = append(held, h.String())
	}
	held = append(held, `see "CPUs Held by Another Claim" in the driver's troubleshooting guide`)
	return strings.Join(held, "; ")
}

type OwnerIdent struct {
	PodUID        types.UID
	ContainerName string
}

func (oi OwnerIdent) Vacant() bool {
	return oi.PodUID == types.UID("") && oi.ContainerName == ""
}

func (oi OwnerIdent) Equal(ident OwnerIdent) bool {
	return oi.PodUID == ident.PodUID && oi.ContainerName == ident.ContainerName
}

// RequestAllocation is the CPUs one request of a claim was given.
type RequestAllocation struct {
	Request string
	CPUs    cpuset.CPUSet
}

// ClaimRequestRef names one request of a claim, or the whole claim where
// Request is empty.
type ClaimRequestRef struct {
	ClaimUID types.UID
	Request  string
}

func compareClaimRequestRefs(a, b ClaimRequestRef) int {
	return cmp.Or(strings.Compare(string(a.ClaimUID), string(b.ClaimUID)), strings.Compare(a.Request, b.Request))
}

// ClaimRecord is what the store records of a prepared claim.
type ClaimRecord struct {
	Namespace string
	Name      string
	Requests  []RequestAllocation
}

// CPUs returns the CPUs of every request of the claim.
func (r ClaimRecord) CPUs() cpuset.CPUSet {
	cpus := cpuset.New()
	for _, request := range r.Requests {
		cpus = cpus.Union(request.CPUs)
	}
	return cpus
}

type claimAllocation struct {
	cpus      cpuset.CPUSet
	requests  []RequestAllocation
	namespace string
	name      string
	owner     OwnerIdent
}

func newClaimAllocation(record ClaimRecord) claimAllocation {
	sorted := cloneRequests(record.Requests)
	slices.SortFunc(sorted, func(a, b RequestAllocation) int { return strings.Compare(a.Request, b.Request) })
	return claimAllocation{cpus: record.CPUs(), requests: sorted, namespace: record.Namespace, name: record.Name}
}

func (ca claimAllocation) record() ClaimRecord {
	return ClaimRecord{Namespace: ca.namespace, Name: ca.name, Requests: cloneRequests(ca.requests)}
}

func cloneRequests(requests []RequestAllocation) []RequestAllocation {
	cloned := make([]RequestAllocation, len(requests))
	for i, request := range requests {
		cloned[i] = RequestAllocation{Request: request.Request, CPUs: request.CPUs.Clone()}
	}
	return cloned
}

func (ca claimAllocation) cpusOf(request string) (cpuset.CPUSet, bool) {
	if request == "" {
		return ca.cpus, true
	}
	for _, held := range ca.requests {
		if held.Request == request {
			return held.CPUs, true
		}
	}
	return cpuset.CPUSet{}, false
}

func (ca claimAllocation) sameRequests(requests []RequestAllocation) bool {
	return slices.EqualFunc(ca.requests, requests, func(a, b RequestAllocation) bool {
		return a.Request == b.Request && a.CPUs.Equals(b.CPUs)
	})
}

// Acquire returns an owned copy if the allocation has no owner.
func (ca claimAllocation) Acquire(owner OwnerIdent) (claimAllocation, bool) {
	if !ca.owner.Vacant() {
		return claimAllocation{}, false
	}
	return claimAllocation{
		cpus:      ca.cpus.Clone(),
		requests:  cloneRequests(ca.requests),
		namespace: ca.namespace,
		name:      ca.name,
		owner:     owner,
	}, true
}

// Release returns a copy without ownership, retaining its CPUs.
func (ca claimAllocation) Release() claimAllocation {
	return claimAllocation{
		cpus:      ca.cpus.Clone(),
		requests:  cloneRequests(ca.requests),
		namespace: ca.namespace,
		name:      ca.name,
		// intentional empty owner
	}
}

func (ca claimAllocation) Clone() claimAllocation {
	return claimAllocation{
		cpus:      ca.cpus.Clone(),
		requests:  cloneRequests(ca.requests),
		namespace: ca.namespace,
		name:      ca.name,
		owner: OwnerIdent{
			PodUID:        ca.owner.PodUID,
			ContainerName: ca.owner.ContainerName,
		},
	}
}

// AllocationState is the single source of truth for CPU allocations.
type AllocationState struct {
	mu                       sync.RWMutex
	availableCPUs            cpuset.CPUSet
	reservedCPUs             cpuset.CPUSet
	preparedCPUs             cpuset.CPUSet
	resourceClaimAllocations map[types.UID]claimAllocation
}

// AllocationSnapshot is a point-in-time summary of CPU allocation state.
type AllocationSnapshot struct {
	AllocatedCPUs        int
	AvailableCPUs        int
	ReservedCPUs         int
	ActiveResourceClaims int
}

// NewAllocationState creates a new AllocationState.
func NewAllocationState(cpuTopology *cpuinfo.CPUTopology, reservedCPUs cpuset.CPUSet) *AllocationState {
	cpuIDs := []int{}
	for cpuID := range cpuTopology.CPUDetails {
		cpuIDs = append(cpuIDs, cpuID)
	}
	allCPUsSet := cpuset.New(cpuIDs...)
	availableCPUs := allCPUsSet.Difference(reservedCPUs)

	return &AllocationState{
		availableCPUs:            availableCPUs,
		reservedCPUs:             reservedCPUs,
		preparedCPUs:             cpuset.New(),
		resourceClaimAllocations: make(map[types.UID]claimAllocation),
	}
}

// Reserve records a prepared claim and the CPUs each of its requests was given.
// Its CPUs remain unavailable to shared containers and other exclusive claims
// until Unprepare. When shared containers are present, the reservation must
// leave at least one CPU in the shared pool because NRI cannot represent an
// empty CPUSet.
func (s *AllocationState) Reserve(logger logr.Logger, claimUID types.UID, record ClaimRecord, hasSharedContainers bool) error {
	logger = logger.WithValues("claimUID", claimUID)
	reserved := newClaimAllocation(record)
	cpus := reserved.cpus
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocation, ok := s.resourceClaimAllocations[claimUID]; ok {
		if allocation.sameRequests(reserved.requests) {
			return nil
		}
		return fmt.Errorf("claim %q is already prepared with CPUs %q (requested %q)", claimUID, allocation.cpus.String(), cpus.String())
	}
	sharedCPUs := s.availableCPUs.Difference(s.preparedCPUs)
	if !cpus.IsSubsetOf(sharedCPUs) {
		return AssignmentOverlapError{ClaimUID: claimUID, CPUs: cpus, Holders: s.holdersLocked(cpus)}
	}
	if hasSharedContainers && !cpus.IsEmpty() && sharedCPUs.Difference(cpus).IsEmpty() {
		return fmt.Errorf("claim %q would exhaust the shared CPU pool while shared containers are running", claimUID)
	}
	s.resourceClaimAllocations[claimUID] = reserved
	s.preparedCPUs = s.preparedCPUs.Union(cpus)
	logger.Info("reserved allocation for resource claim", "cpus", cpus.String())
	return nil
}

// Restore records a claim an earlier process prepared, with the CPUs that
// process recorded for each of its requests. Unlike Reserve it keeps a claim
// whose CPUs overlap another claim's, and Bind refuses both until one of them
// is released. A claim already recorded is left as it is.
func (s *AllocationState) Restore(logger logr.Logger, claimUID types.UID, record ClaimRecord) {
	logger = logger.WithValues("claimUID", claimUID)
	restored := newClaimAllocation(record)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.resourceClaimAllocations[claimUID]; ok {
		return
	}
	s.resourceClaimAllocations[claimUID] = restored
	s.preparedCPUs = s.preparedCPUs.Union(restored.cpus)
	logger.Info("restored allocation for resource claim", "cpus", restored.cpus.String())
}

// Release removes a prepared claim, releasing its CPUs and ownership.
func (s *AllocationState) Release(logger logr.Logger, claimUID types.UID) {
	logger = logger.WithValues("claimUID", claimUID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if allocInfo, ok := s.resourceClaimAllocations[claimUID]; ok {
		s.removeLocked(claimUID)
		logger.Info("removed allocation for resource claim", "cpus", allocInfo.cpus.String())
	}
}

// Bind validates the requests a container names, comparing the CPUs of those
// in expected, and atomically binds their claims to the container. It returns
// only newly bound claims so callers can roll back their ownership.
func (s *AllocationState) Bind(logger logr.Logger, podUID types.UID, containerName string, named []ClaimRequestRef, expected map[ClaimRequestRef]cpuset.CPUSet) ([]types.UID, error) {
	refs := append(slices.Collect(maps.Keys(expected)), named...)
	slices.SortFunc(refs, compareClaimRequestRefs)
	refs = slices.Compact(refs)
	if len(refs) == 0 {
		return nil, ErrEmptyClaimList
	}
	logger = logger.WithValues("podUID", podUID, "containerName", containerName)
	curIdent := OwnerIdent{
		PodUID:        podUID,
		ContainerName: containerName,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var claimUIDs []types.UID
	for _, ref := range refs {
		allocation, ok := s.resourceClaimAllocations[ref.ClaimUID]
		if !ok {
			return nil, UnknownClaimError{
				ClaimUID: ref.ClaimUID,
			}
		}
		held, ok := allocation.cpusOf(ref.Request)
		if !ok {
			return nil, UnknownRequestError(ref)
		}
		if got, compare := expected[ref]; compare && !held.Equals(got) {
			return nil, ClaimCPUMismatchError{
				ClaimUID: ref.ClaimUID,
				Request:  ref.Request,
				Expected: held,
				Got:      got,
			}
		}
		if !allocation.owner.Vacant() && !allocation.owner.Equal(curIdent) {
			return nil, ClaimAlreadyBoundError{
				ClaimUID: ref.ClaimUID,
				Owner:    allocation.owner,
			}
		}
		if err := s.overlapLocked(ref.ClaimUID, allocation.cpus); err != nil {
			return nil, err
		}
		if !slices.Contains(claimUIDs, ref.ClaimUID) {
			claimUIDs = append(claimUIDs, ref.ClaimUID)
		}
	}

	updatedClaims := []types.UID{}
	for _, claimUID := range claimUIDs {
		allocation := s.resourceClaimAllocations[claimUID]
		allocInfo, acquired := allocation.Acquire(curIdent)
		if !acquired {
			logger.V(2).Info("claim bound again to the same owner", "claimUID", claimUID)
			continue
		}
		s.resourceClaimAllocations[claimUID] = allocInfo
		updatedClaims = append(updatedClaims, claimUID)
		logger.V(4).Info("claim bound", "claimUID", claimUID)
	}
	return updatedClaims, nil
}

// Unbind detaches a claim from the container it was bound to, but doesn't release the prepared claim.
func (s *AllocationState) Unbind(logger logr.Logger, claimUIDs ...types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, claimUID := range claimUIDs {
		if allocation, ok := s.resourceClaimAllocations[claimUID]; ok {
			oldOwner := allocation.owner
			s.resourceClaimAllocations[claimUID] = allocation.Release()
			logger.V(4).Info("claim unbound", "claimUID", claimUID, "previousPodUID", oldOwner.PodUID, "previousContainerName", oldOwner.ContainerName)
		}
	}
}

// Owner returns the container a claim is bound to, and false if the claim is
// not prepared or not bound.
func (s *AllocationState) Owner(claimUID types.UID) (OwnerIdent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, found := s.resourceClaimAllocations[claimUID]
	if !found || info.owner.Vacant() {
		return OwnerIdent{}, false
	}
	return info.owner, true
}

// IsOwner checks if a claim, identified by its UID, is owned by the given container
func (s *AllocationState) IsOwner(claimUID types.UID, podUID types.UID, containerName string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, found := s.resourceClaimAllocations[claimUID]
	if !found {
		return false
	}
	if info.owner.Vacant() {
		return false
	}
	return info.owner.Equal(OwnerIdent{PodUID: podUID, ContainerName: containerName})
}

// GetSharedCPUs returns CPUs available to shared containers.
func (s *AllocationState) GetSharedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.availableCPUs.Difference(s.preparedCPUs)
}

// GetResourceClaimAllocation returns the cpuset for a given resource claim.
func (s *AllocationState) GetResourceClaimAllocation(claimUID types.UID) (cpuset.CPUSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.resourceClaimAllocations[claimUID]
	return allocation.cpus, ok
}

// GetClaimRecord returns what the store records of a prepared claim.
func (s *AllocationState) GetClaimRecord(claimUID types.UID) (ClaimRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocation, ok := s.resourceClaimAllocations[claimUID]
	if !ok {
		return ClaimRecord{}, false
	}
	return allocation.record(), true
}

// GetRequestAllocationUnion returns the CPUs the referenced requests were given together.
func (s *AllocationState) GetRequestAllocationUnion(refs ...ClaimRequestRef) (cpuset.CPUSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	union := cpuset.New()
	for _, ref := range refs {
		allocation, ok := s.resourceClaimAllocations[ref.ClaimUID]
		if !ok {
			return cpuset.New(), UnknownClaimError{ClaimUID: ref.ClaimUID}
		}
		cpus, ok := allocation.cpusOf(ref.Request)
		if !ok {
			return cpuset.New(), UnknownRequestError(ref)
		}
		union = union.Union(cpus)
	}
	return union, nil
}

// Holders returns the prepared claims holding any of cpus, from their records,
// with no API call.
func (s *AllocationState) Holders(cpus cpuset.CPUSet) ClaimHolders {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.holdersLocked(cpus)
}

func (s *AllocationState) holdersLocked(cpus cpuset.CPUSet) ClaimHolders {
	var holders ClaimHolders
	for _, uid := range slices.Sorted(maps.Keys(s.resourceClaimAllocations)) {
		allocation := s.resourceClaimAllocations[uid]
		if held := cpus.Intersection(allocation.cpus); !held.IsEmpty() {
			holders = append(holders, ClaimHolder{ClaimUID: uid, Namespace: allocation.namespace, Name: allocation.name, CPUs: held, Owner: allocation.owner})
		}
	}
	return holders
}

// GetReservedCPUs returns the set of reserved CPUs.
func (s *AllocationState) GetReservedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reservedCPUs
}

// GetPreparedCPUs returns the CPUs reserved for prepared claims.
func (s *AllocationState) GetPreparedCPUs() cpuset.CPUSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.preparedCPUs
}

// Snapshot returns a point-in-time summary of CPU allocation state.
func (s *AllocationState) Snapshot() AllocationSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return AllocationSnapshot{
		AllocatedCPUs:        s.preparedCPUs.Size(),
		AvailableCPUs:        s.availableCPUs.Difference(s.preparedCPUs).Size(),
		ReservedCPUs:         s.reservedCPUs.Size(),
		ActiveResourceClaims: len(s.resourceClaimAllocations),
	}
}

// Clone copies allocation state without sharing maps or locks.
func (s *AllocationState) Clone() *AllocationState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allocations := make(map[types.UID]claimAllocation, len(s.resourceClaimAllocations))
	for uid, info := range s.resourceClaimAllocations {
		allocations[uid] = info.Clone()
	}

	return &AllocationState{
		availableCPUs:            s.availableCPUs.Clone(),
		reservedCPUs:             s.reservedCPUs.Clone(),
		preparedCPUs:             s.preparedCPUs.Clone(),
		resourceClaimAllocations: allocations,
	}
}

func (s *AllocationState) removeLocked(claimUID types.UID) {
	if _, ok := s.resourceClaimAllocations[claimUID]; !ok {
		return
	}
	delete(s.resourceClaimAllocations, claimUID)
	// Restored claims can share a CPU, which stays prepared until the last of them goes.
	prepared := cpuset.New()
	for _, allocation := range s.resourceClaimAllocations {
		prepared = prepared.Union(allocation.cpus)
	}
	s.preparedCPUs = prepared
}

func (s *AllocationState) overlapLocked(claimUID types.UID, cpus cpuset.CPUSet) error {
	for _, otherUID := range slices.Sorted(maps.Keys(s.resourceClaimAllocations)) {
		if otherUID == claimUID {
			continue
		}
		if shared := cpus.Intersection(s.resourceClaimAllocations[otherUID].cpus); !shared.IsEmpty() {
			return ClaimOverlapError{ClaimUID: claimUID, Other: otherUID, CPUs: shared}
		}
	}
	return nil
}
