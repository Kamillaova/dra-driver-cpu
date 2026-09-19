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

package store

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

type AlreadyOwned struct {
	ClaimUID k8stypes.UID
	Owner    OwnerIdent
}

func (ao AlreadyOwned) Error() string {
	return fmt.Sprintf("claimUID %q already bound to pod %q container %q", ao.ClaimUID, ao.Owner.PodUID, ao.Owner.ContainerName)
}

type OwnerIdent struct {
	PodUID        k8stypes.UID
	ContainerName string
}

func (oi OwnerIdent) Equal(x OwnerIdent) bool {
	return oi.PodUID == x.PodUID && oi.ContainerName == x.ContainerName
}

type ClaimTracker struct {
	mu sync.Mutex
	// claimUID => the containers holding it, in the order they took it.
	//
	// CCX-FORK: upstream binds a claim to one container and refuses every other.
	// A claim binds to one pod here, and every container of that pod may hold it:
	// the shape a workload needs when an init container reads the claim's device
	// metadata to render the configuration the long-running container then uses,
	// which upstream's rule makes impossible because the init container takes the
	// binding first and never gives it back.
	ownersByClaimUID map[k8stypes.UID][]OwnerIdent
	// reservedForByClaimUID records, for a prepared claim, the pod UIDs its own
	// status.reservedFor names at Prepare -- the API server's record of intent,
	// which a pod spec cannot forge the way it can a DRA_CPUSET_* env value.
	reservedForByClaimUID map[k8stypes.UID]ClaimReservation
}

// ClaimReservation is what a claim's status.reservedFor names: the pods it was
// reserved for, and the pod groups.
//
// A group is named rather than identified, because that is all a pod can check
// itself against: a pod carries the name of its group in its spec and never its
// UID, which is why the upstream helper compares the name too.
type ClaimReservation struct {
	PodUIDs   []k8stypes.UID
	PodGroups []string
}

// HasPod reports whether the reservation names this pod outright, which is the
// whole answer for a claim reserved for pods alone.
func (r ClaimReservation) HasPod(podUID k8stypes.UID) bool {
	return slices.Contains(r.PodUIDs, podUID)
}

// HasGroup reports whether the reservation names this pod group.
func (r ClaimReservation) HasGroup(name string) bool {
	return name != "" && slices.Contains(r.PodGroups, name)
}

// IsEmpty reports whether the reservation names nothing at all.
func (r ClaimReservation) IsEmpty() bool {
	return len(r.PodUIDs) == 0 && len(r.PodGroups) == 0
}

func NewClaimTracker() *ClaimTracker {
	return &ClaimTracker{
		ownersByClaimUID:      make(map[k8stypes.UID][]OwnerIdent),
		reservedForByClaimUID: make(map[k8stypes.UID]ClaimReservation),
	}
}

// SetOwner atomically binds claims to a container. It returns the claims this
// container newly took, so callers can roll them back if a later operation
// fails.
//
// A claim already held by a container of another pod is refused: its CPUs are
// the first pod's, and handing them to a second pod would put two workloads on
// cores each was promised alone.
func (ctk *ClaimTracker) SetOwner(logger logr.Logger, podUID k8stypes.UID, containerName string, claimUIDs ...k8stypes.UID) ([]k8stypes.UID, error) {
	if len(claimUIDs) == 0 {
		return nil, errors.New("no claims to bind")
	}
	curIdent := OwnerIdent{
		PodUID:        podUID,
		ContainerName: containerName,
	}
	ctk.mu.Lock()
	defer ctk.mu.Unlock()

	for _, claimUID := range claimUIDs {
		owners := ctk.ownersByClaimUID[claimUID]
		if len(owners) == 0 || owners[0].PodUID == podUID {
			continue
		}
		return nil, AlreadyOwned{
			ClaimUID: claimUID,
			Owner:    owners[0],
		}
	}

	newlyBound := make([]k8stypes.UID, 0, len(claimUIDs))
	for _, claimUID := range claimUIDs {
		if slices.ContainsFunc(ctk.ownersByClaimUID[claimUID], curIdent.Equal) {
			logger.V(2).Info("claim bound again to the same container", "claimUID", claimUID)
			continue
		}
		ctk.ownersByClaimUID[claimUID] = append(ctk.ownersByClaimUID[claimUID], curIdent)
		newlyBound = append(newlyBound, claimUID)
		logger.V(4).Info("claim bound", "claimUID", claimUID)
	}
	return newlyBound, nil
}

// Owners returns the containers a claim is bound to, in the order they took it,
// and whether it is bound at all. A claim with no owner is one whose container
// has not been created yet, or one the driver prepared for a pod that never
// started.
func (ctk *ClaimTracker) Owners(claimUID k8stypes.UID) ([]OwnerIdent, bool) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	owners := ctk.ownersByClaimUID[claimUID]
	if len(owners) == 0 {
		return nil, false
	}
	return slices.Clone(owners), true
}

// ReleaseOwner undoes one container's binding and leaves everything else in
// place: the claim's other containers keep theirs, and the reservation recorded
// at Prepare stays recorded. It is what a CreateContainer that fails after
// binding rolls back with; Cleanup is Unprepare's, and takes the whole claim.
func (ctk *ClaimTracker) ReleaseOwner(podUID k8stypes.UID, containerName string, claimUIDs ...k8stypes.UID) {
	ident := OwnerIdent{
		PodUID:        podUID,
		ContainerName: containerName,
	}
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	for _, claimUID := range claimUIDs {
		kept := slices.DeleteFunc(ctk.ownersByClaimUID[claimUID], ident.Equal)
		if len(kept) == 0 {
			delete(ctk.ownersByClaimUID, claimUID)
			continue
		}
		ctk.ownersByClaimUID[claimUID] = kept
	}
}

// SetReservedFor records what a claim's own reservation names at Prepare,
// replacing any previously recorded reservation for it.
func (ctk *ClaimTracker) SetReservedFor(claimUID k8stypes.UID, reservation ClaimReservation) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	ctk.reservedForByClaimUID[claimUID] = ClaimReservation{
		PodUIDs:   append([]k8stypes.UID(nil), reservation.PodUIDs...),
		PodGroups: append([]string(nil), reservation.PodGroups...),
	}
}

// ReservedFor returns a claim's recorded reservation. recorded is false when the
// reservation was never recorded at all (never prepared, or prepared before this
// driver started tracking it), which callers must not treat the same as a
// reservation that names nothing.
func (ctk *ClaimTracker) ReservedFor(claimUID k8stypes.UID) (reservation ClaimReservation, recorded bool) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	reservation, ok := ctk.reservedForByClaimUID[claimUID]
	return reservation, ok
}

func (ctk *ClaimTracker) Cleanup(claimUIDs ...k8stypes.UID) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	for _, claimUID := range claimUIDs {
		delete(ctk.ownersByClaimUID, claimUID)
		delete(ctk.reservedForByClaimUID, claimUID)
	}
}

func (ctk *ClaimTracker) Len() int {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	return len(ctk.ownersByClaimUID)
}
