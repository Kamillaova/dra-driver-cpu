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
	// claimUID => podUID(+containerName) mapping.
	// No claims can be shared by containers or pods
	// But a container can have more than a claim.
	ownerByClaimUID map[k8stypes.UID]OwnerIdent
	// reservedForByClaimUID records, for a prepared claim, the pod UIDs its own
	// status.reservedFor names at Prepare -- the API server's record of intent,
	// which a pod spec cannot forge the way it can a DRA_CPUSET_* env value.
	reservedForByClaimUID map[k8stypes.UID][]k8stypes.UID
}

func NewClaimTracker() *ClaimTracker {
	return &ClaimTracker{
		ownerByClaimUID:       make(map[k8stypes.UID]OwnerIdent),
		reservedForByClaimUID: make(map[k8stypes.UID][]k8stypes.UID),
	}
}

// SetOwner atomically binds claims to a single container. It returns the claims
// which were newly bound so callers can roll them back if a later operation fails.
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
		owner, ok := ctk.ownerByClaimUID[claimUID]
		if !ok || owner.Equal(curIdent) {
			continue
		}
		return nil, AlreadyOwned{
			ClaimUID: claimUID,
			Owner:    owner,
		}
	}

	newlyBound := make([]k8stypes.UID, 0, len(claimUIDs))
	for _, claimUID := range claimUIDs {
		if _, ok := ctk.ownerByClaimUID[claimUID]; ok {
			logger.V(2).Info("claim bound again to the same owner", "claimUID", claimUID)
			continue
		}
		ctk.ownerByClaimUID[claimUID] = curIdent
		newlyBound = append(newlyBound, claimUID)
		logger.V(4).Info("claim bound", "claimUID", claimUID)
	}
	return newlyBound, nil
}

// Owner returns the container a claim is bound to, and whether it is bound at
// all. A claim with no owner is one whose container has not been created yet, or
// one the driver prepared for a pod that never started.
func (ctk *ClaimTracker) Owner(claimUID k8stypes.UID) (OwnerIdent, bool) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	owner, ok := ctk.ownerByClaimUID[claimUID]
	return owner, ok
}

// SetReservedFor records the pod UIDs a claim's own reservation names at
// Prepare, replacing any previously recorded reservation for it.
func (ctk *ClaimTracker) SetReservedFor(claimUID k8stypes.UID, podUIDs []k8stypes.UID) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	ctk.reservedForByClaimUID[claimUID] = append([]k8stypes.UID(nil), podUIDs...)
}

// ReservedFor reports whether podUID is one of a claim's reserved consumers.
// recorded is false when the claim's reservation was never recorded at all
// (never prepared, or prepared before this driver started tracking it), which
// callers must not treat the same as an empty reservation.
func (ctk *ClaimTracker) ReservedFor(claimUID, podUID k8stypes.UID) (reserved, recorded bool) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	podUIDs, ok := ctk.reservedForByClaimUID[claimUID]
	if !ok {
		return false, false
	}
	for _, p := range podUIDs {
		if p == podUID {
			return true, true
		}
	}
	return false, true
}

func (ctk *ClaimTracker) Cleanup(claimUIDs ...k8stypes.UID) {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	for _, claimUID := range claimUIDs {
		delete(ctk.ownerByClaimUID, claimUID)
		delete(ctk.reservedForByClaimUID, claimUID)
	}
}

func (ctk *ClaimTracker) Len() int {
	ctk.mu.Lock()
	defer ctk.mu.Unlock()
	return len(ctk.ownerByClaimUID)
}
