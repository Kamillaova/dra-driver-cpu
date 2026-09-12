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

package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
)

// placementWriter is one claim's turn at its own status.
type placementWriter struct {
	mu sync.Mutex
	// waiting counts who holds this writer or is queued behind it, so the last
	// one out can drop it from the map.
	waiting int
}

// lockClaimPlacement takes this claim's turn and returns the release.
func (cp *CPUDriver) lockClaimPlacement(claimUID types.UID) func() {
	cp.placementWritersMu.Lock()
	if cp.placementWriters == nil {
		cp.placementWriters = map[types.UID]*placementWriter{}
	}
	writer, ok := cp.placementWriters[claimUID]
	if !ok {
		writer = &placementWriter{}
		cp.placementWriters[claimUID] = writer
	}
	writer.waiting++
	cp.placementWritersMu.Unlock()

	writer.mu.Lock()
	return func() {
		writer.mu.Unlock()
		cp.placementWritersMu.Lock()
		writer.waiting--
		if writer.waiting == 0 {
			delete(cp.placementWriters, claimUID)
		}
		cp.placementWritersMu.Unlock()
	}
}

// publishClaimPlacementStatus updates the ResourceClaim object to publish the
// per-cache CPUs actually occupied by the claim.
func (cp *CPUDriver) publishClaimPlacementStatus(ctx context.Context, logger logr.Logger, claimUID types.UID) {
	if cp.kubeClient == nil || cp.claimReader == nil {
		return
	}

	// We do this asynchronously to avoid blocking.
	ctx = context.WithoutCancel(ctx)
	go func() {
		release := cp.lockClaimPlacement(claimUID)
		defer release()
		if err := cp.doPublishClaimPlacementStatus(ctx, logger, claimUID); err != nil {
			logger.Error(err, "failed to publish claim placement status", "claimUID", claimUID)
		}
	}()
}

func (cp *CPUDriver) doPublishClaimPlacementStatus(ctx context.Context, logger logr.Logger, claimUID types.UID) error {
	claims, err := cp.claimReader.AllocatedClaims()
	if err != nil {
		return fmt.Errorf("failed to list allocated claims: %w", err)
	}
	var originalClaim *resourceapi.ResourceClaim
	for _, c := range claims {
		if c.UID == claimUID {
			originalClaim = c
			break
		}
	}
	if originalClaim == nil {
		return fmt.Errorf("claim %s not found in cache", claimUID)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		claim, err := cp.kubeClient.ResourceV1().ResourceClaims(originalClaim.Namespace).Get(ctx, originalClaim.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}

		if claim.Status.Allocation == nil {
			return nil
		}

		// Inside the retry, because a conflict means something wrote this claim
		// in between, and a move is one of the things that writes it: a record
		// read once before the first attempt would republish the placement the
		// claim has already left.
		record, ok := cp.cpuAllocationStore.GetClaimRecord(claimUID)
		if !ok {
			return fmt.Errorf("claim %s has no allocation record", claimUID)
		}
		assignedCPUs := store.UnionOf(record.Requests)

		// Prepare map from device name to its CPUs
		deviceNameToCPUs := cp.topology.deviceNameToCPUs

		modified := false
		var newDevices []resourceapi.AllocatedDeviceStatus

		// keep existing ones that are not ours
		for _, d := range claim.Status.Devices {
			if d.Driver != cp.driverName {
				newDevices = append(newDevices, d)
			}
		}

		// create ones for our driver
		for _, result := range claim.Status.Allocation.Devices.Results {
			if result.Driver != cp.driverName {
				continue
			}

			devCPUs, ok := deviceNameToCPUs[result.Device]
			if !ok {
				continue
			}

			intersection := assignedCPUs.Intersection(devCPUs)

			placement := v1alpha1.ClaimPlacementStatus{
				APIVersion: v1alpha1.APIVersion,
				Kind:       "ClaimPlacementStatus",
				CPUSet:     intersection.String(),
				CPUCount:   intersection.Size(),
			}

			data, err := json.Marshal(placement)
			if err != nil {
				return err
			}

			status := resourceapi.AllocatedDeviceStatus{
				Driver: result.Driver,
				Pool:   result.Pool,
				Device: result.Device,
				Data:   &runtime.RawExtension{Raw: data},
			}
			newDevices = append(newDevices, status)
			modified = true
		}

		if !modified {
			return nil
		}

		claim.Status.Devices = newDevices

		_, err = cp.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
		return err
	})
}
