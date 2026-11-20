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

package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	resourceapplyv1 "k8s.io/client-go/applyconfigurations/resource/v1"
)

// claimStatus returns the status.devices entries of the claim's results on
// this driver's devices, one per result, each keyed as the API server keys
// them, by driver, pool, device and share. A result on a device this node no
// longer publishes gets none, its partition being unknown.
func (cp *CPUDriver) claimStatus(logger logr.Logger, claim *resourceapi.ResourceClaim) (*resourceapplyv1.ResourceClaimStatusApplyConfiguration, error) {
	record, ok := cp.allocationState.GetClaimRecord(claim.UID)
	if !ok {
		return nil, fmt.Errorf("claim %s/%s has no record to write in its status", claim.Namespace, claim.Name)
	}
	cpusKnown := requestCPUsKnown(claim, record, cp.driverName)
	status := resourceapplyv1.ResourceClaimStatus()
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != cp.driverName {
			continue
		}
		attrs, published := getDeviceAttributes(cp.topology.deviceSlices, result.Device)
		partition := attrs[device.AttributePartition].StringValue
		if !published || partition == nil {
			logger.V(2).Info("writing no status for a result on a device this node does not publish", "request", result.Request, "device", result.Device)
			continue
		}
		data := v1alpha1.ClaimPlacementStatus{APIVersion: v1alpha1.APIVersion, Kind: v1alpha1.ClaimPlacementStatusKind, Partition: *partition}
		_, cpus, err := cp.resultExclusiveCPUs(claim, record, result, cpusKnown)
		if err != nil {
			return nil, err
		}
		if !cpus.IsEmpty() {
			data.CPUSet = cpus.String()
		}
		data.Group = requestGroup(record, metadataRef(claim.UID, record, result.Request).Request)
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("encode the status of device %q: %w", result.Device, err)
		}
		entry := resourceapplyv1.AllocatedDeviceStatus().
			WithDriver(result.Driver).
			WithPool(result.Pool).
			WithDevice(result.Device).
			WithData(runtime.RawExtension{Raw: raw})
		// The API server keys the entries by the result's share, and refuses
		// an entry for a result that has one without it.
		if result.ShareID != nil {
			entry.WithShareID(string(*result.ShareID))
		}
		status.WithDevices(entry)
	}
	return status, nil
}

// applyClaimStatus writes the driver's entries of a claim's status in one
// server-side apply, forcing its ownership of them, on the claim with the UID
// it was prepared for.
func (cp *CPUDriver) applyClaimStatus(ctx context.Context, claim *resourceapi.ResourceClaim, status *resourceapplyv1.ResourceClaimStatusApplyConfiguration) error {
	apply := resourceapplyv1.ResourceClaim(claim.Name, claim.Namespace).WithUID(claim.UID).WithStatus(status)
	if _, err := cp.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).ApplyStatus(ctx, apply, metav1.ApplyOptions{FieldManager: cp.driverName, Force: true}); err != nil {
		return fmt.Errorf("claim %s/%s: status not written: %w", claim.Namespace, claim.Name, err)
	}
	return nil
}
