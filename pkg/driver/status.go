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
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
)

// claimStatus returns the status.devices entries of the claim's results on
// this driver's devices, one per result, each keyed as the API server keys
// them, by driver, pool, device and share. A result on a device this node no
// longer publishes gets none, its partition being unknown.
func (cp *CPUDriver) claimStatus(logger logr.Logger, claim *resourceapi.ResourceClaim) ([]resourceapi.AllocatedDeviceStatus, error) {
	record, ok := cp.allocationState.GetClaimRecord(claim.UID)
	if !ok {
		return nil, fmt.Errorf("claim %s/%s has no record to write in its status", claim.Namespace, claim.Name)
	}
	cpusKnown := requestCPUsKnown(claim, record, cp.driverName)
	var entries []resourceapi.AllocatedDeviceStatus
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
		raw, err := sortedJSON(data)
		if err != nil {
			return nil, fmt.Errorf("encode the status of device %q: %w", result.Device, err)
		}
		entry := resourceapi.AllocatedDeviceStatus{
			Driver: result.Driver,
			Pool:   result.Pool,
			Device: result.Device,
			Data:   &runtime.RawExtension{Raw: raw},
		}
		// The API server keys the entries by the result's share, and refuses
		// an entry for a result that has one without it.
		if result.ShareID != nil {
			entry.ShareID = new(string(*result.ShareID))
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// sortedJSON encodes v with its object keys sorted, the form the API server
// gives raw JSON when it applies to an object, so another writer's
// server-side apply to the claim leaves the entry's bytes as they are.
func sortedJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

// writeClaimStatus makes entries this driver's status.devices entries of the
// claim, every other driver's kept, by one update at the resourceVersion the
// claim was read at. Not an apply or a patch: the API server re-encodes the
// claim's opaque parameters on those paths, then refuses the write as a change
// of its allocation where a client sent them in another form
// (kubernetes/kubernetes#143901); an update over protobuf keeps their bytes.
func (cp *CPUDriver) writeClaimStatus(ctx context.Context, claim *resourceapi.ResourceClaim, entries []resourceapi.AllocatedDeviceStatus) error {
	claims := cp.kubeClient.ResourceV1().ResourceClaims(claim.Namespace)
	current := claim
	// The status strategy takes an update without a resourceVersion
	// unconditionally, over any write made since the claim was read.
	if claim.ResourceVersion == "" {
		current = nil
	}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if current == nil {
			fresh, err := claims.Get(ctx, claim.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if fresh.UID != claim.UID {
				return fmt.Errorf("the claim was replaced by one with UID %s", fresh.UID)
			}
			if !apiequality.Semantic.DeepEqual(fresh.Status.Allocation, claim.Status.Allocation) {
				return errors.New("its allocation changed since it was prepared")
			}
			current = fresh
		}
		devices, changed := withDriverEntries(current.Status.Devices, cp.driverName, entries)
		if !changed {
			return nil
		}
		update := current.DeepCopy()
		update.Status.Devices = devices
		current = nil
		_, err := claims.UpdateStatus(ctx, update, metav1.UpdateOptions{FieldManager: cp.driverName})
		return err
	})
	if err != nil {
		return fmt.Errorf("claim %s/%s: status not written: %w", claim.Namespace, claim.Name, err)
	}
	return nil
}

// withDriverEntries returns devices with the driver's entries replaced by
// entries and every other driver's kept, and whether that changes them.
func withDriverEntries(devices []resourceapi.AllocatedDeviceStatus, driver string, entries []resourceapi.AllocatedDeviceStatus) ([]resourceapi.AllocatedDeviceStatus, bool) {
	var own, merged []resourceapi.AllocatedDeviceStatus
	for _, entry := range devices {
		if entry.Driver == driver {
			own = append(own, entry)
			continue
		}
		merged = append(merged, entry)
	}
	if apiequality.Semantic.DeepEqual(own, entries) {
		return devices, false
	}
	return append(merged, entries...), true
}
