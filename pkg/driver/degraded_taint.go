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
	"errors"
	"fmt"
	"slices"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

// checkDegradedPartitions refuses a result on a device of a partition whose smt
// the kernel contradicts, unless the partition's devices are published tainted
// and the claim's request for the result tolerates the taint. The request is
// read from the claim's spec: at Kubernetes 1.34 the scheduler copies no
// toleration into the allocation result.
func (cp *CPUDriver) checkDegradedPartitions(claim *resourceapi.ResourceClaim) error {
	taint := device.DegradedTaint()
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		partition, degraded := cp.topology.degradedDevices[alloc.Device]
		if !degraded {
			continue
		}
		if !cp.taintDegradedPartitions {
			return fmt.Errorf("request %q: partition %q is degraded (SiblingThreadsOnline) and its devices are not published (`taintDegradedPartitions: false`); take its sibling threads offline", alloc.Request, partition)
		}
		if cp.deviceTaintsDropped.Load() {
			return fmt.Errorf("request %q: partition %q is degraded (SiblingThreadsOnline) and its devices are not published; take its sibling threads offline", alloc.Request, partition)
		}
		tolerated := slices.ContainsFunc(requestTolerations(claim, alloc.Request), func(toleration resourceapi.DeviceToleration) bool {
			return resourceclaim.ToleratesTaint(toleration, taint)
		})
		if !tolerated {
			return fmt.Errorf("request %q: partition %q is degraded (SiblingThreadsOnline); tolerate dra.cpu/degraded=SiblingThreadsOnline:NoSchedule, or take its sibling threads offline", alloc.Request, partition)
		}
	}
	return nil
}

// requestTolerations returns the tolerations of the claim's request, or
// subrequest, a result belongs to.
func requestTolerations(claim *resourceapi.ResourceClaim, request string) []resourceapi.DeviceToleration {
	base := resourceclaim.BaseRequestRef(request)
	for _, req := range claim.Spec.Devices.Requests {
		if req.Name != base {
			continue
		}
		if req.Exactly != nil {
			return req.Exactly.Tolerations
		}
		for _, sub := range req.FirstAvailable {
			if resourceclaim.CreateSubRequestRef(base, sub.Name) == request {
				return sub.Tolerations
			}
		}
	}
	return nil
}

// handleDroppedTaints reports a slice the API server stored without the taint
// of a degraded partition's devices, which it drops when DRADeviceTaints is
// off, and reports whether err was that drop. The devices then stand
// untainted, so any claim can be allocated them, until the condition writer's
// worker publishes again without them; Prepare refuses every claim on them
// from now on. As handleDroppedMapping, it takes no lock and publishes
// nothing.
func (cp *CPUDriver) handleDroppedTaints(ctx context.Context, err error, msg string) bool {
	var dropped *resourceslice.DroppedFieldsError
	if !errors.As(err, &dropped) || dropped.DesiredSlice == nil || dropped.ActualSlice == nil ||
		!slices.Contains(dropped.DisabledFeatures(), "DRADeviceTaints") {
		return false
	}
	runtime.HandleErrorWithContext(ctx, err, "API server dropped the taint of degraded partitions' devices from a ResourceSlice, so they are withheld and every claim on them is refused; enable DRADeviceTaints on the API server, then restart the driver pod",
		"message", msg, "pool", dropped.PoolName, "slice", dropped.SliceIndex, "condition", conditionPartitionDegraded)
	cp.deviceTaintsDropped.Store(true)
	cp.nodeConditions.requeue()
	return true
}

// withholdDroppedTaints publishes again without the devices whose taint the
// API server dropped, once, and counts it.
func (cp *CPUDriver) withholdDroppedTaints(ctx context.Context, logger logr.Logger) {
	if !cp.deviceTaintsDropped.Load() || !cp.taintsWithheld.CompareAndSwap(false, true) {
		return
	}
	logger.Info("degraded partitions' devices withheld until the driver restarts: the API server dropped their taint", "node", cp.nodeName)
	cp.metrics.RecordDeviceTaintsDropped()
	cp.PublishResources(ctx)
}

// untaintedSlices leaves out the slices of tainted devices. A partition's
// devices are chunked apart from every other partition's, so a slice holds
// tainted devices alone.
func untaintedSlices(deviceSlices [][]resourceapi.Device) [][]resourceapi.Device {
	var kept [][]resourceapi.Device
	for _, chunk := range deviceSlices {
		if !slices.ContainsFunc(chunk, func(dev resourceapi.Device) bool { return len(dev.Taints) > 0 }) {
			kept = append(kept, chunk)
		}
	}
	return kept
}
