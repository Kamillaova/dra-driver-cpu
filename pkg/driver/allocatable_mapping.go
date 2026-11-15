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

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

const (
	conditionAllocatableMappingDropped corev1.NodeConditionType = "CPUAllocatableMappingDropped"

	allocatableMappingDroppedEffect = "so dra.cpu claims are not counted against node cpu; enable DRANodeAllocatableResources on the API server, then restart the driver pod"
)

var (
	allocatableMappingDropped = nodeConditionValue{
		status:  corev1.ConditionTrue,
		reason:  "DroppedByAPIServer",
		message: "API server dropped nodeAllocatableResources from dra.cpu ResourceSlices, " + allocatableMappingDroppedEffect,
	}
	allocatableMappingPublished = nodeConditionValue{
		status:  corev1.ConditionFalse,
		reason:  "MappingPublished",
		message: "nodeAllocatableResources kept in dra.cpu ResourceSlices since the driver started",
	}
)

// allocatableMappingCondition reports whether the API server has dropped the
// node-allocatable mapping from a slice since the driver started.
func (cp *CPUDriver) allocatableMappingCondition() nodeConditionSource {
	return nodeConditionSource{
		conditionType: conditionAllocatableMappingDropped,
		evaluate: func() nodeConditionValue {
			if cp.allocatableMappingDropped.Load() {
				return allocatableMappingDropped
			}
			return allocatableMappingPublished
		},
		stoppedMessage: "driver stopped; mapping not checked",
	}
}

// handleDroppedMapping reports a slice the API server stored without the
// node-allocatable mapping, which it drops, without refusing the slice, when
// DRANodeAllocatableResources is off, and reports whether err was that drop.
// The library's error names the disabled features "unknown", so the driver
// logs its own message, with the effect and the fix, in place of the
// library's. It leaves the devices published and publishes nothing: at
// v1.37.0 the plugin's shutdown holds the lock PublishResources takes while
// it waits for the worker reporting the error.
func (cp *CPUDriver) handleDroppedMapping(ctx context.Context, err error, msg string) bool {
	var dropped *resourceslice.DroppedFieldsError
	if !errors.As(err, &dropped) || !carriesAllocatableMapping(dropped.DesiredSlice) || carriesAllocatableMapping(dropped.ActualSlice) {
		return false
	}
	runtime.HandleErrorWithContext(ctx, err, "API server dropped nodeAllocatableResources from a ResourceSlice, "+allocatableMappingDroppedEffect,
		"message", msg, "pool", dropped.PoolName, "slice", dropped.SliceIndex, "condition", conditionAllocatableMappingDropped)
	cp.metrics.RecordNodeAllocatableMappingDropped()
	cp.allocatableMappingDropped.Store(true)
	cp.nodeConditions.requeue()
	return true
}

func carriesAllocatableMapping(slice *resourceapi.ResourceSlice) bool {
	if slice == nil {
		return false
	}
	for _, d := range slice.Spec.Devices {
		if len(d.NodeAllocatableResources) > 0 {
			return true
		}
	}
	return false
}
