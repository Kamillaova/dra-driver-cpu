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

package claim

import (
	"context"
	"fmt"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// WithSelector returns a copy of spec whose exact requests also select by the
// CEL expression cel, or an unchanged copy when cel is empty.
func WithSelector(spec resourcev1.ResourceClaimSpec, cel string) resourcev1.ResourceClaimSpec {
	out := *spec.DeepCopy()
	if cel == "" {
		return out
	}
	for i := range out.Devices.Requests {
		if exact := out.Devices.Requests[i].Exactly; exact != nil {
			exact.Selectors = append(exact.Selectors, resourcev1.DeviceSelector{
				CEL: &resourcev1.CELDeviceSelector{Expression: cel},
			})
		}
	}
	return out
}

// NUMANodeSelector is a CEL expression selecting the devices of NUMA node
// numaID where groupBy publishes devices that each lie in one NUMA node, and
// empty under any other grouping.
func NUMANodeSelector(groupBy string, numaID int) string {
	if groupBy != device.GROUP_BY_NUMA_NODE && groupBy != device.GROUP_BY_UNCORE_CACHE {
		return ""
	}
	return fmt.Sprintf(`device.attributes["resource.kubernetes.io"].numaNode == %d`, numaID)
}

// AllocationStep is the amount a CPU request on nodeName is rounded up to a
// multiple of, read from the request policy driverName publishes there: the
// step, or the minimum on a device too small for a step, else 1.
func AllocationStep(ctx context.Context, cs kubernetes.Interface, driverName, nodeName string) (int, error) {
	sliceList, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{
		FieldSelector: "spec.driver=" + driverName,
	})
	if err != nil {
		return 0, fmt.Errorf("list ResourceSlices of driver %s: %w", driverName, err)
	}
	for _, slice := range sliceList.Items {
		if slice.Spec.Driver != driverName || slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
			continue
		}
		for _, dev := range slice.Spec.Devices {
			capacity, ok := dev.Capacity[device.CPUResourceQualifiedName]
			if !ok || capacity.RequestPolicy == nil || capacity.RequestPolicy.ValidRange == nil {
				continue
			}
			valid := capacity.RequestPolicy.ValidRange
			if valid.Step != nil && valid.Step.Value() > 1 {
				return int(valid.Step.Value()), nil
			}
			if valid.Min != nil && valid.Min.Value() > 1 {
				return int(valid.Min.Value()), nil
			}
		}
	}
	return 1, nil
}
