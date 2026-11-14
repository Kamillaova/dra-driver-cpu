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

package claim_test

import (
	"testing"

	e2eclaim "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/claim"
	"github.com/stretchr/testify/require"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestWithSelector(t *testing.T) {
	spec := func() resourcev1.ResourceClaimSpec {
		return resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
			{Name: "exact", Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: "dra.cpu"}},
			{Name: "alternatives", FirstAvailable: []resourcev1.DeviceSubRequest{{Name: "first", DeviceClassName: "dra.cpu"}}},
		}}}
	}

	for _, tc := range []struct {
		name string
		cel  string
		want []resourcev1.DeviceSelector
	}{
		{name: "no expression", cel: ""},
		{
			name: "an expression",
			cel:  `device.attributes["dra.cpu"].threadsPerCore == 2`,
			want: []resourcev1.DeviceSelector{{CEL: &resourcev1.CELDeviceSelector{Expression: `device.attributes["dra.cpu"].threadsPerCore == 2`}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := spec()
			got := e2eclaim.WithSelector(in, tc.cel)
			require.Equal(t, tc.want, got.Devices.Requests[0].Exactly.Selectors)
			require.Empty(t, got.Devices.Requests[1].FirstAvailable[0].Selectors, "a request with alternatives is left as it is")
			require.Equal(t, spec(), in, "the spec given is not changed")
		})
	}
}

func TestNUMANodeSelector(t *testing.T) {
	for _, tc := range []struct {
		name    string
		groupBy string
		want    string
	}{
		{name: "numanode", groupBy: "numanode", want: `device.attributes["resource.kubernetes.io"].numaNode == 1`},
		{name: "uncorecache", groupBy: "uncorecache", want: `device.attributes["resource.kubernetes.io"].numaNode == 1`},
		{name: "socket", groupBy: "socket"},
		{name: "machine", groupBy: "machine"},
		{name: "unset", groupBy: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, e2eclaim.NUMANodeSelector(tc.groupBy, 1))
		})
	}
}

func TestAllocationStep(t *testing.T) {
	policy := func(minimum, step int64) *resourcev1.CapacityRequestPolicy {
		valid := &resourcev1.CapacityRequestPolicyRange{Min: resource.NewQuantity(minimum, resource.DecimalSI)}
		if step > 0 {
			valid.Step = resource.NewQuantity(step, resource.DecimalSI)
		}
		return &resourcev1.CapacityRequestPolicy{ValidRange: valid}
	}
	slice := func(name, driver, node string, policies ...*resourcev1.CapacityRequestPolicy) runtime.Object {
		var devices []resourcev1.Device
		for _, p := range policies {
			devices = append(devices, resourcev1.Device{
				Name: "dev",
				Capacity: map[resourcev1.QualifiedName]resourcev1.DeviceCapacity{
					"dra.cpu/cpu": {Value: resource.MustParse("8"), RequestPolicy: p},
				},
			})
		}
		return &resourcev1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       resourcev1.ResourceSliceSpec{Driver: driver, NodeName: new(node), Devices: devices},
		}
	}

	for _, tc := range []struct {
		name   string
		slices []runtime.Object
		want   int
	}{
		{name: "a step", slices: []runtime.Object{slice("s", "dra.cpu", "node-a", policy(2, 2))}, want: 2},
		{name: "a minimum without a step", slices: []runtime.Object{slice("s", "dra.cpu", "node-a", policy(2, 0))}, want: 2},
		{name: "no policy", slices: []runtime.Object{slice("s", "dra.cpu", "node-a", nil)}, want: 1},
		{name: "a policy of one CPU", slices: []runtime.Object{slice("s", "dra.cpu", "node-a", policy(1, 1))}, want: 1},
		{
			name: "another node's and another driver's policies",
			slices: []runtime.Object{
				slice("other-node", "dra.cpu", "node-b", policy(2, 2)),
				slice("other-driver", "gpu.example.com", "node-a", policy(4, 4)),
				slice("s", "dra.cpu", "node-a", nil),
			},
			want: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e2eclaim.AllocationStep(t.Context(), fake.NewClientset(tc.slices...), "dra.cpu", "node-a")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
