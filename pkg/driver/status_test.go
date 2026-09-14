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
	"testing"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/cpuset"
)

// staticClaimReader answers with the claims it was built from.
type staticClaimReader struct {
	claims []*resourceapi.ResourceClaim
}

func (r staticClaimReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	return r.claims, nil
}

func (r staticClaimReader) IsProjectedDeallocated(types.UID) bool { return false }

// GetProjectedClaims is not part of the interface at this commit and is not
// used by the publisher. It is here so that the stub keeps satisfying the
// interface where a later commit widens it, rather than breaking a commit that
// has nothing to do with this test.
func (r staticClaimReader) GetProjectedClaims() (*v1alpha1.ProjectedClaims, error) {
	return &v1alpha1.ProjectedClaims{}, nil
}

// TestPublishClaimPlacementStatusCarriesShareID pins the identity rule the
// apiserver enforces and a fake client does not: a device status names an
// allocated device only when its driver, pool, device and share all match the
// allocation. A status built without the share is refused whole, which is how
// the placement went unpublished on a real cluster while every unit test
// passed.
func TestPublishClaimPlacementStatusCarriesShareID(t *testing.T) {
	const (
		driverName = "dra.cpu"
		deviceName = "cpudevnuma000"
		poolName   = "test-node"
	)

	share := types.UID("e8847903-ac7e-4811-b0d4-4c711aac6864")
	claimUID := types.UID("claim-1")

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "default", UID: claimUID},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{{
						Request: "cpus",
						Driver:  driverName,
						Pool:    poolName,
						Device:  deviceName,
						ShareID: &share,
					}},
				},
			},
		},
	}

	topo := &cpuinfo.CPUTopology{CPUDetails: cpuinfo.CPUDetails{
		0: {CpuID: 0, CoreID: 0, SocketID: 0},
		1: {CpuID: 1, CoreID: 1, SocketID: 0},
	}}

	allocation := store.NewCPUAllocation(topo, cpuset.New())
	require.NoError(t, allocation.ReserveResourceClaimAllocation(logr.Discard(), claimUID, store.ClaimRecord{
		Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive}},
	}, false))

	client := k8sfake.NewSimpleClientset(claim)
	d := &CPUDriver{
		nodeName:           poolName,
		driverName:         driverName,
		kubeClient:         client,
		claimReader:        staticClaimReader{claims: []*resourceapi.ResourceClaim{claim}},
		cpuAllocationStore: allocation,
	}
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{deviceName: cpuset.New(0, 1)}

	require.NoError(t, d.doPublishClaimPlacementStatus(context.Background(), logr.Discard(), claimUID))

	published, err := client.ResourceV1().ResourceClaims("default").Get(context.Background(), "claim", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, published.Status.Devices, 1)

	device := published.Status.Devices[0]
	require.Equal(t, driverName, device.Driver)
	require.Equal(t, poolName, device.Pool)
	require.Equal(t, deviceName, device.Device)
	require.NotNil(t, device.ShareID, "a status without the share does not name an allocated device")
	require.Equal(t, string(share), *device.ShareID)

	var placement v1alpha1.ClaimPlacementStatus
	require.NoError(t, json.Unmarshal(device.Data.Raw, &placement))
	require.Equal(t, "0-1", placement.CPUSet)
	require.Equal(t, 2, placement.CPUCount)
}

// TestShareIDOfUnshared keeps a device that carries no share from gaining one:
// the field is optional, and an empty string is not the same as absent.
func TestShareIDOfUnshared(t *testing.T) {
	require.Nil(t, shareIDOf(resourceapi.DeviceRequestAllocationResult{Driver: "dra.cpu"}))
}
