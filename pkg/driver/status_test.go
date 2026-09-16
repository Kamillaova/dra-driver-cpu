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

	require.NoError(t, d.doPublishClaimPlacementStatus(context.Background(), logr.Discard(), claimUID,
		types.NamespacedName{Namespace: "default", Name: "claim"}))

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

// publisherFixture is a driver whose store holds one claim on CPUs 0-1 of one
// device, and a fake API server holding that claim.
func publisherFixture(t *testing.T, reader claimReader) (*CPUDriver, *k8sfake.Clientset, types.UID) {
	t.Helper()

	const (
		driverName = "dra.cpu"
		deviceName = "cpudevnuma000"
		poolName   = "test-node"
	)

	share := types.UID("e8847903-ac7e-4811-b0d4-4c711aac6864")
	claimUID := types.UID("claim-unprojected")

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "volta", UID: claimUID},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{{
						Request: "cpus", Driver: driverName, Pool: poolName, Device: deviceName, ShareID: &share,
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
		claimReader:        reader,
		cpuAllocationStore: allocation,
	}
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{deviceName: cpuset.New(0, 1)}

	return d, client, claimUID
}

// TestPublishClaimPlacementStatusDoesNotNeedTheProjection pins B100. At Prepare
// the kubelet hands over a claim the scheduler allocated in the same instant,
// and the projector has not necessarily written it for this node yet. Resolving
// the claim's name through that projection lost the race and the placement was
// never published for the life of the pod, because nothing retries.
func TestPublishClaimPlacementStatusDoesNotNeedTheProjection(t *testing.T) {
	d, client, claimUID := publisherFixture(t, staticClaimReader{})

	require.NoError(t, d.doPublishClaimPlacementStatus(context.Background(), logr.Discard(), claimUID,
		types.NamespacedName{Namespace: "volta", Name: "claim"}))

	published, err := client.ResourceV1().ResourceClaims("volta").Get(context.Background(), "claim", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, published.Status.Devices, 1, "a caller holding the claim's name must not depend on the projection")

	var placement v1alpha1.ClaimPlacementStatus
	require.NoError(t, json.Unmarshal(published.Status.Devices[0].Data.Raw, &placement))
	require.Equal(t, "0-1", placement.CPUSet)
}

// TestPublishClaimPlacementStatusFallsBackToTheProjection: a caller with nothing
// but a UID -- the defragmenter -- still resolves through the projection, and
// still fails where the projection does not know the claim.
func TestPublishClaimPlacementStatusFallsBackToTheProjection(t *testing.T) {
	d, _, claimUID := publisherFixture(t, staticClaimReader{})

	err := d.doPublishClaimPlacementStatus(context.Background(), logr.Discard(), claimUID, types.NamespacedName{})
	require.ErrorContains(t, err, "not found in cache")
}

// TestPublishClaimPlacementStatusNarrowsCorrelationPerDevice pins B98. A claim
// holding devices in two partitions records one correlation, taken from the
// first device; publishing it whole told a reader that the ctrl pool device sat
// in the spdk partition and had started on the spdk cores. Since each entry
// names a device, each entry must describe that device.
func TestPublishClaimPlacementStatusNarrowsCorrelationPerDevice(t *testing.T) {
	const (
		driverName = "dra.cpu"
		poolName   = "c1-4"
		spdkDevice = "cpudevcache000-spdk"
		ctrlDevice = "cpudevpool000-ctrl"
	)

	spdkShare := types.UID("11111111-1111-1111-1111-111111111111")
	ctrlShare := types.UID("22222222-2222-2222-2222-222222222222")
	claimUID := types.UID("claim-two-partitions")

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "volta", UID: claimUID},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{Request: "poll", Driver: driverName, Pool: poolName, Device: spdkDevice, ShareID: &spdkShare},
						{Request: "control", Driver: driverName, Pool: poolName, Device: ctrlDevice, ShareID: &ctrlShare},
					},
				},
			},
		},
	}

	topo := &cpuinfo.CPUTopology{CPUDetails: cpuinfo.CPUDetails{
		2: {CpuID: 2, CoreID: 2, SocketID: 0, UncoreCacheID: 0},
		3: {CpuID: 3, CoreID: 3, SocketID: 0, UncoreCacheID: 1},
		4: {CpuID: 4, CoreID: 4, SocketID: 0, UncoreCacheID: 2},
		5: {CpuID: 5, CoreID: 5, SocketID: 0, UncoreCacheID: 2},
	}}

	allocation := store.NewCPUAllocation(topo, cpuset.New())
	require.NoError(t, allocation.ReserveResourceClaimAllocation(logr.Discard(), claimUID, store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "poll", CPUs: cpuset.New(4, 5), Role: store.RoleExclusive},
			{Request: "control", CPUs: cpuset.New(2, 3), Role: store.RoleShared},
		},
		// One correlation for the whole claim, taken from the first device: the
		// shape every prepared claim has.
		Correlation: store.ClaimCorrelation{
			Partition:        "spdk",
			InitialCPUSet:    "2-5",
			FrontierSnapshot: "3,2,1,0",
			RuntimeOutcome:   "aligned",
		},
	}, false))

	client := k8sfake.NewSimpleClientset(claim)
	d := &CPUDriver{
		nodeName:           poolName,
		driverName:         driverName,
		kubeClient:         client,
		claimReader:        staticClaimReader{claims: []*resourceapi.ResourceClaim{claim}},
		cpuAllocationStore: allocation,
	}
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{
		spdkDevice: cpuset.New(4, 5),
		ctrlDevice: cpuset.New(2, 3),
	}
	d.topology.deviceNameToPartition = map[string]string{spdkDevice: "spdk", ctrlDevice: "ctrl"}
	d.topology.deviceNameToNUMANodeID = map[string]int{spdkDevice: 0, ctrlDevice: 0}
	d.topology.cpuTopology = topo

	require.NoError(t, d.doPublishClaimPlacementStatus(context.Background(), logr.Discard(), claimUID,
		types.NamespacedName{Namespace: "volta", Name: "claim"}))

	published, err := client.ResourceV1().ResourceClaims("volta").Get(context.Background(), "claim", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, published.Status.Devices, 2)

	byDevice := map[string]v1alpha1.ClaimPlacementStatus{}
	for _, device := range published.Status.Devices {
		var placement v1alpha1.ClaimPlacementStatus
		require.NoError(t, json.Unmarshal(device.Data.Raw, &placement))
		byDevice[device.Device] = placement
	}

	spdk := byDevice[spdkDevice]
	require.Equal(t, "spdk", spdk.Partition)
	require.Equal(t, "4-5", spdk.CPUSet)
	require.Equal(t, "4-5", spdk.InitialCPUSet)
	require.Equal(t, "3,2,1,0", spdk.FrontierSnapshot, "the frontier was taken for this device's partition")
	require.Equal(t, "aligned", spdk.RuntimeOutcome)
	require.Equal(t, []int{2}, spdk.UncoreCaches)

	ctrl := byDevice[ctrlDevice]
	require.Equal(t, "ctrl", ctrl.Partition, "the ctrl device must not report the claim's first partition")
	require.Equal(t, "2-3", ctrl.CPUSet)
	require.Equal(t, "2-3", ctrl.InitialCPUSet, "the initial cpuset is this device's share of the claim's")
	require.Empty(t, ctrl.FrontierSnapshot, "a frontier taken for another partition is not an answer for this one")
	require.Empty(t, ctrl.RuntimeOutcome)
	require.Equal(t, []int{0, 1}, ctrl.UncoreCaches, "a pool device names every cache its own CPUs sit in")
}

func TestUncoreCachesOf(t *testing.T) {
	d := &CPUDriver{}
	require.Nil(t, d.uncoreCachesOf(cpuset.New(0, 1)), "no topology, no caches")

	d.topology.cpuTopology = &cpuinfo.CPUTopology{CPUDetails: cpuinfo.CPUDetails{
		0: {CpuID: 0, UncoreCacheID: 3},
		1: {CpuID: 1, UncoreCacheID: 1},
		2: {CpuID: 2, UncoreCacheID: 3},
		3: {CpuID: 3, UncoreCacheID: -1},
	}}
	require.Equal(t, []int{1, 3}, d.uncoreCachesOf(cpuset.New(0, 1, 2, 3)))
	require.Empty(t, d.uncoreCachesOf(cpuset.New(3)), "a CPU with no known cache names none")
}
