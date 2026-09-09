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
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/cpuset"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
)

func TestClaimConfigMapReaderAllocatedClaims(t *testing.T) {
	nodeName := "test-node"
	driverName := "dra.cpu"
	namespace := "default"
	cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)

	projected := v1alpha1.ProjectedClaims{
		APIVersion: v1alpha1.APIVersion,
		Generation: 1,
		Claims: []v1alpha1.ProjectedClaim{
			{
				UID:       "claim-1",
				Namespace: "default",
				Name:      "pod-claim-1",
				State:     v1alpha1.ClaimStateAllocated,
				Devices: []v1alpha1.ProjectedDevice{
					{
						Request: "req-1",
						Pool:    nodeName,
						Device:  "cache-0",
					},
				},
			},
			{
				UID:       "claim-2",
				Namespace: "default",
				Name:      "pod-claim-2",
				State:     v1alpha1.ClaimStateDeallocated,
				Devices: []v1alpha1.ProjectedDevice{
					{
						Request: "req-2",
						Pool:    nodeName,
						Device:  "cache-1",
					},
				},
			},
		},
	}
	data, err := json.Marshal(projected)
	require.NoError(t, err)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: namespace,
		},
		Data: map[string]string{
			v1alpha1.ProjectedClaimsKey: string(data),
		},
	}

	client := k8sfake.NewSimpleClientset(cm)
	d := &CPUDriver{
		nodeName:   nodeName,
		driverName: driverName,
		namespace:  namespace,
		kubeClient: client,
	}

	reader, err := watchAllocatedClaims(context.Background(), d)
	require.NoError(t, err)
	require.NotNil(t, reader)

	claims, err := reader.AllocatedClaims()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, types.UID("claim-1"), claims[0].UID)
	require.Equal(t, "default", claims[0].Namespace)
	require.Equal(t, "pod-claim-1", claims[0].Name)
	require.Len(t, claims[0].Status.Allocation.Devices.Results, 1)
	require.Equal(t, driverName, claims[0].Status.Allocation.Devices.Results[0].Driver)
	require.Equal(t, "cache-0", claims[0].Status.Allocation.Devices.Results[0].Device)

	require.False(t, reader.IsProjectedDeallocated("claim-1"))
	require.True(t, reader.IsProjectedDeallocated("claim-2"))
	require.False(t, reader.IsProjectedDeallocated("claim-unknown"))
}

func TestClaimConfigMapReaderMissingOrCorrupt(t *testing.T) {
	nodeName := "test-node"
	driverName := "dra.cpu"
	namespace := "default"
	cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)

	client := k8sfake.NewSimpleClientset()
	d := &CPUDriver{
		nodeName:   nodeName,
		driverName: driverName,
		namespace:  namespace,
		kubeClient: client,
	}

	reader, err := watchAllocatedClaims(context.Background(), d)
	require.NoError(t, err)

	claims, err := reader.AllocatedClaims()
	require.NoError(t, err)
	require.Empty(t, claims)
	require.False(t, reader.IsProjectedDeallocated("any-claim"))

	// Create ConfigMap with corrupt JSON
	corruptCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: namespace,
		},
		Data: map[string]string{
			v1alpha1.ProjectedClaimsKey: "{not valid json",
		},
	}
	_, err = client.CoreV1().ConfigMaps(namespace).Create(context.Background(), corruptCM, metav1.CreateOptions{})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	claims, err = reader.AllocatedClaims()
	require.NoError(t, err)
	require.Empty(t, claims)

	// Incompatible APIVersion
	wrongVer := v1alpha1.ProjectedClaims{
		APIVersion: "v2beta1",
		Generation: 1,
		Claims: []v1alpha1.ProjectedClaim{
			{UID: "c1", State: v1alpha1.ClaimStateAllocated},
		},
	}
	wrongVerBytes, _ := json.Marshal(wrongVer)
	corruptCM.Data[v1alpha1.ProjectedClaimsKey] = string(wrongVerBytes)
	_, err = client.CoreV1().ConfigMaps(namespace).Update(context.Background(), corruptCM, metav1.UpdateOptions{})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	claims, err = reader.AllocatedClaims()
	require.NoError(t, err)
	require.Empty(t, claims)
}

func TestClaimConfigMapReaderStaleGeneration(t *testing.T) {
	nodeName := "test-node"
	driverName := "dra.cpu"
	namespace := "default"
	cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)

	projGen5 := v1alpha1.ProjectedClaims{
		APIVersion: v1alpha1.APIVersion,
		Generation: 5,
		Claims: []v1alpha1.ProjectedClaim{
			{UID: "claim-gen5", State: v1alpha1.ClaimStateAllocated},
		},
	}
	dataGen5, _ := json.Marshal(projGen5)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: namespace,
		},
		Data: map[string]string{
			v1alpha1.ProjectedClaimsKey: string(dataGen5),
		},
	}

	client := k8sfake.NewSimpleClientset(cm)
	d := &CPUDriver{
		nodeName:   nodeName,
		driverName: driverName,
		namespace:  namespace,
		kubeClient: client,
	}

	reader, err := watchAllocatedClaims(context.Background(), d)
	require.NoError(t, err)

	claims, err := reader.AllocatedClaims()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, types.UID("claim-gen5"), claims[0].UID)

	// Now update with older generation 3
	projGen3 := v1alpha1.ProjectedClaims{
		APIVersion: v1alpha1.APIVersion,
		Generation: 3,
		Claims: []v1alpha1.ProjectedClaim{
			{UID: "claim-gen3", State: v1alpha1.ClaimStateAllocated},
		},
	}
	dataGen3, _ := json.Marshal(projGen3)
	cm.Data[v1alpha1.ProjectedClaimsKey] = string(dataGen3)
	_, err = client.CoreV1().ConfigMaps(namespace).Update(context.Background(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)
	claims, err = reader.AllocatedClaims()
	require.NoError(t, err)
	require.Empty(t, claims, "older generation must be treated as stale / no in-flight knowledge")
}

func TestDepartureTermRetractsOnProjectedDeallocation(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	cache0 := "cache-0"
	cache1 := "cache-1"
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{
		cache0: cpuset.New(0, 1, 2, 3),
		cache1: cpuset.New(4, 5, 6, 7),
	}

	logger := testr.New(t)
	claimUID := types.UID("claim-moved")
	// Claim was recorded on cache0 for 4 CPUs, but now holds cpus on cache1 (moved)
	err := d.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{
				Request: "main",
				Role:    store.RoleExclusive,
				CPUs:    cpuset.New(4, 5, 6, 7),
			},
		},
		Relocatable: true,
		Recorded: map[string]int{
			cache0: 4,
		},
	}, false)
	require.NoError(t, err)

	fakeReader := fakeClaimReader{
		deallocated: map[types.UID]bool{},
	}
	d.claimReader = fakeReader

	d.applyMu.Lock()
	mirror := d.capacityMirror()
	d.applyMu.Unlock()

	require.Equal(t, 4, mirror[cache0].departed, "departure must be credited when not projected deallocated")
	require.Equal(t, 4, mirror[cache1].squatters, "squatters must be credited on destination")

	// Mark claim as projected deallocated
	fakeReader.deallocated[claimUID] = true
	d.claimReader = fakeReader

	d.applyMu.Lock()
	mirrorDeallocated := d.capacityMirror()
	d.applyMu.Unlock()

	require.Equal(t, 0, mirrorDeallocated[cache0].departed, "departure must retract on projected deallocation")
	require.Equal(t, 4, mirrorDeallocated[cache1].squatters, "squatters must remain until Unprepare")

	// Unprepare releases allocation from store
	d.cpuAllocationStore.RemoveResourceClaimAllocation(logger, claimUID)

	d.applyMu.Lock()
	mirrorUnprepared := d.capacityMirror()
	d.applyMu.Unlock()

	require.Equal(t, 0, mirrorUnprepared[cache0].departed)
	require.Equal(t, 0, mirrorUnprepared[cache1].squatters, "squatters must retract at Unprepare")
}
