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

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
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
		metrics:    cpumetrics.Noop(),
	}

	require.NoError(t, watchAllocatedClaims(context.Background(), d))
	reader := d.claimReader
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

	require.False(t, reader.IsProjectedDeallocated("claim-1", nil))
	require.True(t, reader.IsProjectedDeallocated("claim-2", nil))
	require.False(t, reader.IsProjectedDeallocated("claim-unknown", nil))
}

// TestClaimConfigMapReaderReadsAbsenceAgainstTheWatermark: the projector marks
// a claim deallocated for one write only, so the reading that has to last is
// absence -- but only absence from a projection newer than the one the claim was
// first seen in, and only from the same projector.
func TestClaimConfigMapReaderReadsAbsenceAgainstTheWatermark(t *testing.T) {
	nodeName := "test-node"
	namespace := "default"
	cmName := v1alpha1.ProjectedClaimsConfigMapName(nodeName)

	// Generation 7 lists nothing: the claim below was prepared against an
	// earlier one and has since gone.
	data, err := json.Marshal(v1alpha1.ProjectedClaims{APIVersion: v1alpha1.APIVersion, Generation: 7})
	require.NoError(t, err)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: namespace,
			UID:       "projection-a",
		},
		Data: map[string]string{v1alpha1.ProjectedClaimsKey: string(data)},
	}

	d := &CPUDriver{
		nodeName:   nodeName,
		driverName: "dra.cpu",
		namespace:  namespace,
		kubeClient: k8sfake.NewSimpleClientset(cm),
		metrics:    cpumetrics.Noop(),
	}
	require.NoError(t, watchAllocatedClaims(context.Background(), d))
	reader := d.claimReader

	for _, tc := range []struct {
		name string
		mark *store.ProjectionWatermark
		want bool
	}{
		{"no mark at all", nil, false},
		{"first seen in an older projection", &store.ProjectionWatermark{Lineage: "projection-a", Generation: 6}, true},
		{"first seen in this very projection", &store.ProjectionWatermark{Lineage: "projection-a", Generation: 7}, false},
		{"first seen in a newer projection", &store.ProjectionWatermark{Lineage: "projection-a", Generation: 8}, false},
		{"first seen in another projector's", &store.ProjectionWatermark{Lineage: "projection-b", Generation: 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, reader.IsProjectedDeallocated("claim-gone", tc.mark))
		})
	}

	_, ok := reader.ProjectedAllocatedAt("claim-gone")
	require.False(t, ok, "a claim the projection does not list is not marked")
}

// TestClaimConfigMapReaderMarksWhereAClaimWasFirstSeen: the mark is the lineage
// and generation of the projection listing the claim as allocated.
func TestClaimConfigMapReaderMarksWhereAClaimWasFirstSeen(t *testing.T) {
	nodeName := "test-node"
	namespace := "default"

	projected := v1alpha1.ProjectedClaims{
		APIVersion: v1alpha1.APIVersion,
		Generation: 12,
		Claims: []v1alpha1.ProjectedClaim{
			{UID: "claim-live", Namespace: namespace, Name: "c", State: v1alpha1.ClaimStateAllocated},
			{UID: "claim-gone", Namespace: namespace, Name: "d", State: v1alpha1.ClaimStateDeallocated},
		},
	}
	data, err := json.Marshal(projected)
	require.NoError(t, err)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v1alpha1.ProjectedClaimsConfigMapName(nodeName),
			Namespace: namespace,
			UID:       "projection-a",
		},
		Data: map[string]string{v1alpha1.ProjectedClaimsKey: string(data)},
	}

	d := &CPUDriver{
		nodeName:   nodeName,
		driverName: "dra.cpu",
		namespace:  namespace,
		kubeClient: k8sfake.NewSimpleClientset(cm),
		metrics:    cpumetrics.Noop(),
	}
	require.NoError(t, watchAllocatedClaims(context.Background(), d))

	mark, ok := d.claimReader.ProjectedAllocatedAt("claim-live")
	require.True(t, ok)
	require.Equal(t, store.ProjectionWatermark{Lineage: "projection-a", Generation: 12}, mark)

	_, ok = d.claimReader.ProjectedAllocatedAt("claim-gone")
	require.False(t, ok, "a claim listed as deallocated is not one that was seen allocated")
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
		metrics:    cpumetrics.Noop(),
	}

	require.NoError(t, watchAllocatedClaims(context.Background(), d))
	reader := d.claimReader

	claims, err := reader.AllocatedClaims()
	require.NoError(t, err)
	require.Empty(t, claims)
	require.False(t, reader.IsProjectedDeallocated("any-claim", nil))

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
	require.Error(t, err, "a projection that will not parse was written by something, and reading the node as empty would hand out CPUs a claim already holds")
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

// TestClaimConfigMapReaderAcceptsARestartedProjector: a generation lower than
// one already seen is not a stale read -- an informer's cache never moves
// backwards -- but a projector that restarted. Refusing it would leave the
// driver on a projection nothing can supersede for the life of the node.
func TestClaimConfigMapReaderAcceptsARestartedProjector(t *testing.T) {
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
		metrics:    cpumetrics.Noop(),
	}

	require.NoError(t, watchAllocatedClaims(context.Background(), d))
	reader := d.claimReader

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
	require.Len(t, claims, 1, "a projector that restarted counts from its own beginning, and its projection is the current one")
	require.Equal(t, types.UID("claim-gen3"), claims[0].UID)
}

// fakeClaimReader stands in for the projection: the claims the scheduler has
// allocated to this node, and the ones it has taken back.
type fakeClaimReader struct {
	claims      []*resourceapi.ResourceClaim
	deallocated map[types.UID]bool
	allocatedAt map[types.UID]store.ProjectionWatermark
	// asked records the mark each claim was judged against, for a test whose
	// point is that the record's own mark is what reaches the reader.
	asked     map[types.UID]*store.ProjectionWatermark
	projected *v1alpha1.ProjectedClaims
}

func (f fakeClaimReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	return f.claims, nil
}

func (f fakeClaimReader) IsProjectedDeallocated(claimUID types.UID, mark *store.ProjectionWatermark) bool {
	if f.asked != nil {
		f.asked[claimUID] = mark
	}
	return f.deallocated != nil && f.deallocated[claimUID]
}

func (f fakeClaimReader) ProjectedAllocatedAt(claimUID types.UID) (store.ProjectionWatermark, bool) {
	if f.allocatedAt == nil {
		return store.ProjectionWatermark{}, false
	}
	mark, ok := f.allocatedAt[claimUID]
	return mark, ok
}

func (f fakeClaimReader) GetProjectedClaims() (*v1alpha1.ProjectedClaims, error) {
	return f.projected, nil
}
