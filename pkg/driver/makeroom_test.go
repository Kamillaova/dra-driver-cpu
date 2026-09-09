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
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kubeletplugin "k8s.io/dynamic-resource-allocation/kubeletplugin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/cpuset"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
)

func TestMakeRoomTargetCreatedFromProjectedClaims(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{
		"cache-0": cpuset.New(0, 1, 2, 3),
		"cache-1": cpuset.New(4, 5, 6, 7),
	}
	d.topology.deviceNameToUncoreCacheID = map[string]int{
		"cache-0": 0,
		"cache-1": 1,
	}
	d.topology.deviceNameToNUMANodeID = map[string]int{
		"cache-0": 0,
		"cache-1": 0,
	}

	d.placeClaim(t, "squatter", cpuset.New(0, 1))

	fakeReader := fakeClaimReader{
		projected: &v1alpha1.ProjectedClaims{
			Claims: []v1alpha1.ProjectedClaim{
				{
					UID:       "claim-never-split",
					Namespace: "default",
					Name:      "pod-never-split",
					State:     v1alpha1.ClaimStateAllocated,
					Shape:     "never-split",
					Devices: []v1alpha1.ProjectedDevice{
						{
							Device: "cache-0",
						},
					},
				},
				{
					UID:       "claim-flexible",
					Namespace: "default",
					Name:      "pod-flexible",
					State:     v1alpha1.ClaimStateAllocated,
					Shape:     "flexible",
					Devices: []v1alpha1.ProjectedDevice{
						{
							Device: "cache-0",
						},
					},
				},
				{
					UID:       "claim-empty-cache",
					Namespace: "default",
					Name:      "pod-empty-cache",
					State:     v1alpha1.ClaimStateAllocated,
					Shape:     "never-split",
					Devices: []v1alpha1.ProjectedDevice{
						{
							Device: "cache-1",
						},
					},
				},
			},
		},
	}
	d.claimReader = fakeReader

	d.reconcileMakeRoomTargets(context.Background())

	d.applyMu.Lock()
	defer d.applyMu.Unlock()

	target, ok := d.makeRoomTargets["claim-never-split"]
	require.True(t, ok)
	require.Equal(t, 0, target.cacheID)
	require.Equal(t, 0, target.numaNodeID)
	require.Equal(t, "cache-0", target.device)

	_, hasFlexible := d.makeRoomTargets["claim-flexible"]
	require.False(t, hasFlexible)

	_, hasEmptyCache := d.makeRoomTargets["claim-empty-cache"]
	require.False(t, hasEmptyCache)
}

func TestMakeRoomTargetServedByExactSearchAndClearedOnPrepare(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "squatter", cpuset.New(0, 1))
	d.runContainer(t, "pod-1", "cont-1", "container-1", "squatter")

	logger := testr.New(t)
	scope := defaultScope(0)
	online := d.allCPUs

	d.applyMu.Lock()
	d.makeRoomTargets["target-claim"] = &makeRoomTarget{
		claimUID:   "target-claim",
		namespace:  "default",
		name:       "target-claim",
		cacheID:    0,
		numaNodeID: 0,
		partition:  "",
		device:     "cache-0",
	}
	d.applyMu.Unlock()

	round := d.beginDefragRound(context.Background(), logger, scope, online)
	require.NotNil(t, round)
	require.Len(t, round.moves, 1)
	require.Equal(t, types.UID("squatter"), round.moves[0].ClaimUID)
	require.True(t, round.moves[0].To.IsSubsetOf(cpuset.New(4, 5, 6, 7)))

	d.applyMu.Lock()
	require.True(t, d.hasActiveExactPlan(0))
	d.applyMu.Unlock()

	res := d.finishDefragRound(logger, round, nil, nil)
	require.Equal(t, cpumetrics.ResultSuccess, res)

	d.applyMu.Lock()
	require.False(t, d.hasActiveExactPlan(0))
	d.applyMu.Unlock()

	namespacedObj := kubeletplugin.NamespacedObject{
		UID: "target-claim",
	}
	err := d.unprepareResourceClaim(logger, namespacedObj)
	require.NoError(t, err)

	d.applyMu.Lock()
	_, hasTarget := d.makeRoomTargets["target-claim"]
	require.False(t, hasTarget)
	d.applyMu.Unlock()
}

func TestMakeRoomTargetDroppedOnDeallocation(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	fakeClient := k8sfake.NewSimpleClientset()
	d.kubeClient = fakeClient

	d.applyMu.Lock()
	d.makeRoomTargets["claim-1"] = &makeRoomTarget{
		claimUID:   "claim-1",
		namespace:  "default",
		name:       "pod-claim-1",
		cacheID:    0,
		numaNodeID: 0,
	}
	d.applyMu.Unlock()

	fakeReader := fakeClaimReader{
		projected: &v1alpha1.ProjectedClaims{
			Claims: []v1alpha1.ProjectedClaim{
				{
					UID:       "claim-1",
					Namespace: "default",
					Name:      "pod-claim-1",
					State:     v1alpha1.ClaimStateDeallocated,
				},
			},
		},
	}
	d.claimReader = fakeReader

	d.reconcileMakeRoomTargets(context.Background())

	d.applyMu.Lock()
	_, ok := d.makeRoomTargets["claim-1"]
	require.False(t, ok)
	d.applyMu.Unlock()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		events, err := fakeClient.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
		assert.NoError(c, err)
		if !assert.Len(c, events.Items, 1) {
			return
		}
		assert.Equal(c, "MakeRoomDropped", events.Items[0].Reason)
		assert.Equal(c, "pod-claim-1", events.Items[0].InvolvedObject.Name)
	}, time.Second, 10*time.Millisecond)
}

func TestMakeRoomTargetDroppedOnDeletion(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	fakeClient := k8sfake.NewSimpleClientset()
	d.kubeClient = fakeClient

	d.applyMu.Lock()
	d.makeRoomTargets["claim-1"] = &makeRoomTarget{
		claimUID:   "claim-1",
		namespace:  "default",
		name:       "pod-claim-1",
		cacheID:    0,
		numaNodeID: 0,
	}
	d.applyMu.Unlock()

	fakeReader := fakeClaimReader{
		projected: &v1alpha1.ProjectedClaims{
			Claims: []v1alpha1.ProjectedClaim{},
		},
	}
	d.claimReader = fakeReader

	d.reconcileMakeRoomTargets(context.Background())

	d.applyMu.Lock()
	_, ok := d.makeRoomTargets["claim-1"]
	require.False(t, ok)
	d.applyMu.Unlock()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		events, err := fakeClient.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
		assert.NoError(c, err)
		if !assert.Len(c, events.Items, 1) {
			return
		}
		assert.Equal(c, "MakeRoomDropped", events.Items[0].Reason)
		assert.Equal(c, "pod-claim-1", events.Items[0].InvolvedObject.Name)
	}, time.Second, 10*time.Millisecond)
}

func TestMakeRoomTargetDroppedOnUnreachable(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	fakeClient := k8sfake.NewSimpleClientset()
	d.kubeClient = fakeClient

	d.placeFixedClaim(t, "fixed-0", cpuset.New(0, 1, 2, 3))
	d.placeFixedClaim(t, "fixed-1", cpuset.New(4, 5, 6, 7))

	d.applyMu.Lock()
	d.makeRoomTargets["claim-unreachable"] = &makeRoomTarget{
		claimUID:   "claim-unreachable",
		namespace:  "default",
		name:       "pod-claim-unreachable",
		cacheID:    0,
		numaNodeID: 0,
		partition:  "",
		device:     "cache-0",
	}
	d.applyMu.Unlock()

	logger := testr.New(t)
	scope := defaultScope(0)
	online := d.allCPUs

	round := d.beginDefragRound(context.Background(), logger, scope, online)
	require.Nil(t, round)

	d.applyMu.Lock()
	_, ok := d.makeRoomTargets["claim-unreachable"]
	require.False(t, ok)
	d.applyMu.Unlock()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		events, err := fakeClient.CoreV1().Events("default").List(context.Background(), metav1.ListOptions{})
		assert.NoError(c, err)
		if !assert.Len(c, events.Items, 1) {
			return
		}
		assert.Equal(c, "MakeRoomUnreachable", events.Items[0].Reason)
		assert.Equal(c, "pod-claim-unreachable", events.Items[0].InvolvedObject.Name)
	}, time.Second, 10*time.Millisecond)
}
