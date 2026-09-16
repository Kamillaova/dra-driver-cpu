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
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/cpuset"
)

// storedSliceFunc answers as the API server, however a test wants it to.
type storedSliceFunc func() ([]*resourceapi.ResourceSlice, error)

func (f storedSliceFunc) StoredSlices() ([]*resourceapi.ResourceSlice, error) { return f() }

// storedFrom is what the API server would hold if it had stored what the driver
// last published, whole and at once.
func storedFrom(plugin *mockKubeletPlugin) []*resourceapi.ResourceSlice {
	if plugin.publishedResources == nil {
		return nil
	}
	nodeName := testNodeName
	pool := plugin.publishedResources.Pools[nodeName]
	stored := make([]*resourceapi.ResourceSlice, 0, len(pool.Slices))
	for i, slice := range pool.Slices {
		stored = append(stored, &resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-%d", nodeName, i)},
			Spec: resourceapi.ResourceSliceSpec{
				Driver:   testDriverName,
				NodeName: &nodeName,
				Pool: resourceapi.ResourcePool{
					Name:               nodeName,
					Generation:         1,
					ResourceSliceCount: int64(len(pool.Slices)),
				},
				Devices: slice.Devices,
			},
		})
	}
	return stored
}

// newShrinkDriver is a cache-grouped driver that moves claims, with a short
// patience for a capacity that is not stored.
func newShrinkDriver(t *testing.T, wholeCores bool) (*CPUDriver, *mockKubeletPlugin) {
	t.Helper()
	cp, plugin, _ := newMirrorDriver(t, devattr.GROUP_BY_UNCORE_CACHE, wholeCores)
	cp.defrag = defragOptions{
		enabled:               true,
		allowTransientOverlap: true,
		batchTimeout:          time.Second,
		publishTimeout:        50 * time.Millisecond,
	}
	cp.poisonedNodes = map[int]*poisonedNode{}
	cp.pendingRounds = map[defragScope]*defragRound{}
	cp.defragRetries = workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[defragScope]())
	t.Cleanup(cp.defragRetries.ShutDown)
	cp.containerUpdater = &fakeContainerUpdater{}
	cp.storedSlices = storedSliceFunc(func() ([]*resourceapi.ResourceSlice, error) { return storedFrom(plugin), nil })
	return cp, plugin
}

// beginTestMove reserves a move of claimUID onto target and returns the round
// that would carry it, as beginDefragRound does before a batch goes out.
func beginTestMove(t *testing.T, cp *CPUDriver, claimUID types.UID, charged string, from, to cpuset.CPUSet) *defragRound {
	t.Helper()
	placeCharged(t, cp, claimUID, charged, from)
	require.NoError(t, cp.cpuAllocationStore.BeginRebind(testr.New(t), claimUID, to))
	round := newDefragRound(defaultScope(0), cp.cpuAllocationStore)
	round.moves = []defrag.Move{{ClaimUID: claimUID, From: from, To: to}}
	return round
}

// TestAwaitStoredShrinkReturnsOnceTheCapacityIsStored: the move may go ahead
// only when a scheduler can see that the CPUs it is about to take are gone from
// the destination's capacity.
func TestAwaitStoredShrinkReturnsOnceTheCapacityIsStored(t *testing.T) {
	cp, plugin := newShrinkDriver(t, false)
	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), cpuset.New(2, 10))

	require.NoError(t, cp.awaitStoredShrink(context.Background(), round))
	require.Equal(t, int64(2), publishedCapacity(t, plugin)[cacheDevice(1)],
		"the destination was published short of the CPUs the move takes")
}

// TestAwaitStoredShrinkGivesUpWhenTheCapacityIsNotStored: publication is
// asynchronous and nothing reports when a write landed, so a controller that has
// not written yet is indistinguishable from one that never will. Either way the
// claim stays where it is.
func TestAwaitStoredShrinkGivesUpWhenTheCapacityIsNotStored(t *testing.T) {
	cp, plugin := newShrinkDriver(t, false)
	cp.PublishResources(context.Background())
	frozen := storedFrom(plugin)
	cp.storedSlices = storedSliceFunc(func() ([]*resourceapi.ResourceSlice, error) { return frozen, nil })

	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), cpuset.New(2, 10))
	err := cp.awaitStoredShrink(context.Background(), round)
	require.ErrorContains(t, err, "was not stored")
}

// TestAwaitStoredShrinkWaitsForAWholePool: a pool missing a slice is not
// allocated from at all, so a capacity published across several slices says
// nothing until the last of them is stored -- and a driver that took the first
// as an answer would move a claim on a shrink no consumer can see.
func TestAwaitStoredShrinkWaitsForAWholePool(t *testing.T) {
	cp, plugin := newShrinkDriver(t, false)
	cp.storedSlices = storedSliceFunc(func() ([]*resourceapi.ResourceSlice, error) {
		stored := storedFrom(plugin)
		for _, slice := range stored {
			slice.Spec.Pool.ResourceSliceCount++
		}
		return stored, nil
	})

	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), cpuset.New(2, 10))
	require.ErrorContains(t, cp.awaitStoredShrink(context.Background(), round), "was not stored")
}

// TestAwaitStoredShrinkRefusesADroppedTaint: the publishing controller adopts
// fields the API server dropped and carries on, so a device whose capacity is
// right and whose taint is gone reads as stored while it over-advertises exactly
// the CPUs the taint was there to withdraw. The number alone is not the answer.
func TestAwaitStoredShrinkRefusesADroppedTaint(t *testing.T) {
	cp, plugin := newShrinkDriver(t, true)
	cp.storedSlices = storedSliceFunc(func() ([]*resourceapi.ResourceSlice, error) {
		stored := storedFrom(plugin)
		for _, slice := range stored {
			devices := slices.Clone(slice.Spec.Devices)
			for i := range devices {
				devices[i].Taints = nil
			}
			slice.Spec.Devices = devices
		}
		return stored, nil
	})

	// Cache 1 ends up holding three CPUs it was never charged for, so its
	// computed capacity is below the floor its request policy sets and it is
	// published at the floor with a taint.
	placeCharged(t, cp, "claim-squatter", cacheDevice(0), cpuset.New(3))
	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), cpuset.New(2, 10))

	require.ErrorContains(t, cp.awaitStoredShrink(context.Background(), round), "was not stored")
	require.Equal(t, []string{devattr.FloorTaintKey}, publishedTaintKeys(t, plugin, cacheDevice(1)),
		"the driver did publish the taint; the point is that it will not proceed while the stored device lacks it")
}

// TestAbandonDefragRoundReleasesTheHoldsOfAFreshRound: nothing has been asked of
// the runtime, so the claim is where it was and the reservation is the only
// thing to undo. Leaving it would hold the destination's CPUs against every
// other claim until something else released them.
func TestAbandonDefragRoundReleasesTheHoldsOfAFreshRound(t *testing.T) {
	cp, _ := newShrinkDriver(t, false)
	origin, target := cpuset.New(0, 8), cpuset.New(2, 10)
	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), origin, target)

	result := cp.abandonDefragRound(context.Background(), testr.New(t), round, fmt.Errorf("not stored"))
	require.Equal(t, cpumetrics.ResultError, result)

	_, inFlight := cp.cpuAllocationStore.GetRebindOrigin("claim-a")
	require.False(t, inFlight, "the reservation is released")
	got, ok := cp.cpuAllocationStore.GetResourceClaimAllocation("claim-a")
	require.True(t, ok)
	require.Equal(t, origin, got, "the claim is back on the CPUs it never left")
	require.NotContains(t, cp.pendingRounds, round.scope)
}

// TestAbandonDefragRoundKeepsTheHoldsOfAReplayedRound: a round an earlier
// attempt left unsettled may already be applied. Releasing its holds would offer
// a partner's running CPUs to the next claim, which is the breach the transit set
// exists to prevent.
func TestAbandonDefragRoundKeepsTheHoldsOfAReplayedRound(t *testing.T) {
	cp, _ := newShrinkDriver(t, false)
	target := cpuset.New(2, 10)
	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), target)
	round.replayed = true
	cp.pendingRounds[round.scope] = round

	cp.abandonDefragRound(context.Background(), testr.New(t), round, fmt.Errorf("not stored"))

	_, inFlight := cp.cpuAllocationStore.GetRebindOrigin("claim-a")
	require.True(t, inFlight, "an unsettled round keeps both halves")
	require.Contains(t, cp.pendingRounds, round.scope, "and stays pending until a read-back settles it")
}

// TestAwaitStoredShrinkIsSkippedWithNothingPublished: a driver with no plugin to
// publish through and no way to read a slice back has no inventory for a shrink
// to be missing from, and must not refuse to move claims over it.
func TestAwaitStoredShrinkIsSkippedWithNothingPublished(t *testing.T) {
	cp, _ := newShrinkDriver(t, false)
	round := beginTestMove(t, cp, "claim-a", cacheDevice(0), cpuset.New(0, 8), cpuset.New(2, 10))

	cp.storedSlices = nil
	require.NoError(t, cp.awaitStoredShrink(context.Background(), round))

	cp.draPlugin = nil
	require.NoError(t, cp.awaitStoredShrink(context.Background(), round))
}
