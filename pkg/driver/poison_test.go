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

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// fencedDriver is the node an unsettleable exchange leaves behind: the runtime
// refused one container, then refused to put the other back, so nobody can say
// which CPUs the two containers are on.
func fencedDriver(t *testing.T) *defragTestDriver {
	t.Helper()
	d := exchangeDriver(t)
	d.updater.reply = func(call int, updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
		if call < 3 {
			return refusing("ctr-uid-2")(call, updates)
		}
		return refusing("ctr-uid-1")(call, updates)
	}
	d.defragPass(context.Background())
	require.True(t, d.nodeIsPoisoned(0), "an exchange nobody can settle must fence its NUMA node")
	return d
}

func TestPoisonedNodeIsFencedAndCounted(t *testing.T) {
	d := fencedDriver(t)

	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_poisoned_nodes_total", nil), 0.01)
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_numa_node_poisoned", map[string]string{"numa_node": "0"}), 0.01)

	// A fence does not stop the node being measured. A gauge that disappears
	// when a node is in trouble is the one nobody can alert on.
	require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_excess_uncore_caches", nil))
}

func TestPoisonedNodeIsNotPlannedOn(t *testing.T) {
	d := fencedDriver(t)
	sent := d.updater.allCalls()

	// The unsettled round is sent again, under the same reservation, because
	// that is the one thing that can still finish it. Nothing new is planned.
	d.updater.reply = nil
	d.defragPass(context.Background())

	calls := d.updater.allCalls()
	require.Len(t, calls, len(sent)+1)
	require.Equal(t, sent[0], calls[len(calls)-1], "a fenced node sends its unsettled round again and plans nothing new")
}

func TestPoisonedNodeTaintsItsDevices(t *testing.T) {
	d := fencedDriver(t)
	d.devicesPerResourceSlice = 64
	d.topology.devicesByPartition = [][]resourceapi.Device{{{Name: "numa-0"}, {Name: "numa-1"}}}
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{
		"numa-0": cpuset.New(0, 1, 2, 3),
		"numa-1": cpuset.New(4, 5),
	}

	d.applyMu.Lock()
	slices := d.refreshDeviceOrder()
	d.applyMu.Unlock()

	require.Len(t, slices, 1)
	require.Equal(t, []resourceapi.DeviceTaint{{
		Key:    devattr.PoisonTaintKey,
		Value:  "0",
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	}}, slices[0][0].Taints, "a device reaching into the fenced node is tainted")
	require.Empty(t, slices[0][1].Taints, "a device that does not is untouched")
}

func TestPoisonedNodeReopensWhenTheKernelAgrees(t *testing.T) {
	d := fencedDriver(t)
	// The runtime did apply the first half and refused the second, so the kernel
	// says the exchange never completed: both containers are where they started.
	d.liveCPUs("ctr-uid-1", cpuset.New(0, 3))
	d.liveCPUs("ctr-uid-2", cpuset.New(1, 2))
	d.updater.reply = nil

	d.liftPoison(testr.New(t), 0)

	require.False(t, d.nodeIsPoisoned(0))
	require.NotContains(t, d.pendingRounds, defaultScope(0))
	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 3), first, "the ledger is rebuilt from what the kernel says")
	require.Equal(t, cpuset.New(0, 3), d.recordedPlacement(t, "claim-1"))
	for _, claimUID := range []string{"claim-1", "claim-2"} {
		_, inFlight := d.cpuAllocationStore.GetRebindOrigin(types.UID(claimUID))
		require.False(t, inFlight)
	}
	require.Positive(t, metricValue(t, d.metrics, "dra_cpu_defrag_poisoned_duration_seconds", nil))
}

func TestPoisonedNodeCommitsAnExchangeTheKernelSaysCompleted(t *testing.T) {
	d := fencedDriver(t)
	// The other coherent answer: both containers took their new cpusets after
	// all, and the reply that said otherwise was lost or wrong.
	d.liveCPUs("ctr-uid-1", cpuset.New(0, 1))
	d.liveCPUs("ctr-uid-2", cpuset.New(2, 3))

	d.liftPoison(testr.New(t), 0)

	require.False(t, d.nodeIsPoisoned(0))
	first, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	second, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-2")
	require.Equal(t, cpuset.New(0, 1), first)
	require.Equal(t, cpuset.New(2, 3), second)
}

func TestPoisonedNodeStaysFencedWhileTheKernelDisagrees(t *testing.T) {
	d := fencedDriver(t)
	// Half applied: exactly the state the fence exists for, and the one the
	// driver may not settle by guessing.
	d.liveCPUs("ctr-uid-1", cpuset.New(0, 1))
	d.liveCPUs("ctr-uid-2", cpuset.New(1, 2))

	d.liftPoison(testr.New(t), 0)

	require.True(t, d.nodeIsPoisoned(0))
	require.Contains(t, d.pendingRounds, defaultScope(0))
	require.InDelta(t, 1, metricValue(t, d.metrics, "dra_cpu_defrag_readback_mismatches_total", nil), 0.01)

	// So does a node whose containers cannot be read at all.
	d.cgroups = nil
	d.cgroupfs = nil
	d.liftPoison(testr.New(t), 0)
	require.True(t, d.nodeIsPoisoned(0))
}

func TestPreparingOnAPoisonedNodeFailsClosed(t *testing.T) {
	d := fencedDriver(t)
	d.topology.deviceNameToCPUs = map[string]cpuset.CPUSet{"numa-0": cpuset.New(0, 1, 2, 3)}

	d.applyMu.Lock()
	fenced := d.poisonedNUMANodesOf(d.topology.deviceNameToCPUs["numa-0"])
	d.applyMu.Unlock()

	require.Equal(t, []int{0}, fenced,
		"a device reaching into the fenced node hands out nothing until a read-back agrees")
}
