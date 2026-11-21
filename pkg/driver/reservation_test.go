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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

// reservationDriver is a driver without profiles on mixedCoreCPUInfos' node,
// cores 0,4 and 1,5 of two threads and 2 and 3 of one, not yet started.
func reservationDriver(t *testing.T, client kubernetes.Interface, partitions ...device.Partition) *CPUDriver {
	t.Helper()
	driver, err := New(testr.New(t), Providers{
		CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
		SysFS:     partitionSysFS(),
		K8SClient: client,
	}, &Config{
		DriverName: testDriverName, NodeName: testNodeName,
		ReservedCPUs:  cpuset.New(),
		CPUDeviceMode: device.CPU_DEVICE_MODE_GROUPED, CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
		CPUPartitions: partitions,
	})
	require.NoError(t, err)
	driver.cdiMgr = newMockCdiMgrWithAllocations(nil)
	driver.reservationReadInterval = time.Millisecond
	return driver
}

func reservationNode(capacity, allocatable string) *corev1.Node {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName}}
	if capacity != "" {
		node.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(capacity)}
	}
	if allocatable != "" {
		node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(allocatable)}
	}
	return node
}

func exclusivePartition(name string, threads int, cpus ...int) device.Partition {
	return device.Partition{Name: name, Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(cpus...), ThreadsPerCore: threads}
}

func TestRequiredReservation(t *testing.T) {
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)
	defaultEntry := device.Partition{Name: device.DefaultPartitionName}
	defaultOneThread := device.Partition{Name: device.DefaultPartitionName, ThreadsPerCore: 1}
	for _, tc := range []struct {
		name        string
		partitions  []device.Partition
		unused      bool
		wantCPUs    int
		wantOffline []expectedOffline
	}{{
		name:       "unpartitioned",
		partitions: []device.Partition{{Name: device.DefaultPartitionName, Role: device.PARTITION_ROLE_DEFAULT, CPUs: cpuset.New(0, 1, 2, 3, 4, 5)}},
		unused:     true,
	}, {
		name:       "the default entry alone",
		partitions: []device.Partition{defaultEntry},
	}, {
		name:       "an exclusive partition of one core",
		partitions: []device.Partition{defaultEntry, exclusivePartition("vms", 0, 0, 4)},
		wantCPUs:   2,
	}, {
		name:       "a reserved partition counts like an exclusive one",
		partitions: []device.Partition{defaultEntry, {Name: "infra", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(1, 5)}},
		wantCPUs:   2,
	}, {
		name:       "a named default-role partition",
		partitions: []device.Partition{defaultEntry, {Name: "batch", Role: device.PARTITION_ROLE_DEFAULT, CPUs: cpuset.New(2)}},
		wantCPUs:   1,
	}, {
		name:        "an exclusive partition whose smt leaves a sibling online",
		partitions:  []device.Partition{defaultEntry, exclusivePartition("vms", 1, 0)},
		wantCPUs:    2,
		wantOffline: []expectedOffline{{partition: "vms", cpus: cpuset.New(4)}},
	}, {
		name:        "a default entry under smt: false with online siblings",
		partitions:  []device.Partition{defaultOneThread},
		wantCPUs:    2,
		wantOffline: []expectedOffline{{partition: device.DefaultPartitionName, cpus: cpuset.New(4, 5)}},
	}, {
		name:        "both, in the partitions' order",
		partitions:  []device.Partition{defaultOneThread, exclusivePartition("vms", 1, 0), {Name: "batch", Role: device.PARTITION_ROLE_DEFAULT, CPUs: cpuset.New(3)}},
		wantCPUs:    4,
		wantOffline: []expectedOffline{{partition: "vms", cpus: cpuset.New(4)}, {partition: device.DefaultPartitionName, cpus: cpuset.New(5)}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := requiredReservation(topo.CPUDetails, tc.partitions, !tc.unused)
			require.Equal(t, tc.wantCPUs, got.cpus)
			require.Equal(t, tc.wantOffline, got.offline)
		})
	}
}

func TestReservationMessages(t *testing.T) {
	for _, tc := range []struct {
		name          string
		r             reservationRequirement
		reservedMilli int64
		want          string
	}{{
		name: "no threads expected offline",
		r:    reservationRequirement{cpus: 234},
		want: "kubelet CPU reservation below 234: drain the node, then set `kubeReserved: {cpu: \"234\"}` or `systemReserved: {cpu: \"234\"}`; no dra.cpu devices are published until then",
	}, {
		name:          "threads expected offline, the reservation below the count without them",
		r:             reservationRequirement{cpus: 240, offline: []expectedOffline{{partition: "default", cpus: cpuset.New(128, 129, 130, 131, 132, 133)}}},
		reservedMilli: 233950,
		want:          "kubelet CPU reservation below 240: drain the node, take CPUs 128-133 offline at boot, then set `kubeReserved: {cpu: \"234\"}` or `systemReserved: {cpu: \"234\"}`; no dra.cpu devices are published until then",
	}, {
		name:          "threads of two partitions expected offline, as one list",
		r:             reservationRequirement{cpus: 8, offline: []expectedOffline{{partition: "vms", cpus: cpuset.New(4)}, {partition: "default", cpus: cpuset.New(5, 6)}}},
		reservedMilli: 2000,
		want:          "kubelet CPU reservation below 8: drain the node, take CPUs 4-6 offline at boot, then set `kubeReserved: {cpu: \"5\"}` or `systemReserved: {cpu: \"5\"}`; no dra.cpu devices are published until then",
	}, {
		name:          "the reservation reaches the count without the threads, which are online",
		r:             reservationRequirement{cpus: 240, offline: []expectedOffline{{partition: "default", cpus: cpuset.New(128, 129, 130, 131, 132, 133)}}},
		reservedMilli: 234000,
		want:          "kubelet CPU reservation of 234 needs CPUs 128-133 offline: take them offline at boot",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, tc.r.missingMessage("dra.cpu", tc.reservedMilli))
		})
	}
}

// The Node's capacity minus its allocatable is compared, in millicores, with
// the count of CPUs required: release §5.2's prod node needs 234 of its 246.
func TestCheckReservation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		node        *corev1.Node
		wantMissing bool
		wantMessage string
		wantErr     string
	}{{
		name:        "nothing reserved but Talos's 50m",
		node:        reservationNode("246", "245950m"),
		wantMissing: true,
		wantMessage: "kubelet CPU reservation below 234: drain the node, then set `kubeReserved: {cpu: \"234\"}` or `systemReserved: {cpu: \"234\"}`; no " + testDriverName + " devices are published until then",
	}, {
		name:        "234 and Talos's 50m",
		node:        reservationNode("246", "11950m"),
		wantMessage: "kubelet reserves 234.05 CPUs, 234 required",
	}, {
		name:        "more than required",
		node:        reservationNode("246", "11"),
		wantMessage: "kubelet reserves 235 CPUs, 234 required",
	}, {
		name:        "one millicore short",
		node:        reservationNode("246", "12001m"),
		wantMissing: true,
		wantMessage: "kubelet CPU reservation below 234: drain the node, then set `kubeReserved: {cpu: \"234\"}` or `systemReserved: {cpu: \"234\"}`; no " + testDriverName + " devices are published until then",
	}, {
		name:    "a Node without capacity yet",
		node:    reservationNode("", "11"),
		wantErr: `node "` + testNodeName + `" reports no cpu capacity yet`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			driver := reservationDriver(t, fake.NewClientset())
			driver.reservation = reservationRequirement{cpus: 234}
			_, err := driver.checkReservation(testr.New(t), tc.node)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantMissing, driver.reservationMissing.Load())
			driver.stateMu.Lock()
			value := driver.reservationCondition().evaluate()
			driver.stateMu.Unlock()
			require.Equal(t, tc.wantMessage, value.message)
		})
	}
}

// Where no CPU is counted the check reads nothing and keeps nothing back, and
// the driver keeps no reservation condition.
func TestAwaitReservationWithoutPartitions(t *testing.T) {
	client := fake.NewClientset()
	var reads atomic.Int32
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		reads.Add(1)
		return true, nil, errors.New("connection refused")
	})
	for _, partitions := range [][]device.Partition{nil, {{Name: device.DefaultPartitionName}}} {
		driver := reservationDriver(t, client, partitions...)
		ctx := ctxlog.NewContext(context.Background(), testr.New(t))
		require.NoError(t, driver.awaitReservation(ctx, make(chan error)))
		require.False(t, driver.reservationMissing.Load())
		for _, src := range driver.nodeConditions.sources {
			require.NotEqual(t, conditionReservationMissing, src.conditionType)
		}
	}
	require.Zero(t, reads.Load())
}

// The call that comes before the registration does not return while the API
// server does not answer, and returns once the Node is read, healthy or
// degraded.
func TestAwaitReservationHoldsTheRegistration(t *testing.T) {
	for _, tc := range []struct {
		name        string
		node        *corev1.Node
		wantMissing bool
	}{
		{name: "healthy", node: reservationNode("6", "4")},
		{name: "degraded", node: reservationNode("6", "6"), wantMissing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			client := fake.NewClientset(tc.node)
			var reachable atomic.Bool
			var reads atomic.Int32
			client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				reads.Add(1)
				if !reachable.Load() {
					return true, nil, errors.New("dial tcp 10.96.0.1:443: connect: connection refused")
				}
				return false, nil, nil
			})
			driver := reservationDriver(t, client, device.Partition{Name: device.DefaultPartitionName}, exclusivePartition("vms", 0, 0, 4))
			require.Equal(t, 2, driver.reservation.cpus)

			done := make(chan error, 1)
			go func() { done <- driver.awaitReservation(ctx, make(chan error)) }()
			require.Eventually(t, func() bool { return reads.Load() >= 3 }, 5*time.Second, time.Millisecond)
			select {
			case err := <-done:
				t.Fatalf("awaitReservation returned %v before the Node was read", err)
			default:
			}
			reachable.Store(true)
			require.NoError(t, <-done)
			require.Equal(t, tc.wantMissing, driver.reservationMissing.Load())
		})
	}
}

func TestAwaitReservationEndsOnAnNRIFailure(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	client := fake.NewClientset()
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	driver := reservationDriver(t, client, device.Partition{Name: device.DefaultPartitionName}, exclusivePartition("vms", 0, 0, 4))
	nriErr := make(chan error, 1)
	nriErr <- errors.New("connection to the runtime lost")
	require.EqualError(t, driver.awaitReservation(ctx, nriErr),
		"NRI plugin failed before the node's kubelet CPU reservation was checked: connection to the runtime lost")
}

type recordingPlugin struct {
	mu        sync.Mutex
	published []int
}

func (p *recordingPlugin) PublishResources(_ context.Context, resources resourceslice.DriverResources) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	devices := 0
	for _, slice := range resources.Pools[testNodeName].Slices {
		devices += len(slice.Devices)
	}
	p.published = append(p.published, devices)
	return nil
}

func (p *recordingPlugin) RegistrationStatus() *registerapi.RegistrationStatus {
	return &registerapi.RegistrationStatus{PluginRegistered: true}
}

func (p *recordingPlugin) Stop() {}

func (p *recordingPlugin) last() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.published) == 0 {
		return 0, -1
	}
	return len(p.published), p.published[len(p.published)-1]
}

func reservationConditionOf(t *testing.T, client kubernetes.Interface) *corev1.NodeCondition {
	t.Helper()
	node, err := client.CoreV1().Nodes().Get(context.Background(), testNodeName, metav1.GetOptions{})
	require.NoError(t, err)
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == conditionReservationMissing {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

// A node read degraded publishes its pool empty, refuses Prepare but not
// Unprepare and is not ready; once the kubelet reserves enough, the Node
// update the informer hands the writer republishes the devices and turns the
// condition False; the shutdown writes it Unknown.
func TestReservationTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(ctxlog.NewContext(context.Background(), testr.New(t)))
	defer cancel()
	client := fake.NewClientset(reservationNode("6", "6"))
	driver := reservationDriver(t, client, device.Partition{Name: device.DefaultPartitionName}, exclusivePartition("vms", 0, 0, 4))
	plugin := &recordingPlugin{}
	driver.draPlugin = plugin
	_, err := driver.Synchronize(ctx, nil, nil)
	require.NoError(t, err)

	require.NoError(t, driver.awaitReservation(ctx, make(chan error)))
	require.True(t, driver.reservationMissing.Load())
	require.False(t, driver.Ready(), "a node missing its reservation is ready")

	driver.serve(ctx)
	defer driver.health.Stop()
	require.Eventually(t, func() bool {
		cond := reservationConditionOf(t, client)
		return cond != nil && cond.Status == corev1.ConditionTrue
	}, 5*time.Second, time.Millisecond)
	cond := reservationConditionOf(t, client)
	require.Equal(t, "InsufficientCPUReservation", cond.Reason)
	require.Equal(t, driver.reservation.missingMessage(testDriverName, 0), cond.Message)
	require.Eventually(t, func() bool { n, devices := plugin.last(); return n > 0 && devices == 0 }, 5*time.Second, time.Millisecond)

	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "ns", UID: "uid-1"}}
	prepared, err := driver.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.EqualError(t, prepared[claim.UID].Err, driver.reservation.missingMessage(testDriverName, 0))
	unprepared, err := driver.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: types.UID("uid-2")}})
	require.NoError(t, err)
	require.NoError(t, unprepared["uid-2"])

	node := reservationNode("6", "4")
	_, err = client.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cond := reservationConditionOf(t, client)
		return cond != nil && cond.Status == corev1.ConditionFalse
	}, 5*time.Second, time.Millisecond)
	cond = reservationConditionOf(t, client)
	require.Equal(t, "SufficientCPUReservation", cond.Reason)
	require.Equal(t, "kubelet reserves 2 CPUs, 2 required", cond.Message)
	_, devices := plugin.last()
	require.Positive(t, devices, "the recovery did not republish the devices")
	require.False(t, driver.reservationMissing.Load())

	driver.stopNodeWatch()
	driver.nodeConditions.stop()
	driver.nodeConditions.writeStopped()
	cond = reservationConditionOf(t, client)
	require.Equal(t, corev1.ConditionUnknown, cond.Status)
	require.Equal(t, "driver stopped; reservation not checked", cond.Message)
}

// Without list and watch on nodes the watch stops after the first refusal,
// with one log line naming the verbs, and the driver keeps its start's result.
func TestWatchNodeStopsWhenForbidden(t *testing.T) {
	var mu sync.Mutex
	var lines []string
	logger := funcr.New(func(prefix, args string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, args)
	}, funcr.Options{})
	ctx, cancel := context.WithCancel(logr.NewContext(context.Background(), logger))
	defer cancel()
	client := fake.NewClientset(reservationNode("6", "4"))
	var lists atomic.Int32
	client.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		lists.Add(1)
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "", errors.New("RBAC"))
	})
	driver := reservationDriver(t, client, device.Partition{Name: device.DefaultPartitionName}, exclusivePartition("vms", 0, 0, 4))
	driver.watchNode(ctx)
	select {
	case <-driver.nodeWatchDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the node watch did not stop after a forbidden list")
	}
	require.Equal(t, int32(1), lists.Load())
	mu.Lock()
	defer mu.Unlock()
	var named int
	for _, line := range lines {
		if strings.Contains(line, "grant the driver list and watch on nodes") {
			named++
		}
	}
	require.Equal(t, 1, named, "log lines: %v", lines)
}
