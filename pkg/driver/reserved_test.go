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
	"fmt"
	"maps"
	"testing"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

// reservedPartitions is mixedCoreCPUInfos' node with the reserved partition
// system on core 1, which held claims' CPUs before the partitions changed.
func reservedPartitions(vms device.Partition) []device.Partition {
	return []device.Partition{
		{Name: device.DefaultPartitionName},
		vms,
		{Name: "system", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(1, 5)},
	}
}

var vmsOnCore0 = device.Partition{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 4)}

func exclusiveRecord(name string, cpus ...int) cdiClaimRecord {
	return cdiClaimRecord{Namespace: "vms", Name: name, Requests: []cdiRequestRecord{
		{Request: "vcpu", CPUs: cpuset.New(cpus...), Results: []cdiResultRecord{{Device: "cpudevnuma000-vms"}}},
	}}
}

// reservedDriver is a driver whose configuration declares partitions, with
// onDisk restored at its start.
func reservedDriver(t *testing.T, partitions []device.Partition, onDisk map[types.UID]cdiClaimRecord) (*CPUDriver, *fake.Clientset) {
	t.Helper()
	client := fake.NewClientset(testConditionNode(testReadyCond))
	d, err := New(testr.New(t), Providers{
		CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
		SysFS:     partitionSysFS(),
		K8SClient: client,
	}, &Config{
		DriverName:       testDriverName,
		NodeName:         testNodeName,
		ReservedCPUs:     cpuset.New(),
		CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
		CPUPartitions:    partitions,
	})
	require.NoError(t, err)
	cdi := newMockCdiMgr()
	maps.Copy(cdi.records, onDisk)
	d.cdiMgr = cdi
	d.seedAllocationStoreFromDisk(testr.New(t))
	t.Cleanup(d.nodeConditions.queue.ShutDown)
	return d, client
}

func claimContainer(id string, state api.ContainerState, claim types.UID, cpus string) *api.Container {
	return &api.Container{Id: id, Name: id, PodSandboxId: "sandbox", State: state, Env: []string{fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claim, cpus)}}
}

// killed returns the containers the updates kill, and the cpusets of the
// others.
func killed(updates []*api.ContainerUpdate) ([]string, map[string]string) {
	var kills []string
	cpus := make(map[string]string)
	for _, update := range updates {
		if update.GetLinux().GetResources().GetUnified()["cgroup.kill"] == "1" {
			kills = append(kills, update.GetContainerId())
			continue
		}
		cpus[update.GetContainerId()] = update.GetLinux().GetResources().GetCpu().GetCpus()
	}
	return kills, cpus
}

// Without profiles the partitions and the restored claims are known before
// the first Synchronize, whose reply kills each running container of a claim
// whose exclusive CPUs a reserved partition now holds, and every later reply
// again while the claim is prepared.
func TestSynchronizeKillsContainersOfClaimsAReservedPartitionHolds(t *testing.T) {
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	for _, tc := range []struct {
		name       string
		onDisk     map[types.UID]cdiClaimRecord
		containers []*api.Container
		wantKills  []string
		wantCPUs   map[string]string
	}{{
		name:   "a running and a paused container of a held claim, and a created one",
		onDisk: map[types.UID]cdiClaimRecord{"vm": exclusiveRecord("vm", 1, 5)},
		containers: []*api.Container{
			claimContainer("running", api.ContainerState_CONTAINER_RUNNING, "vm", "1,5"),
			claimContainer("paused", api.ContainerState_CONTAINER_PAUSED, "vm", "1,5"),
			claimContainer("created", api.ContainerState_CONTAINER_CREATED, "vm", "1,5"),
		},
		wantKills: []string{"running", "paused"},
		wantCPUs:  map[string]string{},
	}, {
		name:       "a reserved partition that shrank holds nothing",
		onDisk:     map[types.UID]cdiClaimRecord{"vm": exclusiveRecord("vm", 0, 4)},
		containers: []*api.Container{claimContainer("running", api.ContainerState_CONTAINER_RUNNING, "vm", "0,4")},
		wantCPUs:   map[string]string{"running": "0,4"},
	}, {
		name:       "a share recorded on CPUs a reserved partition now holds",
		onDisk:     map[types.UID]cdiClaimRecord{"share": shareOnDisk(cpuset.New(1, 2, 3, 5), "cpudevnuma000")},
		containers: []*api.Container{{Id: "running", Name: "running", PodSandboxId: "sandbox", State: api.ContainerState_CONTAINER_RUNNING, Env: []string{fmt.Sprintf("%s_share_vmm-cpu=%s", cdiEnvVarPrefix, cdiEnvDynamicValue)}}},
		wantCPUs:   map[string]string{"running": "2-3"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d, _ := reservedDriver(t, reservedPartitions(vmsOnCore0), tc.onDisk)

			for i := range 2 {
				updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, tc.containers)
				require.NoError(t, err)
				kills, cpus := killed(updates)
				require.Equalf(t, tc.wantKills, kills, "Synchronize %d", i+1)
				require.Equalf(t, tc.wantCPUs, cpus, "Synchronize %d", i+1)
			}
		})
	}
}

// Once the held claim is unprepared, Synchronize kills nothing more.
func TestSynchronizeStopsKillingOnceTheClaimIsUnprepared(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	d, _ := reservedDriver(t, reservedPartitions(vmsOnCore0), map[types.UID]cdiClaimRecord{"vm": exclusiveRecord("vm", 1, 5)})
	running := claimContainer("running", api.ContainerState_CONTAINER_RUNNING, "vm", "1,5")

	updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{running})
	require.NoError(t, err)
	kills, _ := killed(updates)
	require.Equal(t, []string{"running"}, kills)

	_, err = d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "vm"}})
	require.NoError(t, err)
	updates, err = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{running})
	require.NoError(t, err)
	kills, _ = killed(updates)
	require.Empty(t, kills)
}

// With profiles the claims are restored after the profile read, and the kills
// go out with the narrowing: unsolicited where that is safe, and where the
// runtime fails them, in the reply to the next Synchronize; otherwise in the
// reply to the Synchronize of the reconnect the narrowing makes.
func TestKillsWithProfiles(t *testing.T) {
	held := map[types.UID]cpuset.CPUSet{"vm": cpuset.New(1, 5)}
	for _, tc := range []struct {
		name       string
		assumeSafe bool
		failing    bool
	}{
		{name: "unsolicited", assumeSafe: true},
		{name: "unsolicited, the runtime failing them", assumeSafe: true, failing: true},
		{name: "in the reconnect's Synchronize"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			client := fake.NewClientset(labelledNode(map[string]string{driverconfig.ProfileLabel: "r7625"}))
			d := partitionedProfiledDriver(t, client, tc.assumeSafe, held, reservedPartitions(vmsOnCore0)...)
			updater := &failingUpdater{fail: tc.failing}
			if tc.assumeSafe {
				d.containerUpdater = updater
			}
			pod := windowPod("claim", false)
			running := windowContainer("workload", pod, fmt.Sprintf("%s_vm=1,5", cdiEnvVarPrefix))

			_, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{running})
			require.NoError(t, err)
			bound, err := d.configureFromProfile(ctx, make(chan error))
			require.NoError(t, err)
			if !tc.assumeSafe {
				kills, _ := killed(bound)
				require.Equal(t, []string{"workload"}, kills, "the judgment of the kept containers")
				updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{running})
				require.NoError(t, err)
				kills, _ = killed(updates)
				require.Equal(t, []string{"workload"}, kills, "the reconnect's Synchronize")
				return
			}
			require.NoError(t, d.narrowWindowContainers(ctx, make(chan error), bound))
			kills, _ := killed(updater.sent)
			require.Equal(t, []string{"workload"}, kills, "unsolicited")
			if tc.failing {
				updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{running})
				require.NoError(t, err)
				kills, _ = killed(updates)
				require.Equal(t, []string{"workload"}, kills, "the next Synchronize resends the failed kill")
			}
		})
	}
}

type failingUpdater struct {
	fail bool
	sent []*api.ContainerUpdate
}

func (u *failingUpdater) UpdateContainers(updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	u.sent = append(u.sent, updates...)
	if u.fail {
		return updates, fmt.Errorf("runtime refused the updates")
	}
	return nil, nil
}

// A container of a claim a reserved partition holds is refused at
// CreateContainer, and a created container of one at StartContainer.
func TestContainersOfAHeldClaimAreRefused(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	d, _ := reservedDriver(t, reservedPartitions(vmsOnCore0), map[types.UID]cdiClaimRecord{
		"vm":   exclusiveRecord("vm", 1, 5),
		"kept": exclusiveRecord("kept", 0, 4),
	})
	refusal := `reserved partition "system" now holds CPUs 1,5 of prepared claim vms/vm (request "vcpu"); its containers are stopped: delete the claim's pod or keep those CPUs out of the partition`

	_, _, err := d.CreateContainer(ctx, pod, claimContainer("vm", api.ContainerState_CONTAINER_UNKNOWN, "vm", "1,5"))
	require.EqualError(t, err, `cannot create container "vm" in pod "pod": `+refusal)
	require.EqualError(t, d.StartContainer(ctx, pod, claimContainer("vm", api.ContainerState_CONTAINER_CREATED, "vm", "1,5")),
		`cannot start container "vm" in pod "pod": `+refusal)

	adjust, _, err := d.CreateContainer(ctx, pod, claimContainer("kept", api.ContainerState_CONTAINER_UNKNOWN, "kept", "0,4"))
	require.NoError(t, err)
	require.Equal(t, "0,4", adjust.GetLinux().GetResources().GetCpu().GetCpus())
	require.NoError(t, d.StartContainer(ctx, pod, claimContainer("kept", api.ContainerState_CONTAINER_CREATED, "kept", "0,4")))

	_, err = d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "vm"}})
	require.NoError(t, err)
	require.NoError(t, d.StartContainer(ctx, pod, claimContainer("vm", api.ContainerState_CONTAINER_CREATED, "vm", "1,5")), "the claim is no longer held")
}

// CPUPartitionDegraded names every claim a reserved partition holds, beside
// the partitions whose smt the kernel contradicts, and is written again at
// each Unprepare of such a claim.
func TestPartitionDegradedConditionNamesHeldClaims(t *testing.T) {
	twoClaims := map[types.UID]cdiClaimRecord{"vm": exclusiveRecord("vm", 1), "vm2": exclusiveRecord("vm2", 5)}
	smt := `partitions publish no devices while the kernel contradicts their smt: partition "vms" expects 1 online thread per core, and CPUs 4 of its cores are online too; take those CPUs offline at boot, then restart the driver`
	both := `reserved partition "system" now holds CPUs 1 of prepared claim vms/vm (request "vcpu"); reserved partition "system" now holds CPUs 5 of prepared claim vms/vm2 (request "vcpu"); their containers are stopped: delete the claims' pods or keep those CPUs out of the partitions`
	one := `reserved partition "system" now holds CPUs 5 of prepared claim vms/vm2 (request "vcpu"); its containers are stopped: delete the claim's pod or keep those CPUs out of the partition`
	verified := corev1.NodeCondition{Status: corev1.ConditionFalse, Reason: "PartitionsVerified", Message: "the kernel has online the threads per core every CPU partition's smt declares"}
	for _, tc := range []struct {
		name string
		vms  device.Partition
		want []corev1.NodeCondition
	}{{
		name: "claims held",
		vms:  vmsOnCore0,
		want: []corev1.NodeCondition{
			{Status: corev1.ConditionTrue, Reason: "ReservedHoldsClaims", Message: both},
			{Status: corev1.ConditionTrue, Reason: "ReservedHoldsClaims", Message: one},
			verified,
		},
	}, {
		name: "claims held and an smt the kernel contradicts",
		vms:  device.Partition{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0), ThreadsPerCore: 1},
		want: []corev1.NodeCondition{
			{Status: corev1.ConditionTrue, Reason: "SiblingThreadsOnline", Message: smt + "; " + both},
			{Status: corev1.ConditionTrue, Reason: "SiblingThreadsOnline", Message: smt + "; " + one},
			{Status: corev1.ConditionTrue, Reason: "SiblingThreadsOnline", Message: smt},
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d, client := reservedDriver(t, reservedPartitions(tc.vms), twoClaims)
			written := func() corev1.NodeCondition {
				t.Helper()
				require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
				node, err := client.CoreV1().Nodes().Get(t.Context(), testNodeName, metav1.GetOptions{})
				require.NoError(t, err)
				_, got := nodeutil.GetNodeCondition(&node.Status, conditionPartitionDegraded)
				require.NotNil(t, got)
				return corev1.NodeCondition{Status: got.Status, Reason: got.Reason, Message: got.Message}
			}

			require.Equal(t, tc.want[0], written())
			for i, uid := range []types.UID{"vm", "vm2"} {
				_, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: uid}})
				require.NoError(t, err)
				require.Equal(t, 1, d.nodeConditions.queue.Len(), "the Unprepare puts a key on the writer's queue")
				key, _ := d.nodeConditions.queue.Get()
				d.nodeConditions.queue.Done(key)
				d.nodeConditions.queue.Forget(key)
				require.Equalf(t, tc.want[i+1], written(), "after unpreparing %s", uid)
			}
		})
	}
}

// Unpreparing a claim no reserved partition holds puts nothing on the
// writer's queue.
func TestUnprepareOfAClaimNotHeld(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	d, _ := reservedDriver(t, reservedPartitions(vmsOnCore0), map[types.UID]cdiClaimRecord{
		"vm":   exclusiveRecord("vm", 1, 5),
		"kept": exclusiveRecord("kept", 0, 4),
	})
	_, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "kept"}})
	require.NoError(t, err)
	require.Zero(t, d.nodeConditions.queue.Len())
	require.Contains(t, d.reservedHolds, types.UID("vm"))
}

// The driver is not ready while a reserved partition holds a prepared claim's
// CPUs, and is again once the last such claim is unprepared.
func TestReadyWhileAReservedPartitionHoldsAClaim(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	d, _ := reservedDriver(t, reservedPartitions(vmsOnCore0), map[types.UID]cdiClaimRecord{
		"vm":  exclusiveRecord("vm", 1),
		"vm2": exclusiveRecord("vm2", 5),
	})
	d.draPlugin = &mockKubeletPlugin{statusFunc: func(int32) *registerapi.RegistrationStatus {
		return &registerapi.RegistrationStatus{PluginRegistered: true}
	}}
	_, err := d.Synchronize(ctx, nil, nil)
	require.NoError(t, err)

	require.False(t, d.Ready())
	_, err = d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "vm"}})
	require.NoError(t, err)
	require.False(t, d.Ready(), "a claim is still held")
	_, err = d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "vm2"}})
	require.NoError(t, err)
	require.True(t, d.Ready())
}
