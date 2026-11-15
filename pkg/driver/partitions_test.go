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
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	dracpuapi "github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/utils/cpuset"
)

// partitionSysFS is mixedCoreCPUInfos' node: CPUs 0-7 present, 6 and 7 offline,
// and CPU 8 online but missing from the topology.
func partitionSysFS() fstest.MapFS {
	return fstest.MapFS{
		filepath.Join("devices", "system", "cpu", "present"): &fstest.MapFile{Data: []byte("0-8\n")},
		filepath.Join("devices", "system", "cpu", "online"):  &fstest.MapFile{Data: []byte("0-5,8\n")},
	}
}

func TestValidatePartitions(t *testing.T) {
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)
	present, online := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8), cpuset.New(0, 1, 2, 3, 4, 5, 8)
	partition := func(name string, threads int, cpus ...int) device.Partition {
		return device.Partition{Name: name, Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(cpus...), ThreadsPerCore: threads}
	}
	defaultEntry := device.Partition{Name: device.DefaultPartitionName}

	for _, tc := range []struct {
		name     string
		declared []device.Partition
		wantErr  string
	}{
		{
			name:     "whole cores, one of them with its sibling offline",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 4), partition("spdk", 1, 2)},
		},
		{
			name:     "a CPU the node does not have",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 4, 9)},
			wantErr:  `partition "vms" names CPUs 9, which this node does not have: remove them from its cpuset`,
		},
		{
			name:     "an offline CPU",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 2, 6)},
			wantErr:  `partition "spdk" names offline CPUs 6: bring them online, or remove them from its cpuset`,
		},
		{
			name:     "an online CPU outside the topology",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 3, 8)},
			wantErr:  `partition "vms" names CPUs 8, whose topology the kernel reports incompletely so the driver does not manage them: remove them from its cpuset`,
		},
		{
			name:     "two smt false partitions naming the two threads of one core",
			declared: []device.Partition{partition("a", 1, 0), defaultEntry, partition("b", 1, 4)},
			wantErr:  `partitions "a" and "b" name threads of one physical core, CPUs 0,4: list all of a core's threads in one partition`,
		},
		{
			name:     "a core split with the default partition",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 1, 4)},
			wantErr:  `partition "vms" splits the physical cores of CPUs 1: a partition holds whole cores, so list their sibling threads 5 in it too`,
		},
		{
			name:     "an smt false partition whose sibling is online is left to the smt check",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 1)},
		},
		{
			name:     "a partition without smt whose sibling is online",
			declared: []device.Partition{defaultEntry, partition("spdk", 0, 1)},
			wantErr:  `partition "spdk" splits the physical cores of CPUs 1: a partition holds whole cores, so list their sibling threads 5 in it too`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitions(topo.CPUDetails, present, online, tc.declared)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tc.wantErr)
		})
	}
}

func TestNewWithPartitions(t *testing.T) {
	declared := []device.Partition{
		{Name: device.DefaultPartitionName},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 1, 4, 5)},
		{Name: "system", Role: device.PARTITION_ROLE_RESERVED, CPUs: cpuset.New(3)},
	}
	spdk := device.Partition{Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1), ThreadsPerCore: 1}
	for _, tc := range []struct {
		name          string
		sysfs         fstest.MapFS
		declared      []device.Partition
		wantErr       string
		wantReserved  cpuset.CPUSet
		wantSlices    [][]string
		wantShared    map[string]cpuset.CPUSet
		wantClaimless string
		wantVerified  map[string]float64
	}{
		{
			name:         "the default partition publishes shared devices, the exclusive one owned, the reserved one holds its CPUs back",
			sysfs:        partitionSysFS(),
			declared:     declared,
			wantReserved: cpuset.New(3),
			wantSlices:   [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}},
			wantShared:   map[string]cpuset.CPUSet{"cpudevnuma000": cpuset.New(2)},
			wantVerified: map[string]float64{"default": 1, "vms": 1, "system": 1},
		},
		{
			name:          "an exclusive partition whose smt the kernel contradicts publishes nothing",
			sysfs:         partitionSysFS(),
			declared:      []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 4)}, spdk},
			wantReserved:  cpuset.New(),
			wantSlices:    [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}},
			wantShared:    map[string]cpuset.CPUSet{"cpudevnuma000": cpuset.New(2, 3)},
			wantClaimless: "2-3",
			wantVerified:  map[string]float64{"default": 1, "vms": 1, "spdk": 0},
		},
		{
			name:          "the default entry whose smt the kernel contradicts publishes nothing, and containers without claims keep its CPUs",
			sysfs:         partitionSysFS(),
			declared:      []device.Partition{{Name: device.DefaultPartitionName, ThreadsPerCore: 1}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)}},
			wantReserved:  cpuset.New(),
			wantSlices:    [][]string{{"cpudevnuma000-vms"}},
			wantShared:    map[string]cpuset.CPUSet{},
			wantClaimless: "0,2-3",
			wantVerified:  map[string]float64{"default": 0, "vms": 1},
		},
		{
			name:          "every partition degraded publishes an empty pool",
			sysfs:         partitionSysFS(),
			declared:      []device.Partition{{Name: device.DefaultPartitionName, ThreadsPerCore: 1}, spdk},
			wantReserved:  cpuset.New(),
			wantShared:    map[string]cpuset.CPUSet{},
			wantClaimless: "0,2-3",
			wantVerified:  map[string]float64{"default": 0, "spdk": 0},
		},
		{
			name:     "a partition naming an offline CPU stops the driver",
			sysfs:    partitionSysFS(),
			declared: []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(7)}},
			wantErr:  `partition "vms" names offline CPUs 7: bring them online, or remove them from its cpuset`,
		},
		{
			name:     "the present CPUs unreadable",
			sysfs:    testSysFS(mixedCoreCPUInfos),
			declared: declared,
			wantErr:  "failed to read the present CPUs: open devices/system/cpu/present: file does not exist",
		},
		{
			name:         "without partitions nothing is read and the node is upstream's",
			sysfs:        fstest.MapFS{},
			wantReserved: cpuset.New(),
			wantSlices:   [][]string{{"cpudevnuma000"}},
			wantShared:   map[string]cpuset.CPUSet{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			d, err := New(testr.New(t), Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   tc.sysfs,
			}, &Config{
				DriverName:       testDriverName,
				NodeName:         testNodeName,
				ReservedCPUs:     cpuset.New(),
				CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
				CPUPartitions:    tc.declared,
				Metrics:          cpumetrics.New(reg),
			})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			for partition, want := range tc.wantVerified {
				require.Equalf(t, want, metricValue(t, reg, "dra_cpu_partition_verified", map[string]string{"partition": partition}), "partition %s", partition)
			}
			if tc.wantClaimless != "" {
				claimless, confined := d.allocationState.ConfinedClaimlessCPUs()
				require.True(t, confined)
				require.Equal(t, tc.wantClaimless, claimless.String())
			}
			plugin := &mockKubeletPlugin{}
			d.draPlugin = plugin
			d.PublishResources(context.Background())
			require.NotNil(t, plugin.publishedResources, "the node's pool is published even with no device")
			pool, ok := plugin.publishedResources.Pools[testNodeName]
			require.True(t, ok, "the node's pool is published even with no device")
			require.Len(t, pool.Slices, len(tc.wantSlices))
			require.Truef(t, tc.wantReserved.Equals(d.topology.ReservedCPUs), "reserved CPUs: got %s, want %s", d.topology.ReservedCPUs, tc.wantReserved)
			require.Truef(t, d.allocationState.GetSharedCPUs().Intersection(tc.wantReserved).IsEmpty(), "shared CPUs %s hold reserved ones", d.allocationState.GetSharedCPUs())
			var gotSlices [][]string
			for _, slice := range d.topology.deviceSlices {
				var names []string
				for _, dev := range slice {
					names = append(names, dev.Name)
				}
				gotSlices = append(gotSlices, names)
			}
			require.Equal(t, tc.wantSlices, gotSlices)
			require.Equal(t, tc.wantShared, d.topology.sharedDevices)
		})
	}
}

func TestSeedCountsMisplacedClaims(t *testing.T) {
	request := func(name, device string, cpus ...int) cdiRequestRecord {
		return cdiRequestRecord{Request: name, CPUs: cpuset.New(cpus...), Results: []cdiResultRecord{{Device: device}}}
	}
	partitions := []device.Partition{
		{Name: device.DefaultPartitionName},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
		{Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(2)},
	}
	for _, tc := range []struct {
		name     string
		declared []device.Partition
		requests []cdiRequestRecord
		want     float64
	}{
		{
			name:     "requests in two partitions, each inside its device's",
			declared: partitions,
			requests: []cdiRequestRecord{request("vcpu", "cpudevnuma000-vms", 1, 5), request("poll", "cpudevnuma000-spdk", 2)},
		},
		{
			name:     "a request outside its device's partition",
			declared: partitions,
			requests: []cdiRequestRecord{request("vcpu", "cpudevnuma000-vms", 1, 2), request("poll", "cpudevnuma000-spdk", 2)},
			want:     1,
		},
		{
			name:     "two requests outside count the claim once",
			declared: partitions,
			requests: []cdiRequestRecord{request("vcpu", "cpudevnuma000-vms", 0), request("poll", "cpudevnuma000-spdk", 3)},
			want:     1,
		},
		{
			name:     "a device the node no longer publishes",
			declared: partitions,
			requests: []cdiRequestRecord{request("vcpu", "cpudevnuma000-dpdk", 1)},
			want:     1,
		},
		{
			name:     "no partitions",
			requests: []cdiRequestRecord{request("vcpu", "cpudevnuma001", 1)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			d, err := New(testr.New(t), Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   partitionSysFS(),
			}, &Config{
				DriverName:       testDriverName,
				NodeName:         testNodeName,
				ReservedCPUs:     cpuset.New(),
				CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
				CPUPartitions:    tc.declared,
				Metrics:          cpumetrics.New(reg),
			})
			require.NoError(t, err)
			cdi := newMockCdiMgr()
			cdi.records["claim-0"] = cdiClaimRecord{Namespace: "default", Name: "vm-0", Requests: tc.requests}
			d.cdiMgr = cdi

			d.seedAllocationStoreFromDisk(testr.New(t))
			require.Equal(t, tc.want, metricValue(t, reg, "dra_cpu_misplaced_claims_total", nil))
		})
	}
}

func TestPrepareTakesCPUsFromTheDevicePartition(t *testing.T) {
	for _, tc := range []struct {
		groupBy string
		device  string
	}{
		{groupBy: device.GROUP_BY_NUMA_NODE, device: "cpudevnuma000-vms"},
		{groupBy: device.GROUP_BY_SOCKET, device: "cpudevsocket000-vms"},
	} {
		t.Run(tc.groupBy, func(t *testing.T) {
			d, err := New(testr.New(t), Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   partitionSysFS(),
			}, &Config{
				DriverName:       testDriverName,
				NodeName:         testNodeName,
				ReservedCPUs:     cpuset.New(),
				CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy: tc.groupBy,
				CPUPartitions: []device.Partition{
					{Name: device.DefaultPartitionName},
					{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
				},
			})
			require.NoError(t, err)
			d.cdiMgr = newMockCdiMgr()
			d.hasSynchronized = true

			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{
				testClaim("claim-0", testDriverName, testNodeName, map[string]int64{tc.device: 2}),
			})
			require.NoError(t, err)
			require.NoError(t, results["claim-0"].Err)
			got, ok := d.allocationState.GetResourceClaimAllocation("claim-0")
			require.True(t, ok)
			require.Truef(t, cpuset.New(1, 5).Equals(got), "claim CPUs: got %s, want the partition's 1,5", got)

			results, err = d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{
				testClaim("claim-1", testDriverName, testNodeName, map[string]int64{tc.device: 1}),
			})
			require.NoError(t, err)
			require.Error(t, results["claim-1"].Err, "the partition has no CPU left, though the NUMA node and socket do")
		})
	}
}

func TestVerifyThreadArity(t *testing.T) {
	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		partition device.Partition
		want      string
	}{{
		name:      "no smt accepts any thread count",
		partition: device.Partition{Name: "vms", CPUs: cpuset.New(1, 5)},
	}, {
		name:      "smt 2 on cores of two threads",
		partition: device.Partition{Name: "vms", CPUs: cpuset.New(1, 5), ThreadsPerCore: 2},
	}, {
		name:      "smt false on cores whose siblings the kernel has offline",
		partition: device.Partition{Name: "spdk", CPUs: cpuset.New(2, 3), ThreadsPerCore: 1},
	}, {
		name:      "smt false on a core whose sibling is online",
		partition: device.Partition{Name: "spdk", CPUs: cpuset.New(1, 2), ThreadsPerCore: 1},
		want:      `partition "spdk" expects 1 online thread per core, and CPUs 5 of its cores are online too`,
	}, {
		name:      "smt false on a core whose two threads it lists",
		partition: device.Partition{Name: "vms", CPUs: cpuset.New(1, 5), ThreadsPerCore: 1},
		want:      `partition "vms" expects 1 online thread per core, and CPUs 5 of its cores are online too`,
	}, {
		name:      "the default entry's lowest threads, its cores' others online",
		partition: device.Partition{Name: device.DefaultPartitionName, CPUs: cpuset.New(0, 1, 2, 3), ThreadsPerCore: 1},
		want:      `partition "default" expects 1 online thread per core, and CPUs 4-5 of its cores are online too`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, verifyThreadArity(topo.CPUDetails, tc.partition))
		})
	}
}

// CPUPartitionDegraded is written wherever partitions are used, the least
// list included, and nowhere else.
func TestPartitionDegradedCondition(t *testing.T) {
	vms := device.Partition{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)}
	spdk := device.Partition{Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1), ThreadsPerCore: 1}
	for _, tc := range []struct {
		name       string
		partitions []device.Partition
		want       *corev1.NodeCondition
	}{{
		name: "no partitions",
	}, {
		name:       "the least list",
		partitions: []device.Partition{{Name: device.DefaultPartitionName}},
		want:       &corev1.NodeCondition{Status: corev1.ConditionFalse, Reason: "PartitionsVerified", Message: "the kernel has online the threads per core every CPU partition's smt declares"},
	}, {
		name:       "every smt matching",
		partitions: []device.Partition{{Name: device.DefaultPartitionName}, vms},
		want:       &corev1.NodeCondition{Status: corev1.ConditionFalse, Reason: "PartitionsVerified", Message: "the kernel has online the threads per core every CPU partition's smt declares"},
	}, {
		name:       "a named partition and the default entry contradicted",
		partitions: []device.Partition{{Name: device.DefaultPartitionName, ThreadsPerCore: 1}, spdk},
		want: &corev1.NodeCondition{Status: corev1.ConditionTrue, Reason: "SiblingThreadsOnline",
			Message: `partitions publish no devices while the kernel contradicts their smt: partition "default" expects 1 online thread per core, and CPUs 4 of its cores are online too; partition "spdk" expects 1 online thread per core, and CPUs 5 of its cores are online too; take those CPUs offline at boot, then restart the driver`},
	}, {
		name:       "smt false on a partition listing both threads of its core",
		partitions: []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5), ThreadsPerCore: 1}},
		want: &corev1.NodeCondition{Status: corev1.ConditionTrue, Reason: "SiblingThreadsOnline",
			Message: `partitions publish no devices while the kernel contradicts their smt: partition "vms" expects 1 online thread per core, and CPUs 5 of its cores are online too; take those CPUs offline at boot, then restart the driver`},
	}} {
		t.Run(tc.name, func(t *testing.T) {
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
				CPUPartitions:    tc.partitions,
			})
			require.NoError(t, err)
			d.nodeConditions.now = func() time.Time { return testClock }

			require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
			node, err := client.CoreV1().Nodes().Get(t.Context(), testNodeName, metav1.GetOptions{})
			require.NoError(t, err)
			_, got := nodeutil.GetNodeCondition(&node.Status, conditionPartitionDegraded)
			if tc.want == nil {
				require.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			require.Equal(t, tc.want.Status, got.Status)
			require.Equal(t, tc.want.Reason, got.Reason)
			require.Equal(t, tc.want.Message, got.Message)
		})
	}
}

// A container without claims runs on the default partition's CPUs less every
// prepared claim's, where upstream's runs on every CPU no claim holds.
func TestContainersWithoutClaimsRunOnTheDefaultPartition(t *testing.T) {
	vms := device.Partition{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)}
	for _, tc := range []struct {
		name     string
		declared []device.Partition
		onDisk   map[types.UID]cpuset.CPUSet
		want     string
	}{{
		name:   "no partitions: every CPU no claim holds",
		onDisk: map[types.UID]cpuset.CPUSet{"vm": cpuset.New(1, 5)},
		want:   "0,2-4",
	}, {
		name:     "partitions: the default partition alone",
		declared: []device.Partition{{Name: device.DefaultPartitionName}, vms},
		want:     "0,2-4",
	}, {
		name:     "partitions: a free exclusive partition stays off limits",
		declared: []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 4)}},
		want:     "1-3,5",
	}, {
		name:     "partitions: a claim restored inside the default partition is left out",
		declared: []device.Partition{{Name: device.DefaultPartitionName}, vms},
		onDisk:   map[types.UID]cpuset.CPUSet{"moved": cpuset.New(2)},
		want:     "0,3-4",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d, err := New(testr.New(t), Providers{
				CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:   partitionSysFS(),
			}, &Config{
				DriverName:       testDriverName,
				NodeName:         testNodeName,
				ReservedCPUs:     cpuset.New(),
				CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
				CPUPartitions:    tc.declared,
			})
			require.NoError(t, err)
			d.cdiMgr = newMockCdiMgrWithAllocations(tc.onDisk)
			d.seedAllocationStoreFromDisk(testr.New(t))

			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "default"}
			updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{{Id: "running", Name: "running", PodSandboxId: pod.Id}})
			require.NoError(t, err)
			require.Len(t, updates, 1)
			require.Equal(t, "running", updates[0].GetContainerId())
			require.Equal(t, tc.want, updates[0].GetLinux().GetResources().GetCpu().GetCpus(), "Synchronize")

			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: "created", Name: "created", PodSandboxId: pod.Id})
			require.NoError(t, err)
			require.Equal(t, tc.want, adjust.GetLinux().GetResources().GetCpu().GetCpus(), "CreateContainer")
		})
	}
}

// On a node whose default partition holds no CPU a container without claims
// is refused, a claim prepared or not, and a container holding a claim starts.
func TestCreateContainerRefusesContainersWithoutClaimsWhereNoDefaultCPUIsLeft(t *testing.T) {
	held := cpuset.New(1, 5)
	noDefaultCPU := []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 1, 2, 3, 4, 5)}}
	refusal := `container "app": no CPU claim, and node "` + testNodeName + `" leaves no CPUs for containers without one; add a claim on an exclusive partition, or run it elsewhere`
	for _, tc := range []struct {
		name       string
		profiled   bool
		partitions []device.Partition
		onDisk     map[types.UID]cpuset.CPUSet
		env        []string
		wantCPUs   string
		wantErr    string
	}{{
		name:       "no claim prepared",
		partitions: noDefaultCPU,
		wantErr:    refusal,
	}, {
		name:       "a claim prepared",
		partitions: noDefaultCPU,
		onDisk:     map[types.UID]cpuset.CPUSet{"vm": held},
		wantErr:    refusal,
	}, {
		name:       "the node's profile named",
		profiled:   true,
		partitions: noDefaultCPU,
		wantErr:    `container "app": no CPU claim, and node "` + testNodeName + `" (profile "r7625") leaves no CPUs for containers without one; add a claim on an exclusive partition, or run it elsewhere`,
	}, {
		name:       "a container holding a claim starts",
		partitions: noDefaultCPU,
		onDisk:     map[types.UID]cpuset.CPUSet{"vm": held},
		env:        []string{fmt.Sprintf("%s_vm=%s", cdiEnvVarPrefix, held)},
		wantCPUs:   "1,5",
	}, {
		name:       "a default partition holding CPUs",
		partitions: []device.Partition{{Name: device.DefaultPartitionName}, {Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: held}},
		wantCPUs:   "0,2-4",
	}, {
		name:     "no partitions",
		wantCPUs: "0-5",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			var d *CPUDriver
			if tc.profiled {
				client := fake.NewClientset(labelledNode(map[string]string{driverconfig.ProfileLabel: "r7625"}))
				d = partitionedProfiledDriver(t, client, false, tc.onDisk, tc.partitions...)
				_, err := d.Synchronize(ctx, nil, nil)
				require.NoError(t, err)
				_, err = d.configureFromProfile(ctx, make(chan error))
				require.NoError(t, err)
			} else {
				var err error
				d, err = New(testr.New(t), Providers{
					CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
					SysFS:   partitionSysFS(),
				}, &Config{
					DriverName:       testDriverName,
					NodeName:         testNodeName,
					ReservedCPUs:     cpuset.New(),
					CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
					CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
					CPUPartitions:    tc.partitions,
				})
				require.NoError(t, err)
				d.cdiMgr = newMockCdiMgrWithAllocations(tc.onDisk)
				d.seedAllocationStoreFromDisk(testr.New(t))
			}

			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "default"}
			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: tc.env})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Empty(t, d.podState.GetContainersWithSharedCPUs(), "a refused container is not tracked")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantCPUs, adjust.GetLinux().GetResources().GetCpu().GetCpus())
		})
	}
}

// sharingDriver is a driver on mixedCoreCPUInfos' node whose default partition
// is the shared device cpudevnuma000 over 0,2-4, beside the exclusive partition
// vms on 1,5. Claims on disk are seeded before it starts.
func sharingDriver(t *testing.T, fractional bool, onDisk map[types.UID]cdiClaimRecord, recorder Recorder) *CPUDriver {
	t.Helper()
	d, err := New(testr.New(t), Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
		SysFS:   partitionSysFS(),
	}, &Config{
		DriverName:                      testDriverName,
		NodeName:                        testNodeName,
		ReservedCPUs:                    cpuset.New(),
		CPUDeviceMode:                   device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:                device.GROUP_BY_NUMA_NODE,
		AllowFractionalCapacityRequests: fractional,
		Metrics:                         recorder,
		CPUPartitions: []device.Partition{
			{Name: device.DefaultPartitionName},
			{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
		},
	})
	require.NoError(t, err)
	cdi := newMockCdiMgr()
	maps.Copy(cdi.records, onDisk)
	d.cdiMgr = cdi
	d.seedAllocationStoreFromDisk(testr.New(t))
	d.hasSynchronized = true
	return d
}

// claimResult is one allocation result of a test claim: the request, the
// amount its spec writes, if any, and what the scheduler booked on device.
type claimResult struct {
	request  string
	written  string
	consumed string
	device   string
}

func claimWithResults(uid types.UID, results ...claimResult) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: uid, Name: string(uid), Namespace: "vms"}}
	claim.Status.Allocation = &resourceapi.AllocationResult{}
	written := map[string]bool{}
	for _, result := range results {
		claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results, resourceapi.DeviceRequestAllocationResult{
			Driver:           testDriverName,
			Pool:             testNodeName,
			Device:           result.device,
			Request:          result.request,
			ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse(result.consumed)},
		})
		if written[result.request] {
			continue
		}
		written[result.request] = true
		var capacity *resourceapi.CapacityRequirements
		if result.written != "" {
			capacity = &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse(result.written)}}
		}
		request := resourceapi.DeviceRequest{Name: result.request, Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: "dra.cpu", Capacity: capacity}}
		if parent, sub, ok := strings.Cut(result.request, "/"); ok {
			request = resourceapi.DeviceRequest{Name: parent, FirstAvailable: []resourceapi.DeviceSubRequest{
				{Name: "other", DeviceClassName: "dra.cpu.vms"},
				{Name: sub, DeviceClassName: "dra.cpu", Capacity: capacity},
			}}
		}
		claim.Spec.Devices.Requests = append(claim.Spec.Devices.Requests, request)
	}
	return claim
}

func shareOnDisk(cpus cpuset.CPUSet, devices ...string) cdiClaimRecord {
	request := cdiRequestRecord{Request: "vmm-cpu", CPUs: cpus, Role: store.RoleShared}
	for _, name := range devices {
		request.Results = append(request.Results, cdiResultRecord{Device: name})
	}
	return cdiClaimRecord{Namespace: "vms", Name: "shared", Requests: []cdiRequestRecord{request}}
}

func exclusiveOnDisk(cpus cpuset.CPUSet) cdiClaimRecord {
	return cdiClaimRecord{Namespace: "vms", Name: "exclusive", Requests: []cdiRequestRecord{{Request: "vcpu", CPUs: cpus, Results: []cdiResultRecord{{Device: "cpudevnuma000-vms"}}}}}
}

// A request on a shared device books an amount and reserves no CPU: its
// metadata file names no CPUs and counts none, and carries the booked amount.
func TestPrepareRecordsASharedRequest(t *testing.T) {
	for _, tc := range []struct {
		name        string
		fractional  bool
		result      claimResult
		wantErr     string
		wantRequest string
	}{{
		name:        "an amount booked as written",
		fractional:  true,
		result:      claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"},
		wantRequest: "250m",
	}, {
		name:        "no amount books nothing",
		result:      claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"},
		wantRequest: "0",
	}, {
		name:    "a fractional amount rounded up where the option is off",
		result:  claimResult{request: "cpus", written: "500m", consumed: "1", device: "cpudevnuma000"},
		wantErr: `request "cpus": requested 500m, allocated 1; fractional amounts are disabled on this node (allowFractionalCapacityRequests: false)`,
	}, {
		name:        "an alternative's amount, recorded under its request",
		fractional:  true,
		result:      claimResult{request: "vmm-cpu/shared", written: "500m", consumed: "500m", device: "cpudevnuma000"},
		wantRequest: "500m",
	}, {
		name:       "an alternative's amount rounded up",
		fractional: true,
		result:     claimResult{request: "vmm-cpu/shared", written: "500m", consumed: "1", device: "cpudevnuma000"},
		wantErr:    `request "vmm-cpu/shared": requested 500m, allocated 1; the scheduler needs the DRAFractionalCapacityRange feature gate`,
	}, {
		name:       "a fractional amount rounded up by a scheduler without the gate",
		fractional: true,
		result:     claimResult{request: "vmm-cpu", written: "250m", consumed: "1", device: "cpudevnuma000"},
		wantErr:    `request "vmm-cpu": requested 250m, allocated 1; the scheduler needs the DRAFractionalCapacityRange feature gate`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, tc.fractional, nil, nil)
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claimWithResults("share", tc.result)})
			require.NoError(t, err)
			if tc.wantErr != "" {
				require.EqualError(t, results["share"].Err, tc.wantErr)
				_, ok := d.allocationState.GetClaimRecord("share")
				require.False(t, ok, "a refused claim leaves no record")
				return
			}
			require.NoError(t, results["share"].Err)
			record, ok := d.allocationState.GetClaimRecord("share")
			require.True(t, ok)
			base, _, _ := strings.Cut(tc.result.request, "/")
			require.Equal(t, []store.RequestAllocation{{Request: base, CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Devices: []string{"cpudevnuma000"}}}, record.Requests)
			require.True(t, d.allocationState.GetPreparedCPUs().IsEmpty(), "a share reserves no CPU")
			require.Truef(t, cpuset.New(0, 2, 3, 4).Equals(d.allocationState.GetClaimlessCPUs()), "containers without claims keep the default partition, got %s", d.allocationState.GetClaimlessCPUs())

			require.Len(t, results["share"].Devices, 1)
			attrs := results["share"].Devices[0].Metadata.Attributes
			require.NotContains(t, attrs, string(device.AttributeCPUSet))
			require.NotContains(t, attrs, string(device.AttributeAllocatedNumCPUs))
			require.Equal(t, tc.wantRequest, *attrs[string(device.AttributeCPURequest)].StringValue)
			require.Equal(t, "max", *attrs[string(device.AttributeCPULimit)].StringValue)

			written := d.cdiMgr.(*mockCdiMgr).records["share"]
			require.Equal(t, store.RoleShared, written.Requests[0].Role, "the CDI record keeps the role")
		})
	}
}

// A request whose results lie on a shared device and an owned one is prepared
// by each result's role: exclusive CPUs from the owned device and a share of
// the shared one, recorded apart and restored after a restart.
func TestPrepareAMixedRequest(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	mixed := claimWithResults("mixed",
		claimResult{request: "vmm-cpu", consumed: "2", device: "cpudevnuma000-vms"},
		claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"})
	wantRecord := []store.RequestAllocation{
		{Request: "vmm-cpu", CPUs: cpuset.New(1, 5)},
		{Request: "vmm-cpu", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Devices: []string{"cpudevnuma000"}},
	}
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	ctr := &api.Container{Id: "vm", Name: "vm", PodSandboxId: pod.Id, Env: []string{fmt.Sprintf("%s_mixed_vmm-cpu=%s", cdiEnvVarPrefix, cdiEnvDynamicValue)}}
	requireContainer := func(t *testing.T, d *CPUDriver) {
		t.Helper()
		adjust, _, err := d.CreateContainer(ctx, pod, ctr)
		require.NoError(t, err)
		require.Equal(t, "0-5", adjust.GetLinux().GetResources().GetCpu().GetCpus(), "the exclusive CPUs and the shared device's")
		require.Contains(t, adjust.GetEnv(), &api.KeyValue{Key: dracpuapi.EnvVarExclusiveAssignedCPUSet, Value: "1,5"})
	}

	d := sharingDriver(t, true, nil, nil)
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{mixed})
	require.NoError(t, err)
	require.NoError(t, results["mixed"].Err)
	record, ok := d.allocationState.GetClaimRecord("mixed")
	require.True(t, ok)
	require.Equal(t, wantRecord, record.Requests)
	require.Truef(t, cpuset.New(1, 5).Equals(d.allocationState.GetPreparedCPUs()), "only the exclusive CPUs are reserved, got %s", d.allocationState.GetPreparedCPUs())

	require.Len(t, results["mixed"].Devices, 2)
	for _, prepared := range results["mixed"].Devices {
		attrs := prepared.Metadata.Attributes
		switch prepared.DeviceName {
		case "cpudevnuma000-vms":
			require.Equal(t, "1,5", *attrs[string(device.AttributeCPUSet)].StringValue)
			require.Equal(t, int64(2), *attrs[string(device.AttributeAllocatedNumCPUs)].IntValue)
		case "cpudevnuma000":
			require.NotContains(t, attrs, string(device.AttributeCPUSet))
			require.NotContains(t, attrs, string(device.AttributeAllocatedNumCPUs))
			require.Equal(t, "0", *attrs[string(device.AttributeCPURequest)].StringValue)
		default:
			t.Fatalf("unexpected device %s", prepared.DeviceName)
		}
		require.Len(t, prepared.CDIDeviceIDs, 1)
	}
	require.Equal(t, results["mixed"].Devices[0].CDIDeviceIDs, results["mixed"].Devices[1].CDIDeviceIDs, "one CDI device per request")
	requireContainer(t, d)

	written := d.cdiMgr.(*mockCdiMgr).records["mixed"]
	encoded, err := encodeClaimRecord(written)
	require.NoError(t, err)
	decoded, err := decodeClaimRecord(encoded)
	require.NoError(t, err)
	restarted := sharingDriver(t, true, map[types.UID]cdiClaimRecord{"mixed": decoded}, nil)
	record, ok = restarted.allocationState.GetClaimRecord("mixed")
	require.True(t, ok)
	require.Equal(t, wantRecord, record.Requests, "restored")
	requireContainer(t, restarted)
}

// An exclusive result books whole CPUs, so a fractional written amount is
// refused on it, with partitions or without, a mixed request's shared half
// alone staying valid.
func TestPrepareRefusesAFractionalAmountOnAnExclusiveResult(t *testing.T) {
	noPartitions := func(t *testing.T) *CPUDriver {
		t.Helper()
		d, err := New(testr.New(t), Providers{
			CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
			SysFS:   partitionSysFS(),
		}, &Config{
			DriverName:       testDriverName,
			NodeName:         testNodeName,
			ReservedCPUs:     cpuset.New(),
			CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
			CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
		})
		require.NoError(t, err)
		d.cdiMgr = newMockCdiMgr()
		d.hasSynchronized = true
		return d
	}
	withPartitions := func(t *testing.T) *CPUDriver {
		t.Helper()
		return sharingDriver(t, true, nil, nil)
	}
	wholeCores := func(t *testing.T) *CPUDriver {
		t.Helper()
		d, err := New(testr.New(t), Providers{
			CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: wholeCoreCPUInfos},
			SysFS:   testSysFS(wholeCoreCPUInfos),
		}, &Config{
			DriverName:           testDriverName,
			NodeName:             testNodeName,
			ReservedCPUs:         cpuset.New(),
			CPUDeviceMode:        device.CPU_DEVICE_MODE_GROUPED,
			CPUDeviceGroupBy:     device.GROUP_BY_NUMA_NODE,
			FullPhysicalCPUsOnly: true,
		})
		require.NoError(t, err)
		d.cdiMgr = newMockCdiMgr()
		d.hasSynchronized = true
		return d
	}
	for _, tc := range []struct {
		name    string
		driver  func(*testing.T) *CPUDriver
		results []claimResult
		wantErr string
	}{{
		name:   "a mixed request with a fractional amount",
		driver: withPartitions,
		results: []claimResult{
			{request: "vmm-cpu", written: "500m", consumed: "500m", device: "cpudevnuma000"},
			{request: "vmm-cpu", consumed: "1", device: "cpudevnuma000-vms"},
		},
		wantErr: `request "vmm-cpu": 500m is not a whole number of CPUs (device "cpudevnuma000-vms" of partition "vms")`,
	}, {
		name:    "its shared half alone",
		driver:  withPartitions,
		results: []claimResult{{request: "vmm-cpu", written: "500m", consumed: "500m", device: "cpudevnuma000"}},
	}, {
		name:    "an exclusive request with a fractional amount on a partitioned node",
		driver:  withPartitions,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "500m", device: "cpudevnuma000-vms"}},
		wantErr: `request "cpus": 500m is not a whole number of CPUs (device "cpudevnuma000-vms" of partition "vms")`,
	}, {
		name:    "an exclusive request with a whole amount on a partitioned node",
		driver:  withPartitions,
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000-vms"}},
	}, {
		name:    "an exclusive request with a fractional amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus", written: "1500m", consumed: "1500m", device: "cpudevnuma000"}},
		wantErr: `request "cpus": 1500m is not a whole number of CPUs (class dra.cpu, no cpuPartitions)`,
	}, {
		name:    "an alternative with a fractional amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus/whole", written: "500m", consumed: "500m", device: "cpudevnuma000"}},
		wantErr: `request "cpus/whole": 500m is not a whole number of CPUs (class dra.cpu, no cpuPartitions)`,
	}, {
		name:    "an exclusive request with a whole amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma000"}},
	}, {
		name:    "a fractional amount the scheduler rounded up to whole cores",
		driver:  wholeCores,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "2", device: "cpudevnuma000"}},
		wantErr: `request "cpus": 500m is not a whole number of CPUs (class dra.cpu, no cpuPartitions)`,
	}, {
		name:    "a whole amount the scheduler rounded up to whole cores",
		driver:  wholeCores,
		results: []claimResult{{request: "cpus", written: "3", consumed: "4", device: "cpudevnuma000"}},
		wantErr: `request "cpus": requested 3, allocated 4; its device gives whole cores of 2 threads`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := tc.driver(t)
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claimWithResults("claim", tc.results...)})
			require.NoError(t, err)
			_, recorded := d.allocationState.GetClaimRecord("claim")
			if tc.wantErr != "" {
				require.EqualError(t, results["claim"].Err, tc.wantErr)
				require.False(t, recorded, "a refused claim leaves no record")
				return
			}
			require.NoError(t, results["claim"].Err)
			require.True(t, recorded)
		})
	}
}

// A share recorded on CPUs its device no longer shares, the partitions having
// changed across a restart, is refused when the kubelet prepares it again.
func TestPrepareRefusesAShareRecordedOnCPUsItsDeviceNoLongerShares(t *testing.T) {
	share := claimWithResults("share", claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"})
	for _, tc := range []struct {
		name    string
		onDisk  cdiClaimRecord
		wantErr string
	}{{
		name:   "recorded on the device's CPUs",
		onDisk: shareOnDisk(cpuset.New(0, 2, 3, 4), "cpudevnuma000"),
	}, {
		name:    "recorded on CPUs the exclusive partition took since",
		onDisk:  shareOnDisk(cpuset.New(0, 1, 2, 3, 4, 5), "cpudevnuma000"),
		wantErr: `claim vms/share: request "vmm-cpu" was prepared on CPUs 0-5, and CPUs 1,5 of them have left its devices since the node's partitions changed; delete the pods using the claim so that it is allocated again`,
	}, {
		name:    "recorded on a device no longer published",
		onDisk:  shareOnDisk(cpuset.New(0, 2), "cpudevnuma001"),
		wantErr: `claim vms/share: request "vmm-cpu" was prepared on device "cpudevnuma001", which this node no longer publishes as shared since its partitions changed; delete the pods using the claim so that it is allocated again`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, false, map[types.UID]cdiClaimRecord{"share": tc.onDisk}, nil)
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{share})
			require.NoError(t, err)
			if tc.wantErr == "" {
				require.NoError(t, results["share"].Err)
				return
			}
			require.EqualError(t, results["share"].Err, tc.wantErr)
			_, ok := d.allocationState.GetClaimRecord("share")
			require.True(t, ok, "the refusal keeps the record, and its containers their CPUs")
		})
	}
}

// A container holding a share runs on its device's CPUs less every CPU an
// exclusive request holds, and is told its exclusive CPUs alone.
func TestCreateContainerRunsASharedRequestOnItsDevice(t *testing.T) {
	moved := map[types.UID]cdiClaimRecord{"moved": exclusiveOnDisk(cpuset.New(2))}
	for _, tc := range []struct {
		name          string
		results       []claimResult
		wantCPUs      string
		wantExclusive string
		wantLabel     cpumetrics.CPUAllocation
	}{{
		name:      "a share alone",
		results:   []claimResult{{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}},
		wantCPUs:  "0,3-4",
		wantLabel: cpumetrics.CPUAllocationShared,
	}, {
		name: "a share beside exclusive CPUs",
		results: []claimResult{
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"},
		},
		wantCPUs:      "0-1,3-5",
		wantExclusive: "1,5",
		wantLabel:     cpumetrics.CPUAllocationExclusive,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			reg := prometheus.NewRegistry()
			d := sharingDriver(t, false, moved, cpumetrics.New(reg))
			claim := claimWithResults("vm", tc.results...)
			results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			require.NoError(t, results["vm"].Err)

			var env []string
			for _, result := range tc.results {
				env = append(env, fmt.Sprintf("%s_vm_%s=%s", cdiEnvVarPrefix, result.request, cdiEnvDynamicValue))
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
			before := metricValue(t, reg, "dra_cpu_nri_create_container_duration_seconds", map[string]string{"result": "success", "cpu_allocation_mode": tc.wantLabel.String()})
			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Env: env})
			require.NoError(t, err)
			require.Equal(t, tc.wantCPUs, adjust.GetLinux().GetResources().GetCpu().GetCpus())
			var exclusive []string
			for _, kv := range adjust.GetEnv() {
				if kv.GetKey() == dracpuapi.EnvVarExclusiveAssignedCPUSet {
					exclusive = append(exclusive, kv.GetValue())
				}
			}
			if tc.wantExclusive == "" {
				require.Empty(t, exclusive, "a container holding no exclusive CPU is told none")
			} else {
				require.Equal(t, []string{tc.wantExclusive}, exclusive)
			}
			after := metricValue(t, reg, "dra_cpu_nri_create_container_duration_seconds", map[string]string{"result": "success", "cpu_allocation_mode": tc.wantLabel.String()})
			require.Equal(t, before+1, after, "the container is counted by the role of its requests")
		})
	}
}

// Synchronize gives a running share holder its share computed from the device
// as published now, the default partition's CPUs where its device is gone, and
// leaves it where the runtime started it where that set is empty.
func TestSynchronizeConvergesASharedRequest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		onDisk map[types.UID]cdiClaimRecord
		want   map[string]string
	}{{
		name:   "the device's CPUs less exclusive ones",
		onDisk: map[types.UID]cdiClaimRecord{"vm": shareOnDisk(cpuset.New(0, 1, 2, 3, 4, 5), "cpudevnuma000"), "moved": exclusiveOnDisk(cpuset.New(3))},
		want:   map[string]string{"vm": "0,2,4", "other": "0,2,4"},
	}, {
		name:   "the default partition where the device is gone",
		onDisk: map[types.UID]cdiClaimRecord{"vm": shareOnDisk(cpuset.New(1, 5), "cpudevnuma001")},
		want:   map[string]string{"vm": "0,2-4", "other": "0,2-4"},
	}, {
		name:   "no update where no CPU is left",
		onDisk: map[types.UID]cdiClaimRecord{"vm": shareOnDisk(cpuset.New(0, 2, 3, 4), "cpudevnuma000"), "moved": exclusiveOnDisk(cpuset.New(0, 2, 3, 4))},
		want:   map[string]string{},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, false, tc.onDisk, nil)
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
			containers := []*api.Container{
				{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Env: []string{fmt.Sprintf("%s_vm_vmm-cpu=%s", cdiEnvVarPrefix, cdiEnvDynamicValue)}},
				{Id: "other", Name: "sidecar", PodSandboxId: pod.Id},
			}
			updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, containers)
			require.NoError(t, err)
			got := map[string]string{}
			for _, update := range updates {
				got[update.GetContainerId()] = update.GetLinux().GetResources().GetCpu().GetCpus()
			}
			require.Equal(t, tc.want, got)
		})
	}
}
