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
	"cmp"
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	dracpuapi "github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/admission"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
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
	threeThreadCore := cpuinfo.CPUDetails{
		0: {CpuID: 0, CoreID: 0}, 1: {CpuID: 1, CoreID: 0}, 2: {CpuID: 2, CoreID: 0}, 3: {CpuID: 3, CoreID: 1},
	}

	for _, tc := range []struct {
		name     string
		details  cpuinfo.CPUDetails // nil for mixedCoreCPUInfos'
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
			wantErr:  `partition "vms" lists CPUs 9, which this node does not have: remove them from its cpuset`,
		},
		{
			name:     "an offline CPU",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 2, 6)},
			wantErr:  `partition "spdk" lists offline CPUs 6: bring them online, or remove them from its cpuset`,
		},
		{
			name:     "an online CPU outside the topology",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 3, 8)},
			wantErr:  `partition "vms" lists CPUs 8 with incomplete topology (the kernel reports no socket, core or NUMA node for them)`,
		},
		{
			name:     "two smt false partitions naming the two threads of one core",
			declared: []device.Partition{partition("a", 1, 0), defaultEntry, partition("b", 1, 4)},
			wantErr:  `partitions "a" and "b" split one core between them (CPU 0 in "a", CPU 4 in "b"): put both threads in one partition`,
		},
		{
			name:     "a core split with the default partition",
			declared: []device.Partition{defaultEntry, partition("vms", 0, 0, 1, 4)},
			wantErr:  `partition "vms" lists CPUs 1 without their sibling threads 5: list every thread of each core`,
		},
		{
			name:     "an smt false partition whose sibling is online is left to the smt check",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 1)},
		},
		{
			name:     "a partition without smt whose sibling is online",
			declared: []device.Partition{defaultEntry, partition("spdk", 0, 1)},
			wantErr:  `partition "spdk" lists CPUs 1 without their sibling threads 5: list every thread of each core`,
		},
		{
			name:     "an smt false partition listing both threads of a core",
			declared: []device.Partition{defaultEntry, partition("vms", 1, 1, 5)},
			wantErr:  "partition \"vms\" lists CPUs 1,5 of one core under `smt: false`: list only the first thread of each core",
		},
		{
			name:     "an smt false partition listing both threads of a core beside one whose sibling is online",
			declared: []device.Partition{defaultEntry, partition("vms", 1, 0, 1, 5)},
			wantErr:  "partition \"vms\" lists CPUs 1,5 of one core under `smt: false`: list only the first thread of each core",
		},
		{
			name:     "an smt 2 partition listing both threads of a core",
			declared: []device.Partition{defaultEntry, partition("vms", 2, 1, 5)},
		},
		{
			name:     "an smt 2 partition listing three threads of a core",
			details:  threeThreadCore,
			declared: []device.Partition{defaultEntry, partition("vms", 2, 0, 1, 2)},
			wantErr:  "partition \"vms\" lists CPUs 0-2 of one core under `smt: 2`: list only the first 2 threads of each core",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			details := topo.CPUDetails
			if tc.details != nil {
				details = tc.details
			}
			err := validatePartitions(details, present, online, tc.declared)
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
			wantErr:  `partition "vms" lists offline CPUs 7: bring them online, or remove them from its cpuset`,
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
				K8SClient: statusClient(),
				CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:     tc.sysfs,
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
				K8SClient: statusClient(),
				CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:     partitionSysFS(),
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
				K8SClient: statusClient(),
				CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:     partitionSysFS(),
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
				K8SClient: statusClient(),
				CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
				SysFS:     partitionSysFS(),
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
					K8SClient: statusClient(),
					CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
					SysFS:     partitionSysFS(),
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
		K8SClient: statusClient(),
		CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos},
		SysFS:     partitionSysFS(),
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
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: uid, Name: string(uid), Namespace: "vms", ResourceVersion: "1"}}
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
		wantCap     int64
		wantLimit   string
	}{{
		name:        "an amount booked as written and capped at",
		fractional:  true,
		result:      claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"},
		wantRequest: "250m",
		wantCap:     250,
		wantLimit:   "250m",
	}, {
		name:        "a whole amount capped at whole CPUs",
		result:      claimResult{request: "vmm-cpu", written: "2", consumed: "2", device: "cpudevnuma000"},
		wantRequest: "2",
		wantCap:     2000,
		wantLimit:   "2",
	}, {
		name:        "no amount books nothing and caps nothing",
		result:      claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"},
		wantRequest: "0",
		wantLimit:   "max",
	}, {
		name:        "an amount of 0 caps nothing",
		result:      claimResult{request: "vmm-cpu", written: "0", consumed: "0", device: "cpudevnuma000"},
		wantRequest: "0",
		wantLimit:   "max",
	}, {
		name:    "a fractional amount rounded up where the option is off",
		result:  claimResult{request: "cpus", written: "500m", consumed: "1", device: "cpudevnuma000"},
		wantErr: "request \"cpus\": requested 500m, allocated 1; fractional amounts are disabled on this node (`allowFractionalCapacityRequests: false`)",
	}, {
		name:    "an amount below 0",
		result:  claimResult{request: "cpus", written: "-1", consumed: "0", device: "cpudevnuma000"},
		wantErr: `request "cpus": requested -1; ask for at least 0`,
	}, {
		name:        "an alternative's amount, recorded under its request",
		fractional:  true,
		result:      claimResult{request: "vmm-cpu/shared", written: "500m", consumed: "500m", device: "cpudevnuma000"},
		wantRequest: "500m",
		wantCap:     500,
		wantLimit:   "500m",
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
			require.Equal(t, []store.RequestAllocation{{Request: base, CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Devices: []string{"cpudevnuma000"}, Cap: tc.wantCap}}, record.Requests)
			require.True(t, d.allocationState.GetPreparedCPUs().IsEmpty(), "a share reserves no CPU")
			require.Truef(t, cpuset.New(0, 2, 3, 4).Equals(d.allocationState.GetClaimlessCPUs()), "containers without claims keep the default partition, got %s", d.allocationState.GetClaimlessCPUs())

			require.Len(t, results["share"].Devices, 1)
			attrs := results["share"].Devices[0].Metadata.Attributes
			require.NotContains(t, attrs, string(device.AttributeCPUSet))
			require.NotContains(t, attrs, string(device.AttributeAllocatedNumCPUs))
			require.Equal(t, tc.wantRequest, *attrs[string(device.AttributeCPURequest)].StringValue)
			require.Equal(t, tc.wantLimit, *attrs[string(device.AttributeCPULimit)].StringValue)

			written := d.cdiMgr.(*mockCdiMgr).records["share"]
			require.Equal(t, store.RoleShared, written.Requests[0].Role, "the CDI record keeps the role")
			require.Equal(t, tc.wantCap, written.Requests[0].Cap, "the CDI record keeps the cap")
		})
	}
}

// A share capped at its amount takes one device, and is alone in the cgroup of
// the container using its claim, which holds every request of the claim.
func TestPrepareRefusesACappedShareThatIsNotAlone(t *testing.T) {
	spread := func(mode resourceapi.DeviceAllocationMode, count int64) func(*resourceapi.ResourceClaim) {
		return func(claim *resourceapi.ResourceClaim) {
			request := &claim.Spec.Devices.Requests[0]
			if request.Exactly != nil {
				request.Exactly.AllocationMode, request.Exactly.Count = mode, count
				return
			}
			sub := &request.FirstAvailable[len(request.FirstAvailable)-1]
			sub.AllocationMode, sub.Count = mode, count
		}
	}
	for _, tc := range []struct {
		name    string
		results []claimResult
		edit    func(*resourceapi.ResourceClaim)
		wantErr string
	}{{
		name:    "a cap with count 2",
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma000"}},
		edit:    spread(resourceapi.DeviceAllocationModeExactCount, 2),
		wantErr: "request \"cpus\" has limit of 1 with `count: 2`; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name:    "a cap with allocationMode: All on a node where one device matches",
		results: []claimResult{{request: "cpus", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		edit:    spread(resourceapi.DeviceAllocationModeAll, 0),
		wantErr: "request \"cpus\" has limit of 250m with `allocationMode: All`, which may take several devices; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name:    "a capped alternative with count 2",
		results: []claimResult{{request: "cpus/shared", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		edit:    spread(resourceapi.DeviceAllocationModeExactCount, 2),
		wantErr: "subrequest \"cpus/shared\" has limit of 250m with `count: 2`; a limit can be applied to only one device: ask for one device, or remove the limit",
	}, {
		name: "a capped share beside an uncapped one",
		results: []claimResult{
			{request: "helper", consumed: "0", device: "cpudevnuma000"},
			{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"},
		},
		wantErr: `request "vmm-cpu" has limit of 250m, so it cannot be used together with request "helper": remove the limit`,
	}, {
		name: "a capped share beside exclusive CPUs",
		results: []claimResult{
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"},
		},
		wantErr: `request "vmm-cpu" has limit of 250m, so it cannot be used together with request "vcpu": remove the limit`,
	}, {
		name: "a capped share beside several requests, named in the claim's order",
		results: []claimResult{
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"},
			{request: "helper", consumed: "0", device: "cpudevnuma000"},
			{request: "io", consumed: "0", device: "cpudevnuma000"},
		},
		wantErr: `request "vmm-cpu" has limit of 250m, so it cannot be used together with requests "vcpu", "helper", "io": remove the limit`,
	}, {
		name: "a picked capped alternative beside another request",
		results: []claimResult{
			{request: "helper/shared", written: "500m", consumed: "500m", device: "cpudevnuma000"},
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
		},
		wantErr: `subrequest "helper/shared" has limit of 500m, so it cannot be used together with request "vcpu": remove the limit`,
	}, {
		name:    "an uncapped share spanning devices",
		results: []claimResult{{request: "cpus", consumed: "0", device: "cpudevnuma000"}},
		edit:    spread(resourceapi.DeviceAllocationModeExactCount, 2),
	}, {
		name: "an uncapped share beside exclusive CPUs",
		results: []claimResult{
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"},
		},
	}, {
		name:    "a capped share alone",
		results: []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, true, nil, nil)
			claim := claimWithResults("share", tc.results...)
			if tc.edit != nil {
				tc.edit(claim)
			}
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			_, recorded := d.allocationState.GetClaimRecord("share")
			if tc.wantErr != "" {
				require.EqualError(t, results["share"].Err, tc.wantErr)
				require.False(t, recorded, "a refused claim leaves no record")
				return
			}
			require.NoError(t, results["share"].Err)
			require.True(t, recorded)
		})
	}
}

// withConfig gives a test claim an opaque entry of this driver: in its spec
// and, as the scheduler copies it, in its allocation, or in its allocation
// alone where a DeviceClass gives it.
func withConfig(claim *resourceapi.ResourceClaim, source resourceapi.AllocationConfigSource, requests []string, params string) *resourceapi.ResourceClaim {
	config := resourceapi.DeviceConfiguration{Opaque: &resourceapi.OpaqueDeviceConfiguration{Driver: testDriverName, Parameters: runtime.RawExtension{Raw: []byte(params)}}}
	if source == resourceapi.AllocationConfigSourceClaim {
		claim.Spec.Devices.Config = append(claim.Spec.Devices.Config, resourceapi.DeviceClaimConfiguration{Requests: requests, DeviceConfiguration: config})
	}
	claim.Status.Allocation.Devices.Config = append(claim.Status.Allocation.Devices.Config, resourceapi.DeviceAllocationConfiguration{Source: source, Requests: requests, DeviceConfiguration: config})
	return claim
}

func cpuLimitParams(value string) string {
	return `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": ` + value + `}}`
}

// A written limit replaces a share's amount as its cap, recorded and written
// to its metadata file, and the claim checks run at Prepare as at apply.
func TestPrepareAWrittenLimit(t *testing.T) {
	claimSource := resourceapi.AllocationConfigSourceClaim
	vmsClass := func(claim *resourceapi.ResourceClaim) {
		claim.Spec.Devices.Requests[0].Exactly.DeviceClassName = "dra.cpu.vms"
	}
	pickOwnedBesideCappable := func(claim *resourceapi.ResourceClaim) {
		claim.Spec.Devices.Requests[1].FirstAvailable = []resourceapi.DeviceSubRequest{
			{Name: "shared", DeviceClassName: "dra.cpu", Capacity: &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse("500m")}}},
			{Name: "owned", DeviceClassName: "dra.cpu.vms", Capacity: &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse("1")}}},
		}
	}
	chartKinds := admission.ClassKinds(dracpuapi.DriverName, driverconfig.Config{CPUPartitions: []driverconfig.CPUPartition{
		{Name: device.DefaultPartitionName},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE},
	}})
	for _, tc := range []struct {
		name       string
		results    []claimResult
		source     resourceapi.AllocationConfigSource
		limit      string
		edit       func(*resourceapi.ResourceClaim)
		classKinds map[string]admission.Kind
		wantErr    string
		wantCap    int64
		wantLimit  string
	}{{
		name:      "a limit replaces the amount as the cap",
		results:   []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		limit:     `1`,
		wantCap:   1000,
		wantLimit: "1",
	}, {
		name:      "an amount of 0 with a limit",
		results:   []claimResult{{request: "vmm-cpu", written: "0", consumed: "0", device: "cpudevnuma000"}},
		limit:     `"500m"`,
		wantCap:   500,
		wantLimit: "500m",
	}, {
		name:      "limit max caps nothing",
		results:   []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		limit:     `"max"`,
		wantLimit: "max",
	}, {
		name:      "a limit below a millicore is rounded up",
		results:   []claimResult{{request: "vmm-cpu", written: "0", consumed: "0", device: "cpudevnuma000"}},
		limit:     `"500u"`,
		wantCap:   1,
		wantLimit: "1m",
	}, {
		name:    "a limit of 0",
		results: []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		limit:   `0`,
		wantErr: `request "vmm-cpu": invalid limit 0`,
	}, {
		name:    "a limit a DeviceClass gives",
		results: []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}},
		source:  resourceapi.AllocationConfigSourceClass,
		limit:   `"max"`,
		wantErr: `DeviceClass "dra.cpu" sets cpuConfig.limit; set it in the claim`,
	}, {
		name:    "a numeric limit on an exclusive result of a class written by hand",
		results: []claimResult{{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}},
		limit:   `1`,
		wantErr: "request \"vcpu\": limit 1 on exclusive device \"cpudevnuma000-vms\"; only `limit: max` is allowed",
	}, {
		name:       "a numeric limit on an exclusive request of the chart's class",
		results:    []claimResult{{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}},
		limit:      `1`,
		edit:       vmsClass,
		classKinds: chartKinds,
		wantErr:    "request \"vcpu\": limit 1 on exclusive request (class dra.cpu.vms); only `limit: max` is allowed",
	}, {
		name:      "limit max on an exclusive request",
		results:   []claimResult{{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}},
		limit:     `"max"`,
		wantLimit: "max",
	}, {
		name: "every pick judged where the chart's classes are known",
		results: []claimResult{
			{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
			{request: "helper/owned", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
		},
		edit:       func(claim *resourceapi.ResourceClaim) { vmsClass(claim); pickOwnedBesideCappable(claim) },
		classKinds: chartKinds,
		wantErr:    `subrequest "helper/shared" has limit of 500m, so it cannot be used together with request "vcpu": remove the limit`,
	}, {
		name: "the picked alternative alone judged where they are not",
		results: []claimResult{
			{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
			{request: "helper/owned", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
		},
		edit:      func(claim *resourceapi.ResourceClaim) { vmsClass(claim); pickOwnedBesideCappable(claim) },
		wantLimit: "max",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, true, nil, nil)
			d.classKinds = tc.classKinds
			claim := claimWithResults("limited", tc.results...)
			if tc.edit != nil {
				tc.edit(claim)
			}
			if tc.limit != "" {
				source := tc.source
				if source == "" {
					source = claimSource
				}
				withConfig(claim, source, []string{tc.results[0].request}, cpuLimitParams(tc.limit))
			}
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			record, recorded := d.allocationState.GetClaimRecord("limited")
			if tc.wantErr != "" {
				require.EqualError(t, results["limited"].Err, tc.wantErr)
				require.False(t, recorded, "a refused claim leaves no record")
				return
			}
			require.NoError(t, results["limited"].Err)
			require.True(t, recorded)
			require.Equal(t, tc.wantCap, record.Requests[0].Cap)
			require.Equal(t, tc.wantLimit, *results["limited"].Devices[0].Metadata.Attributes[string(device.AttributeCPULimit)].StringValue)
		})
	}
}

// A firstAvailable request's limits are judged on every alternative, so the
// claim is refused whichever one the scheduler allocated.
func TestPrepareJudgesEveryAlternative(t *testing.T) {
	alternatives := func(oneAmount string) []resourceapi.DeviceSubRequest {
		amount := func(value string) *resourceapi.CapacityRequirements {
			if value == "" {
				return nil
			}
			return &resourceapi.CapacityRequirements{Requests: map[resourceapi.QualifiedName]resource.Quantity{device.CPUResourceQualifiedName: resource.MustParse(value)}}
		}
		return []resourceapi.DeviceSubRequest{
			{Name: "two", DeviceClassName: dracpuapi.DriverName, Capacity: amount("250m")},
			{Name: "one", DeviceClassName: dracpuapi.DriverName, Capacity: amount(oneAmount)},
		}
	}
	type entry struct {
		requests []string
		limit    string
	}
	for _, tc := range []struct {
		name      string
		oneAmount string
		entries   []entry
		wantErr   string
	}{{
		name:      "WA29: a parent entry and an alternative's entry both giving a limit",
		oneAmount: "500m",
		entries:   []entry{{requests: []string{"cpus/two"}, limit: `"500m"`}, {requests: []string{"cpus"}, limit: `"1"`}},
		wantErr:   `subrequest "cpus/two": cpuConfig.limit set by two config entries (#0 and #1); set it in one entry`,
	}, {
		name:    "WA30: a parent limit over an alternative without an amount",
		entries: []entry{{requests: []string{"cpus"}, limit: `"500m"`}},
		wantErr: `subrequest "cpus/one": limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero`,
	}} {
		for _, picked := range []claimResult{
			{request: "cpus/two", written: "250m", consumed: "250m", device: "cpudevnuma000"},
			{request: "cpus/one", written: tc.oneAmount, consumed: cmp.Or(tc.oneAmount, "0"), device: "cpudevnuma000"},
		} {
			t.Run(tc.name+", "+picked.request+" allocated", func(t *testing.T) {
				d := sharingDriver(t, true, nil, nil)
				d.classKinds = admission.ClassKinds(dracpuapi.DriverName, driverconfig.Config{CPUPartitions: []driverconfig.CPUPartition{{Name: device.DefaultPartitionName}}})
				claim := claimWithResults("alternatives", picked)
				claim.Spec.Devices.Requests[0].FirstAvailable = alternatives(tc.oneAmount)
				for _, e := range tc.entries {
					withConfig(claim, resourceapi.AllocationConfigSourceClaim, e.requests, cpuLimitParams(e.limit))
				}
				results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
				require.NoError(t, err)
				require.EqualError(t, results["alternatives"].Err, tc.wantErr)
				_, recorded := d.allocationState.GetClaimRecord("alternatives")
				require.False(t, recorded, "a refused claim leaves no record")
			})
		}
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

const exclusiveParams = `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true}}`

// allowingDriver is a driver on mixedCoreCPUInfos whose default entry, CPUs
// 0 and 4, one core, and 2 and 3, a core each, sets allowExclusive to allow.
func allowingDriver(t *testing.T, allow, fullPhysicalCPUsOnly bool, onDisk map[types.UID]cdiClaimRecord) *CPUDriver {
	t.Helper()
	return partitionDriver(t, mixedCoreCPUInfos, []device.Partition{
		{Name: device.DefaultPartitionName, AllowExclusive: allow},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
	}, fullPhysicalCPUsOnly, onDisk)
}

// A request with `cpuConfig.exclusive: true` takes whole CPUs of its own out
// of a shared device whose partition allows it, from those no exclusive
// request holds, earlier results of its own claim included, up to every CPU of
// the device.
func TestPrepareTakesExclusiveCPUsFromADefaultPartition(t *testing.T) {
	holdingTwoAndThree := map[types.UID]cdiClaimRecord{"held": {Namespace: "vms", Name: "held", Requests: []cdiRequestRecord{
		{Request: "cpus", CPUs: cpuset.New(2, 3), Results: []cdiResultRecord{{Device: "cpudevnuma000"}}},
	}}}
	for _, tc := range []struct {
		name           string
		disallowed     bool
		whole          bool
		twoThreadCores bool
		onDisk         map[types.UID]cdiClaimRecord
		results        []claimResult
		flagged        []string
		params         string
		source         resourceapi.AllocationConfigSource
		want           []store.RequestAllocation
		wantErr        string
	}{{
		name:    "two CPUs",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(2, 3)}},
	}, {
		name:    "three, leaving one CPU to share",
		results: []claimResult{{request: "cpus", written: "3", consumed: "3", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 2, 3)}},
	}, {
		name:    "every CPU of the device",
		results: []claimResult{{request: "cpus", written: "4", consumed: "4", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 2, 3, 4)}},
	}, {
		name:    "the last CPUs of the device, after an earlier request of the claim",
		results: []claimResult{{request: "a", written: "2", consumed: "2", device: "cpudevnuma000"}, {request: "b", written: "2", consumed: "2", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "a", CPUs: cpuset.New(2, 3)}, {Request: "b", CPUs: cpuset.New(0, 4)}},
	}, {
		name:    "two requests of one claim on one device",
		results: []claimResult{{request: "a", written: "2", consumed: "2", device: "cpudevnuma000"}, {request: "b", written: "1", consumed: "1", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "a", CPUs: cpuset.New(2, 3)}, {Request: "b", CPUs: cpuset.New(0)}},
	}, {
		name:    "a share beside a flagged request on one device",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}, {request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}},
		flagged: []string{"cpus"},
		want: []store.RequestAllocation{
			{Request: "cpus", CPUs: cpuset.New(2, 3)},
			{Request: "vmm-cpu", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Devices: []string{"cpudevnuma000"}},
		},
	}, {
		name:    "a CPU another claim holds is not taken",
		onDisk:  holdingTwoAndThree,
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0)}},
	}, {
		name:    "every CPU another claim does not hold",
		onDisk:  holdingTwoAndThree,
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 4)}},
	}, {
		name:    "whole cores of either size under fullPhysicalCPUsOnly",
		whole:   true,
		results: []claimResult{{request: "cpus", written: "3", consumed: "3", device: "cpudevnuma000"}},
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 2, 4)}},
	}, {
		name:    "WC14: an odd amount while no core of one thread is free",
		whole:   true,
		onDisk:  holdingTwoAndThree,
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": 1 exclusive CPUs need a free core of 1 thread on device \"cpudevnuma000\", which has none (1 free cores of 2 threads; `fullPhysicalCPUsOnly: true`): claims holding the device's cores of 1 thread: claim vms/held (UID \"held\") holds CPUs 2-3, bound to no container; see \"CPUs Held by Another Claim\" in the driver's troubleshooting guide",
	}, {
		name:       "a partition that does not allow it",
		disallowed: true,
		results:    []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		wantErr:    "request \"cpus\": partition \"default\" on node \"" + testNodeName + "\" does not allow exclusive CPUs (`allowExclusive: false`)",
	}, {
		name:    "no amount",
		results: []claimResult{{request: "cpus", consumed: "0", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": no amount on exclusive request (`cpuConfig.exclusive: true`); ask for at least 1",
	}, {
		name:           "no amount on cores of 2 threads under fullPhysicalCPUsOnly",
		twoThreadCores: true,
		results:        []claimResult{{request: "cpus", consumed: "0", device: "cpudevnuma000"}},
		wantErr:        "request \"cpus\": no amount on exclusive request (`cpuConfig.exclusive: true`); ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
	}, {
		name:    "an amount of 0",
		results: []claimResult{{request: "cpus", written: "0", consumed: "0", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": amount 0 on exclusive request (`cpuConfig.exclusive: true`); ask for at least 1",
	}, {
		name:    "WA25: a fractional amount",
		results: []claimResult{{request: "cpus", written: "1500m", consumed: "2", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": amount 1500m on exclusive request (`cpuConfig.exclusive: true`); ask for a whole number of CPUs",
	}, {
		name:    "WA26: a numeric limit",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		params:  `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true, "limit": "2"}}`,
		wantErr: "request \"cpus\": limit 2 on exclusive request (`cpuConfig.exclusive: true`); only `limit: max` is allowed",
	}, {
		name:    "limit max",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		params:  `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true, "limit": "max"}}`,
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(2, 3)}},
	}, {
		name:    "WB24: a cpuset, which the default allocator does not take",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		params:  `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": true, "cpuset": "0,4"}}`,
		wantErr: `request "cpus": cpuConfig.cpuset is not supported with cpuPartitions; the default allocator chooses the CPUs itself`,
	}, {
		name:    "the flag from a DeviceClass",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		source:  resourceapi.AllocationConfigSourceClass,
		wantErr: `DeviceClass "dra.cpu" sets cpuConfig.exclusive; set it in the claim`,
	}, {
		name:    "exclusive false is a share",
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}},
		params:  `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": false}}`,
		want:    []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Devices: []string{"cpudevnuma000"}, Cap: 2000}},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := allowingDriver(t, !tc.disallowed, tc.whole, tc.onDisk)
			if tc.twoThreadCores {
				d = partitionDriver(t, wholeCoreCPUInfos, []device.Partition{{Name: device.DefaultPartitionName, AllowExclusive: true}}, true, nil)
			}
			params := cmp.Or(tc.params, exclusiveParams)
			source := cmp.Or(tc.source, resourceapi.AllocationConfigSourceClaim)
			claim := withConfig(claimWithResults("claim", tc.results...), source, tc.flagged, params)
			before := d.allocationState.GetPreparedCPUs()
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			record, recorded := d.allocationState.GetClaimRecord("claim")
			if tc.wantErr != "" {
				require.EqualError(t, results["claim"].Err, tc.wantErr)
				require.False(t, recorded, "a refused claim leaves no record")
				return
			}
			require.NoError(t, results["claim"].Err)
			require.True(t, recorded)
			require.Equal(t, tc.want, record.Requests)
			taken := record.CPUs()
			require.Truef(t, before.Union(taken).Equals(d.allocationState.GetPreparedCPUs()), "the exclusive CPUs are reserved, got %s", d.allocationState.GetPreparedCPUs())
			require.Truef(t, cpuset.New(0, 2, 3, 4).Difference(before).Difference(taken).Equals(d.allocationState.GetClaimlessCPUs()),
				"containers without claims leave the exclusive CPUs, got %s", d.allocationState.GetClaimlessCPUs())

			written := d.cdiMgr.(*mockCdiMgr).records["claim"]
			for _, prepared := range results["claim"].Devices {
				attrs := prepared.Metadata.Attributes
				request := prepared.Requests[0]
				cpus := cpusOfRequest(record, request, store.RoleExclusive)
				if cpus.IsEmpty() {
					require.NotContains(t, attrs, string(device.AttributeCPUSet), "request %q", request)
					continue
				}
				require.Equal(t, cpus.String(), *attrs[string(device.AttributeCPUSet)].StringValue, "request %q", request)
				require.Equal(t, int64(cpus.Size()), *attrs[string(device.AttributeAllocatedNumCPUs)].IntValue, "request %q", request)
				require.Equal(t, "max", *attrs[string(device.AttributeCPULimit)].StringValue, "request %q", request)
				for _, onDisk := range written.Requests {
					if onDisk.Request == request {
						require.Equal(t, store.RoleExclusive, onDisk.Role, "request %q's CDI record", request)
						require.Equal(t, []cdiResultRecord{{Device: "cpudevnuma000", Consumed: new(resource.MustParse(strconv.Itoa(cpus.Size())))}}, onDisk.Results, "request %q's CDI record", request)
					}
				}
			}

			encoded, err := encodeClaimRecord(written)
			require.NoError(t, err)
			decoded, err := decodeClaimRecord(encoded)
			require.NoError(t, err)
			onDisk := map[types.UID]cdiClaimRecord{"claim": decoded}
			maps.Copy(onDisk, tc.onDisk)
			restarted := allowingDriver(t, true, tc.whole, onDisk)
			restored, ok := restarted.allocationState.GetClaimRecord("claim")
			require.True(t, ok)
			require.Equal(t, tc.want, restored.Requests, "restored")
		})
	}
}

// cpusOfRequest returns the CPUs a record gives a request in role.
func cpusOfRequest(record store.ClaimRecord, request string, role store.Role) cpuset.CPUSet {
	for _, held := range record.Requests {
		if held.Request == request && held.Role == role {
			return held.CPUs
		}
	}
	return cpuset.New()
}

// A take moves the containers without claims and every share holder off the
// CPUs it takes when its claim's first container is created, a share holder
// keeping its own exclusive CPUs, and the worker gives them back once the
// claim is unprepared.
func TestATakeMovesShareHoldersAndGivesBack(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	d := allowingDriver(t, true, false, nil)
	runtime := &fakeRuntime{}
	d.containerUpdater = runtime
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	env := func(claim, request string) string {
		return fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim, request, cdiEnvDynamicValue)
	}
	create := func(t *testing.T, id string, env ...string) (string, map[string]string) {
		t.Helper()
		adjust, updates, err := d.CreateContainer(ctx, pod, &api.Container{Id: id, Name: id, PodSandboxId: pod.Id, Env: env})
		require.NoError(t, err)
		sent := map[string]string{}
		for _, update := range updates {
			sent[update.GetContainerId()] = update.GetLinux().GetResources().GetCpu().GetCpus()
		}
		return adjust.GetLinux().GetResources().GetCpu().GetCpus(), sent
	}
	prepare := func(t *testing.T, claims ...*resourceapi.ResourceClaim) {
		t.Helper()
		results, err := d.PrepareResourceClaims(ctx, claims)
		require.NoError(t, err)
		for uid, result := range results {
			require.NoError(t, result.Err, "claim %s", uid)
		}
	}

	prepare(t,
		claimWithResults("share", claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}),
		claimWithResults("mixed",
			claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}))
	cpus, _ := create(t, "plain")
	require.Equal(t, "0,2-4", cpus)
	cpus, _ = create(t, "holder", env("share", "vmm-cpu"))
	require.Equal(t, "0,2-4", cpus)
	cpus, _ = create(t, "vm", env("mixed", "vcpu"), env("mixed", "vmm-cpu"))
	require.Equal(t, "0-5", cpus)

	prepare(t, withConfig(claimWithResults("excl", claimResult{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, exclusiveParams))
	cpus, sent := create(t, "app", env("excl", "cpus"))
	require.Equal(t, "2-3", cpus)
	require.Equal(t, map[string]string{"plain": "0,4", "holder": "0,4", "vm": "0-1,4-5"}, sent, "the take moves every container off its CPUs")

	_, err := d.StopContainer(ctx, pod, &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id})
	require.NoError(t, err)
	unprepared, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "excl"}})
	require.NoError(t, err)
	require.NoError(t, unprepared["excl"])
	d.reconcileSharedContainers(testr.New(t))
	require.Equal(t, []map[string]string{{"plain": "0,2-4", "holder": "0,2-4", "vm": "0-5"}}, runtime.sent, "the give-back widens them again")
}

// takeOfEveryCPU is a claim whose flagged request takes all four CPUs of
// allowingDriver's shared device.
func takeOfEveryCPU() *resourceapi.ResourceClaim {
	return withConfig(claimWithResults("excl", claimResult{request: "cpus", written: "4", consumed: "4", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, exclusiveParams)
}

// A container that needs a shared device's CPUs while a take holds every one
// of them is refused when it is created, as upstream refuses a container
// without claims while claims hold every shared CPU, and comes up once the
// take is released.
func TestContainersCreatedAfterATakeOfEveryCPU(t *testing.T) {
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	for _, tc := range []struct {
		name    string
		claim   *resourceapi.ResourceClaim
		env     []string
		wantErr string
	}{{
		name:    "a container without claims",
		wantErr: `cannot create shared container "ctr" in pod "pod": no shared CPUs available`,
	}, {
		name:    "a container holding a share of 0, the only share beside such a take",
		claim:   claimWithResults("share", claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}),
		env:     []string{fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, "share", "vmm-cpu", cdiEnvDynamicValue)},
		wantErr: `cannot create container "ctr" in pod "pod": every CPU of the devices its shared requests use is held by an exclusive claim`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := allowingDriver(t, true, false, nil)
			claims := []*resourceapi.ResourceClaim{takeOfEveryCPU()}
			if tc.claim != nil {
				claims = append(claims, tc.claim)
			}
			results, err := d.PrepareResourceClaims(ctx, claims)
			require.NoError(t, err)
			for uid, result := range results {
				require.NoError(t, result.Err, "claim %s", uid)
			}
			app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, "excl", "cpus", cdiEnvDynamicValue)}}
			_, _, err = d.CreateContainer(ctx, pod, app)
			require.NoError(t, err)

			ctr := &api.Container{Id: "ctr", Name: "ctr", PodSandboxId: pod.Id, Env: tc.env}
			_, _, err = d.CreateContainer(ctx, pod, ctr)
			require.EqualError(t, err, tc.wantErr)

			_, err = d.StopContainer(ctx, pod, app)
			require.NoError(t, err)
			unprepared, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "excl"}})
			require.NoError(t, err)
			require.NoError(t, unprepared["excl"])
			adjust, _, err := d.CreateContainer(ctx, pod, ctr)
			require.NoError(t, err, "the container comes up once the take is released")
			require.Equal(t, "0,2-4", adjust.GetLinux().GetResources().GetCpu().GetCpus())
		})
	}
}

// A running container that a take would leave no CPU refuses the take at
// Prepare, as upstream refuses it while containers without claims would be
// left none, a holder of a share counted as they are. A holder of exclusive
// CPUs beside its share is moved onto those alone, as one created after the
// take gets them. A take reserved beside such a holder all the same has its
// first container refused, since sent no CPUs the holder would keep the
// take's, and the take's container comes up once the holder is gone.
func TestARunningShareHolderRefusesATakeOfEveryCPU(t *testing.T) {
	const exhausts = `claim "excl" would exhaust the shared CPU pool while shared containers are running`
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	env := func(claim, request string) string {
		return fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim, request, cdiEnvDynamicValue)
	}
	shareAlone := func() *resourceapi.ResourceClaim {
		return claimWithResults("held", claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"})
	}
	for _, tc := range []struct {
		name           string
		claim          *resourceapi.ResourceClaim
		env            []string
		pastPrepare    bool
		wantCPUs       string
		wantPrepareErr string
		wantMovedTo    string
	}{{
		name:           "a container without claims",
		wantCPUs:       "0,2-4",
		wantPrepareErr: exhausts,
	}, {
		name:           "a holder of a share alone",
		claim:          shareAlone(),
		env:            []string{env("held", "vmm-cpu")},
		wantCPUs:       "0,2-4",
		wantPrepareErr: exhausts,
	}, {
		name:        "a holder of a share alone beside a take reserved past Prepare's check",
		claim:       shareAlone(),
		env:         []string{env("held", "vmm-cpu")},
		pastPrepare: true,
		wantCPUs:    "0,2-4",
	}, {
		name: "a holder of a share beside exclusive CPUs",
		claim: claimWithResults("held",
			claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"}),
		env:         []string{env("held", "vcpu"), env("held", "vmm-cpu")},
		wantCPUs:    "0-5",
		wantMovedTo: "1,5",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := allowingDriver(t, true, false, nil)
			prepare := func(claim *resourceapi.ResourceClaim) error {
				t.Helper()
				results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
				require.NoError(t, err)
				return results[claim.UID].Err
			}
			if tc.claim != nil {
				require.NoError(t, prepare(tc.claim))
			}
			holder := &api.Container{Id: "holder", Name: "holder", PodSandboxId: pod.Id, Env: tc.env}
			adjust, _, err := d.CreateContainer(ctx, pod, holder)
			require.NoError(t, err)
			require.Equal(t, tc.wantCPUs, adjust.GetLinux().GetResources().GetCpu().GetCpus())

			if tc.wantPrepareErr != "" {
				require.EqualError(t, prepare(takeOfEveryCPU()), tc.wantPrepareErr)
				return
			}
			if tc.pastPrepare {
				take := store.ClaimRecord{Namespace: "vms", Name: "excl", Requests: []store.RequestAllocation{{Request: "cpus", CPUs: cpuset.New(0, 2, 3, 4)}}}
				require.NoError(t, d.allocationState.Reserve(testr.New(t), "excl", take, false))
			} else {
				require.NoError(t, prepare(takeOfEveryCPU()))
			}
			app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: []string{env("excl", "cpus")}}
			adjust, updates, err := d.CreateContainer(ctx, pod, app)
			if tc.wantMovedTo != "" {
				require.NoError(t, err)
				require.Equal(t, "0,2-4", adjust.GetLinux().GetResources().GetCpu().GetCpus())
				require.Len(t, updates, 1)
				require.Equal(t, "holder", updates[0].GetContainerId())
				require.Equal(t, tc.wantMovedTo, updates[0].GetLinux().GetResources().GetCpu().GetCpus(), "the holder keeps its exclusive CPUs alone")
				return
			}
			require.EqualError(t, err, `cannot update shared CPU pool after creating container "app" in pod "pod": cannot update shared containers: no shared CPUs available`)
			owner, _ := d.allocationState.Owner("excl")
			require.True(t, owner.Vacant(), "the refused container does not keep the take")

			_, err = d.StopContainer(ctx, pod, holder)
			require.NoError(t, err)
			adjust, updates, err = d.CreateContainer(ctx, pod, app)
			require.NoError(t, err, "the take's container comes up once the holder is gone")
			require.Equal(t, "0,2-4", adjust.GetLinux().GetResources().GetCpu().GetCpus())
			require.Empty(t, updates)
		})
	}
}

// No container without claims runs on a named partition of role default, so
// there the running holders of its shares alone hold a take back: a take that
// would leave one of them no CPU is refused at Prepare, and one where nobody
// runs, or beside a holder of exclusive CPUs of its own, is accepted.
func TestPrepareCountsRunningShareHoldersOnANamedDefaultPartition(t *testing.T) {
	const infra = "cpudevnuma000-infra"
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
	env := func(claim, request string) string {
		return fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim, request, cdiEnvDynamicValue)
	}
	share := func() *resourceapi.ResourceClaim {
		return claimWithResults("held", claimResult{request: "vmm-cpu", consumed: "0", device: infra})
	}
	for _, tc := range []struct {
		name    string
		held    *resourceapi.ResourceClaim
		running [][]string // each running container's env, none for one without claims
		take    string
		wantErr string
	}{{
		name:    "a running holder of a zero-amount share",
		held:    share(),
		running: [][]string{{env("held", "vmm-cpu")}},
		take:    "2",
		wantErr: `claim "excl" would exhaust the shared CPU pool while shared containers are running`,
	}, {
		name: "nobody running",
		take: "2",
	}, {
		name: "a holder's claim prepared, its container not created",
		held: share(),
		take: "2",
	}, {
		name:    "a running holder left a CPU",
		held:    share(),
		running: [][]string{{env("held", "vmm-cpu")}},
		take:    "1",
	}, {
		name: "a running holder of exclusive CPUs beside its share",
		held: claimWithResults("held",
			claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			claimResult{request: "vmm-cpu", consumed: "0", device: infra}),
		running: [][]string{{env("held", "vcpu"), env("held", "vmm-cpu")}},
		take:    "2",
	}, {
		name:    "a running container without claims, on the default partition",
		running: [][]string{nil},
		take:    "2",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := partitionDriver(t, mixedCoreCPUInfos, []device.Partition{
				{Name: device.DefaultPartitionName},
				{Name: "infra", Role: device.PARTITION_ROLE_DEFAULT, CPUs: cpuset.New(2, 3), AllowExclusive: true},
				{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
			}, false, nil)
			prepare := func(claim *resourceapi.ResourceClaim) error {
				t.Helper()
				results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
				require.NoError(t, err)
				return results[claim.UID].Err
			}
			if tc.held != nil {
				require.NoError(t, prepare(tc.held))
			}
			for i, env := range tc.running {
				id := fmt.Sprintf("ctr%d", i)
				_, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: id, Name: id, PodSandboxId: pod.Id, Env: env})
				require.NoError(t, err)
			}

			err := prepare(withConfig(claimWithResults("excl", claimResult{request: "cpus", written: tc.take, consumed: tc.take, device: infra}),
				resourceapi.AllocationConfigSourceClaim, nil, exclusiveParams))
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				_, recorded := d.allocationState.GetClaimRecord("excl")
				require.False(t, recorded, "a refused take reserves nothing")
				return
			}
			require.NoError(t, err)
		})
	}
}

// `cpuConfig.exclusive: false` asks for a share, which an owned device does
// not give.
func TestPrepareRefusesExclusiveFalseOnAnOwnedDevice(t *testing.T) {
	d := partitionDriver(t, mixedCoreCPUInfos, nil, false, nil)
	claim := withConfig(claimWithResults("claim", claimResult{request: "cpus", written: "500m", consumed: "500m", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, `{"apiVersion": "v1alpha1", "cpuConfig": {"exclusive": false}}`)
	results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.EqualError(t, results["claim"].Err, "request \"cpus\": `cpuConfig.exclusive: false`, but node \""+testNodeName+
		"\" runs without partitions, where "+testDriverName+" gives exclusive CPUs only; add `- name: default` to cpuPartitions, or drop cpuConfig.exclusive")
}

// partitionDriver is a driver on infos whose configuration declares
// partitions, nil for none, with claims on disk seeded before it starts.
func partitionDriver(t *testing.T, infos []cpuinfo.CPUInfo, partitions []device.Partition, fullPhysicalCPUsOnly bool, onDisk map[types.UID]cdiClaimRecord) *CPUDriver {
	t.Helper()
	d, err := New(testr.New(t), Providers{
		K8SClient: statusClient(),
		CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:     partitionSysFS(),
	}, &Config{
		DriverName:           testDriverName,
		NodeName:             testNodeName,
		ReservedCPUs:         cpuset.New(),
		CPUDeviceMode:        device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:     device.GROUP_BY_NUMA_NODE,
		FullPhysicalCPUsOnly: fullPhysicalCPUsOnly,
		CPUPartitions:        partitions,
	})
	require.NoError(t, err)
	cdi := newMockCdiMgr()
	maps.Copy(cdi.records, onDisk)
	d.cdiMgr = cdi
	d.seedAllocationStoreFromDisk(testr.New(t))
	d.hasSynchronized = true
	return d
}

// An exclusive result books whole CPUs, at least one, so a written amount of
// 0 or a fraction is refused on it, with partitions or without, a mixed
// request's shared half alone staying valid. Under fullPhysicalCPUsOnly a
// device of two-thread cores asks for whole cores instead.
func TestPrepareRefusesAnAmountAnExclusiveResultCannotBook(t *testing.T) {
	vms := []device.Partition{
		{Name: device.DefaultPartitionName},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
	}
	noPartitions := func(t *testing.T) *CPUDriver {
		t.Helper()
		return partitionDriver(t, mixedCoreCPUInfos, nil, false, nil)
	}
	withPartitions := func(t *testing.T) *CPUDriver {
		t.Helper()
		return sharingDriver(t, true, nil, nil)
	}
	wholeCores := func(t *testing.T) *CPUDriver {
		t.Helper()
		return partitionDriver(t, wholeCoreCPUInfos, nil, true, nil)
	}
	mixedWholeCores := func(t *testing.T) *CPUDriver {
		t.Helper()
		return partitionDriver(t, mixedCoreCPUInfos, nil, true, nil)
	}
	partitionedWholeCores := func(t *testing.T) *CPUDriver {
		t.Helper()
		return partitionDriver(t, mixedCoreCPUInfos, vms, true, nil)
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
		wantErr: `request "vmm-cpu": amount 500m on exclusive device "cpudevnuma000-vms"; ask for a whole number of CPUs`,
	}, {
		name:    "its shared half alone",
		driver:  withPartitions,
		results: []claimResult{{request: "vmm-cpu", written: "500m", consumed: "500m", device: "cpudevnuma000"}},
	}, {
		name:    "an exclusive request with a fractional amount on a partitioned node",
		driver:  withPartitions,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "500m", device: "cpudevnuma000-vms"}},
		wantErr: `request "cpus": amount 500m on exclusive device "cpudevnuma000-vms"; ask for a whole number of CPUs`,
	}, {
		name:    "an exclusive request for 0 on a partitioned node",
		driver:  withPartitions,
		results: []claimResult{{request: "cpus", written: "0", consumed: "1", device: "cpudevnuma000-vms"}},
		wantErr: `request "cpus": amount 0 on exclusive device "cpudevnuma000-vms"; ask for at least 1`,
	}, {
		name:    "an exclusive request with a whole amount on a partitioned node",
		driver:  withPartitions,
		results: []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000-vms"}},
	}, {
		name:    "an exclusive request with a fractional amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus", written: "1500m", consumed: "1500m", device: "cpudevnuma000"}},
		wantErr: `request "cpus": amount 1500m on exclusive request (class dra.cpu, no cpuPartitions); ask for a whole number of CPUs`,
	}, {
		name:    "an alternative with a fractional amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus/whole", written: "500m", consumed: "500m", device: "cpudevnuma000"}},
		wantErr: `request "cpus/whole": amount 500m on exclusive request (class dra.cpu, no cpuPartitions); ask for a whole number of CPUs`,
	}, {
		name:    "an exclusive request for 0 without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus", written: "0", consumed: "1", device: "cpudevnuma000"}},
		wantErr: `request "cpus": amount 0 on exclusive request (class dra.cpu, no cpuPartitions); ask for at least 1`,
	}, {
		name:    "an exclusive request with a whole amount without partitions",
		driver:  noPartitions,
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma000"}},
	}, {
		name:    "a fractional amount the scheduler rounded up to whole cores",
		driver:  wholeCores,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "2", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": amount 500m on exclusive request (class dra.cpu, no cpuPartitions); ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
	}, {
		name:    "0 the scheduler rounded up to whole cores",
		driver:  wholeCores,
		results: []claimResult{{request: "cpus", written: "0", consumed: "2", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": amount 0 on exclusive request (class dra.cpu, no cpuPartitions); ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
	}, {
		name:    "a whole amount the scheduler rounded up to whole cores",
		driver:  wholeCores,
		results: []claimResult{{request: "cpus", written: "3", consumed: "4", device: "cpudevnuma000"}},
		wantErr: "request \"cpus\": amount 3 on exclusive request (class dra.cpu, no cpuPartitions); ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
	}, {
		name:    "a fractional amount on a device whose cores differ in threads",
		driver:  mixedWholeCores,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "500m", device: "cpudevnuma000"}},
		wantErr: `request "cpus": amount 500m on exclusive request (class dra.cpu, no cpuPartitions); ask for a whole number of CPUs`,
	}, {
		name:    "a whole amount rounded up to whole cores on a partitioned node",
		driver:  partitionedWholeCores,
		results: []claimResult{{request: "cpus", written: "1", consumed: "2", device: "cpudevnuma000-vms"}},
		wantErr: "request \"cpus\": amount 1 on exclusive device \"cpudevnuma000-vms\"; ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
	}, {
		name:    "a fractional amount rounded up to whole cores on a partitioned node",
		driver:  partitionedWholeCores,
		results: []claimResult{{request: "cpus", written: "500m", consumed: "2", device: "cpudevnuma000-vms"}},
		wantErr: "request \"cpus\": amount 500m on exclusive device \"cpudevnuma000-vms\"; ask for whole cores of 2 threads (`fullPhysicalCPUsOnly: true`)",
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

// Where partitions are used, a result on a device the node does not publish is
// refused before any other check of the claim, so a share on a shared device
// the partitions removed is not judged as an exclusive amount.
func TestPrepareRefusesAResultOnADeviceNotPublished(t *testing.T) {
	for _, tc := range []struct {
		name       string
		partitions bool
		results    []claimResult
		wantErr    string
	}{{
		name:       "a share on a shared device removed",
		partitions: true,
		results:    []claimResult{{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000-infra"}},
		wantErr:    `request "vmm-cpu": device "cpudevnuma000-infra" is no longer published by this node`,
	}, {
		name:       "an exclusive request on an owned device removed",
		partitions: true,
		results:    []claimResult{{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma001-vms"}},
		wantErr:    `request "cpus": device "cpudevnuma001-vms" is no longer published by this node`,
	}, {
		name:       "before a refusal of another result's amount",
		partitions: true,
		results: []claimResult{
			{request: "cpus", written: "500m", consumed: "500m", device: "cpudevnuma000-vms"},
			{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma001-vms"},
		},
		wantErr: `request "vcpu": device "cpudevnuma001-vms" is no longer published by this node`,
	}, {
		name:    "without partitions as upstream",
		results: []claimResult{{request: "cpus", written: "1", consumed: "1", device: "cpudevnuma009"}},
		wantErr: `no valid NUMA node ID found for device cpudevnuma009`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := partitionDriver(t, mixedCoreCPUInfos, nil, false, nil)
			if tc.partitions {
				d = sharingDriver(t, true, nil, nil)
			}
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claimWithResults("claim", tc.results...)})
			require.NoError(t, err)
			require.EqualError(t, results["claim"].Err, tc.wantErr)
			_, recorded := d.allocationState.GetClaimRecord("claim")
			require.False(t, recorded, "a refused claim leaves no record")
		})
	}
}

// A share recorded on CPUs its devices no longer share, the partitions having
// changed across a restart, is refused when the kubelet prepares it again.
func TestPrepareRefusesAShareRecordedOnCPUsItsDeviceNoLongerShares(t *testing.T) {
	share := claimWithResults("share", claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"})
	infra := []device.Partition{
		{Name: device.DefaultPartitionName},
		{Name: "infra", Role: device.PARTITION_ROLE_DEFAULT, CPUs: cpuset.New(2)},
		{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
	}
	for _, tc := range []struct {
		name       string
		partitions []device.Partition // nil for sharingDriver's
		onDisk     cdiClaimRecord
		wantErr    string
	}{{
		name:   "recorded on the device's CPUs",
		onDisk: shareOnDisk(cpuset.New(0, 2, 3, 4), "cpudevnuma000"),
	}, {
		name:    "recorded on CPUs the exclusive partition took since",
		onDisk:  shareOnDisk(cpuset.New(0, 1, 2, 3, 4, 5), "cpudevnuma000"),
		wantErr: `request "vmm-cpu": device "cpudevnuma000" has changed since the claim was prepared`,
	}, {
		name:       "recorded on devices that lost CPUs",
		partitions: infra,
		onDisk:     shareOnDisk(cpuset.New(0, 1, 2, 3, 4, 5), "cpudevnuma000", "cpudevnuma000-infra"),
		wantErr:    `request "vmm-cpu": devices "cpudevnuma000", "cpudevnuma000-infra" have changed since the claim was prepared`,
	}, {
		name:    "recorded on a device no longer published",
		onDisk:  shareOnDisk(cpuset.New(0, 2), "cpudevnuma001"),
		wantErr: `request "vmm-cpu": device "cpudevnuma001" is no longer published by this node`,
	}, {
		name:    "recorded on a device now owned",
		onDisk:  shareOnDisk(cpuset.New(1, 5), "cpudevnuma000-vms"),
		wantErr: `request "vmm-cpu": device "cpudevnuma000-vms" has changed since the claim was prepared`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			onDisk := map[types.UID]cdiClaimRecord{"share": tc.onDisk}
			d := sharingDriver(t, false, onDisk, nil)
			if tc.partitions != nil {
				d = partitionDriver(t, mixedCoreCPUInfos, tc.partitions, false, onDisk)
			}
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

func capped(name string, capMilli int64) cdiClaimRecord {
	return cdiClaimRecord{Namespace: "vms", Name: name, Requests: []cdiRequestRecord{{
		Request: "cpus", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Results: []cdiResultRecord{{Device: "cpudevnuma000"}}, Cap: capMilli,
	}}}
}

func kubeletQuota(quota int64) *api.LinuxContainer {
	return &api.LinuxContainer{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Quota: &api.OptionalInt64{Value: quota}, Period: &api.OptionalUInt64{Value: 100000}}}}
}

// A container whose CPU limit gives it a kubelet quota while its claim asks for
// a cap gets a Warning on its pod naming both, and starts under the kubelet's.
func TestCreateContainerWarnsWhenACPULimitReplacesTheCap(t *testing.T) {
	onDisk := map[types.UID]cdiClaimRecord{"a": capped("a", 250), "b": capped("b", 500), "uncapped": capped("uncapped", 0)}
	for _, tc := range []struct {
		name  string
		vars  []string
		linux *api.LinuxContainer
		want  []string
	}{{
		name:  "a capped share with a kubelet quota",
		vars:  []string{"a_cpus"},
		linux: kubeletQuota(50000),
		want:  []string{`Warning CPULimitReplacesCap container "app": its CPU limit of 500m replaces the limit of 250m of request "cpus" of claim "a"`},
	}, {
		name: "a capped share without a quota",
		vars: []string{"a_cpus"},
	}, {
		name:  "an uncapped share with a kubelet quota",
		vars:  []string{"uncapped_cpus"},
		linux: kubeletQuota(50000),
	}, {
		name:  "a refused container",
		vars:  []string{"a_cpus", "b_cpus"},
		linux: kubeletQuota(50000),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, true, onDisk, nil)
			events := &capturingRecorder{}
			d.eventRecorder = events
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod-uid", Name: "vm", Namespace: "vms"}
			var env []string
			for _, v := range tc.vars {
				env = append(env, fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, v, cdiEnvDynamicValue))
			}
			_, _, _ = d.CreateContainer(ctx, pod, &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: env, Linux: tc.linux})
			require.Equal(t, tc.want, events.lines())
			for _, event := range events.events {
				object, ok := event.object.(*corev1.Pod)
				require.True(t, ok)
				require.Equal(t, "vms/vm", object.Namespace+"/"+object.Name)
				require.Equal(t, types.UID("pod-uid"), object.UID)
			}
		})
	}
}

// A container whose cgroup holds a capped share alone gets the cap as its CPU
// quota, unless the kubelet sent one for a CPU limit; a capped share beside any
// other request in one cgroup is refused before the container binds a claim.
func TestCreateContainerCapsACappedShareAlone(t *testing.T) {
	onDisk := map[types.UID]cdiClaimRecord{
		"a":        capped("a", 250),
		"b":        capped("b", 500),
		"tiny":     capped("tiny", 5),
		"uncapped": capped("uncapped", 0),
		"vcpu":     exclusiveOnDisk(cpuset.New(1, 5)),
		"older": {Namespace: "vms", Name: "older", Requests: []cdiRequestRecord{
			{Request: "vcpu", CPUs: cpuset.New(1, 5), Results: []cdiResultRecord{{Device: "cpudevnuma000-vms"}}},
			{Request: "vmm-cpu", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Results: []cdiResultRecord{{Device: "cpudevnuma000"}}, Cap: 250},
		}},
	}
	env := func(vars ...string) []string {
		out := make([]string, 0, len(vars))
		for _, v := range vars {
			out = append(out, fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, v, cdiEnvDynamicValue))
		}
		return out
	}
	for _, tc := range []struct {
		name        string
		env         []string
		linux       *api.LinuxContainer
		wantQuota   int64 // 0 for none
		wantErr     string
		wantUnbound []types.UID
	}{{
		name:      "a capped share alone",
		env:       env("a_cpus"),
		wantQuota: 25000,
	}, {
		name:      "a cap below 10m written as the kernel's 1 ms",
		env:       env("tiny_cpus"),
		wantQuota: 1000,
	}, {
		name:  "a kubelet quota stays",
		env:   env("a_cpus"),
		linux: kubeletQuota(50000),
	}, {
		name: "an uncapped share alone",
		env:  env("uncapped_cpus"),
	}, {
		name:        "two capped shares",
		env:         env("a_cpus", "b_cpus"),
		wantErr:     `container "app": request "cpus" of claim "a" and request "cpus" of claim "b" both have limits, so they cannot be used together: use the claims in separate containers, or remove one of the limits`,
		wantUnbound: []types.UID{"a", "b"},
	}, {
		name:        "a capped share beside an uncapped one",
		env:         env("a_cpus", "uncapped_cpus"),
		wantErr:     `container "app": request "cpus" of claim "a" has limit of 250m, so it cannot be used together with request "cpus" of claim "uncapped": use the claims in separate containers, or remove the limit`,
		wantUnbound: []types.UID{"a", "uncapped"},
	}, {
		name:        "a capped share beside exclusive CPUs",
		env:         env("a_cpus", "vcpu_vcpu"),
		wantErr:     `container "app": request "cpus" of claim "a" has limit of 250m, so it cannot be used together with request "vcpu" of claim "exclusive": use the claims in separate containers, or remove the limit`,
		wantUnbound: []types.UID{"a", "vcpu"},
	}, {
		name:        "a capped share beside another request of its claim",
		env:         env("older_vcpu", "older_vmm-cpu"),
		wantErr:     `container "app": request "vmm-cpu" of claim "older" has limit of 250m, so it cannot be used together with request "vcpu" of claim "older": use the claims in separate containers, or remove the limit`,
		wantUnbound: []types.UID{"older"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, true, onDisk, nil)
			d.eventRecorder = &capturingRecorder{}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: tc.env, Linux: tc.linux})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				for _, uid := range tc.wantUnbound {
					_, bound := d.allocationState.Owner(uid)
					require.Falsef(t, bound, "claim %s is left to another container", uid)
				}
				if len(tc.wantUnbound) > 1 {
					_, _, err = d.CreateContainer(ctx, pod, &api.Container{Id: "other", Name: "other", PodSandboxId: pod.Id, Env: tc.env[:1]})
					require.NoError(t, err, "another container takes a claim the refused one named")
				}
				return
			}
			require.NoError(t, err)
			cpu := adjust.GetLinux().GetResources().GetCpu()
			if tc.wantQuota == 0 {
				require.Nil(t, cpu.GetQuota(), "no quota written")
				require.Nil(t, cpu.GetPeriod())
				return
			}
			require.Equal(t, tc.wantQuota, cpu.GetQuota().GetValue())
			require.Equal(t, uint64(100000), cpu.GetPeriod().GetValue())
			require.Nil(t, cpu.GetShares(), "shares are never set")
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

// Synchronize converges the cap of a listed container as CreateContainer would
// have written it, and leaves a container whose quota the kubelet set or the
// driver already wrote, or that holds its claims under an older rule.
func TestSynchronizeConvergesTheCap(t *testing.T) {
	onDisk := map[types.UID]cdiClaimRecord{
		"a":        capped("a", 250),
		"b":        capped("b", 500),
		"uncapped": capped("uncapped", 0),
		"older": {Namespace: "vms", Name: "older", Requests: []cdiRequestRecord{
			{Request: "vcpu", CPUs: cpuset.New(1, 5), Results: []cdiResultRecord{{Device: "cpudevnuma000-vms"}}},
			{Request: "vmm-cpu", CPUs: cpuset.New(0, 2, 3, 4), Role: store.RoleShared, Results: []cdiResultRecord{{Device: "cpudevnuma000"}}, Cap: 250},
		}},
	}
	linux := func(cpus string, quota int64) *api.LinuxContainer {
		cpu := &api.LinuxCPU{Cpus: cpus}
		if quota > 0 {
			cpu.Quota = &api.OptionalInt64{Value: quota}
		}
		return &api.LinuxContainer{Resources: &api.LinuxResources{Cpu: cpu}}
	}
	for _, tc := range []struct {
		name        string
		vars        []string
		linux       *api.LinuxContainer
		wantQuota   int64 // 0 for none
		wantWarning string
	}{{
		name:      "no quota: the cap written",
		vars:      []string{"a_cpus"},
		linux:     linux("0,2-4", 0),
		wantQuota: 25000,
	}, {
		name:  "a kubelet quota left",
		vars:  []string{"a_cpus"},
		linux: linux("0,2-4", 50000),
	}, {
		name:  "the cap already there left",
		vars:  []string{"a_cpus"},
		linux: linux("0,2-4", 25000),
	}, {
		name:  "a container kept under an older rule left",
		vars:  []string{"older_vmm-cpu"},
		linux: linux("0-5", 0),
	}, {
		name:        "a capped share beside another request left uncapped, with a Warning",
		vars:        []string{"a_cpus", "uncapped_cpus"},
		linux:       linux("0,2-4", 0),
		wantWarning: `container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule limit_with_others: request "cpus" of claim "a" has limit of 250m, so it cannot be used together with request "cpus" of claim "uncapped"`,
	}, {
		name:        "two capped shares left uncapped, with a Warning",
		vars:        []string{"a_cpus", "b_cpus"},
		linux:       linux("0,2-4", 0),
		wantWarning: `container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule limit_with_others: request "cpus" of claim "a" and request "cpus" of claim "b" both have limits, so they cannot be used together`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, true, onDisk, nil)
			events := &capturingRecorder{}
			d.eventRecorder = events
			reg := prometheus.NewRegistry()
			d.metrics = cpumetrics.New(reg)
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
			var env []string
			for _, v := range tc.vars {
				env = append(env, fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, v, cdiEnvDynamicValue))
			}
			updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: env, Linux: tc.linux}})
			require.NoError(t, err)
			var update *api.ContainerUpdate
			for _, u := range updates {
				if u.GetContainerId() == "app" {
					update = u
				}
			}
			require.NotNil(t, update, "the container gets its cpuset")
			owner, bound := d.allocationState.Owner(types.UID(strings.SplitN(tc.vars[0], "_", 2)[0]))
			require.True(t, bound, "the container stays bound")
			require.Equal(t, "app", owner.ContainerName)
			kept := 0.0
			if tc.wantWarning != "" {
				kept = 1
				require.Equal(t, []string{"Warning KeptUnderOlderRule " + tc.wantWarning}, events.lines())
			}
			require.Equal(t, kept, metricValue(t, reg, "dra_cpu_containers_kept_under_older_rule_total", map[string]string{"rule": string(cpumetrics.RuleLimitWithOthers)}))
			cpu := update.GetLinux().GetResources().GetCpu()
			if tc.wantQuota == 0 {
				require.Nil(t, cpu.GetQuota(), "no quota written")
				return
			}
			require.Equal(t, tc.wantQuota, cpu.GetQuota().GetValue())
			require.Equal(t, uint64(100000), cpu.GetPeriod().GetValue())
		})
	}
}
