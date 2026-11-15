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
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
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
			name:     "an smt false partition whose sibling is online",
			declared: []device.Partition{defaultEntry, partition("spdk", 1, 1)},
			wantErr:  `partition "spdk" lists CPUs 1 without their sibling threads 5: list every thread of each core`,
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
	for _, tc := range []struct {
		name         string
		sysfs        fstest.MapFS
		declared     []device.Partition
		wantErr      string
		wantReserved cpuset.CPUSet
		wantSlices   [][]string
	}{
		{
			name:         "the exclusive partition publishes, the reserved one holds its CPUs back",
			sysfs:        partitionSysFS(),
			declared:     declared,
			wantReserved: cpuset.New(3),
			wantSlices:   [][]string{{"cpudevnuma000-vms"}},
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
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			})
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
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
