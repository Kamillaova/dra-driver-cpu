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
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/utils/cpuset"
)

// spdkDegraded is mixedCoreCPUInfos' node with the exclusive partition spdk
// over CPU 1 under smt false, its sibling 5 online.
var spdkDegraded = []device.Partition{
	{Name: device.DefaultPartitionName},
	{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(0, 4)},
	{Name: "spdk", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1), ThreadsPerCore: 1},
}

// defaultDegraded is mixedCoreCPUInfos' node with the default entry under smt
// false, the sibling 4 of its CPU 0 online.
var defaultDegraded = []device.Partition{
	{Name: device.DefaultPartitionName, ThreadsPerCore: 1},
	{Name: "vms", Role: device.PARTITION_ROLE_EXCLUSIVE, CPUs: cpuset.New(1, 5)},
}

var degradedToleration = resourceapi.DeviceToleration{Key: "dra.cpu/degraded", Operator: resourceapi.DeviceTolerationOpEqual, Value: "SiblingThreadsOnline", Effect: resourceapi.DeviceTaintEffectNoSchedule}

func newDegradedDriver(t *testing.T, client *fake.Clientset, reg *prometheus.Registry, declared []device.Partition, taint bool) *CPUDriver {
	t.Helper()
	providers := Providers{CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}, SysFS: partitionSysFS()}
	if client != nil {
		providers.K8SClient = client
	}
	var metrics Recorder
	if reg != nil {
		metrics = cpumetrics.New(reg)
	}
	d, err := New(testr.New(t), providers, &Config{
		DriverName:              testDriverName,
		NodeName:                testNodeName,
		ReservedCPUs:            cpuset.New(),
		CPUDeviceMode:           device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:        device.GROUP_BY_NUMA_NODE,
		CPUPartitions:           declared,
		TaintDegradedPartitions: taint,
		Metrics:                 metrics,
	})
	require.NoError(t, err)
	t.Cleanup(d.nodeConditions.queue.ShutDown)
	d.cdiMgr = newMockCdiMgr()
	d.hasSynchronized = true
	return d
}

// degradedClaim is a claim whose one result, for request result, lies on
// device dev, the claim's spec holding request.
func degradedClaim(uid types.UID, dev, result string, request resourceapi.DeviceRequest) *resourceapi.ResourceClaim {
	claim := testClaim(uid, testDriverName, testNodeName, map[string]int64{dev: 1})
	claim.Status.Allocation.Devices.Results[0].Request = result
	claim.Spec.Devices.Requests = []resourceapi.DeviceRequest{request}
	return claim
}

func exactlyTolerating(class string, tolerations ...resourceapi.DeviceToleration) resourceapi.DeviceRequest {
	return resourceapi.DeviceRequest{Name: "vcpu", Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: class, Tolerations: tolerations}}
}

// subrequestTolerating gives the subrequest vcpu/spdk tolerations, and the
// other alternative, vcpu/vms, others.
func subrequestTolerating(spdk, vms []resourceapi.DeviceToleration) resourceapi.DeviceRequest {
	return resourceapi.DeviceRequest{Name: "vcpu", FirstAvailable: []resourceapi.DeviceSubRequest{
		{Name: "vms", DeviceClassName: "dra.cpu.vms", Tolerations: vms},
		{Name: "spdk", DeviceClassName: "dra.cpu.spdk", Tolerations: spdk},
	}}
}

func TestPrepareOnADegradedPartition(t *testing.T) {
	const (
		refusedUntolerated = `request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline); tolerate dra.cpu/degraded=SiblingThreadsOnline:NoSchedule, or take its sibling threads offline`
		refusedOff         = "request \"vcpu\": partition \"spdk\" is degraded (SiblingThreadsOnline) and its devices are not published (`taintDegradedPartitions: false`); take its sibling threads offline"
		refusedDropped     = `request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline) and its devices are not published; take its sibling threads offline`
	)
	for _, tc := range []struct {
		name     string
		declared []device.Partition
		taint    bool
		dropped  bool
		claim    *resourceapi.ResourceClaim
		wantErr  string
	}{
		{
			name:  "a tolerating request is prepared",
			taint: true,
			claim: degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration)),
		},
		{
			name:    "a request without the toleration is refused",
			taint:   true,
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk")),
			wantErr: refusedUntolerated,
		},
		{
			name:  "a tolerating subrequest is prepared",
			taint: true,
			claim: degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu/spdk", subrequestTolerating([]resourceapi.DeviceToleration{degradedToleration}, nil)),
		},
		{
			name:    "a subrequest is refused by its own tolerations, not another alternative's",
			taint:   true,
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu/spdk", subrequestTolerating(nil, []resourceapi.DeviceToleration{degradedToleration})),
			wantErr: `request "vcpu/spdk": partition "spdk" is degraded (SiblingThreadsOnline); tolerate dra.cpu/degraded=SiblingThreadsOnline:NoSchedule, or take its sibling threads offline`,
		},
		{
			name:  "a toleration with no key and operator Exists tolerates",
			taint: true,
			claim: degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", resourceapi.DeviceToleration{Operator: resourceapi.DeviceTolerationOpExists})),
		},
		{
			name:    "the key with another value does not tolerate",
			taint:   true,
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", resourceapi.DeviceToleration{Key: "dra.cpu/degraded", Operator: resourceapi.DeviceTolerationOpEqual, Value: "Broken"})),
			wantErr: refusedUntolerated,
		},
		{
			name:    "the key with another effect does not tolerate",
			taint:   true,
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", resourceapi.DeviceToleration{Key: "dra.cpu/degraded", Operator: resourceapi.DeviceTolerationOpExists, Effect: resourceapi.DeviceTaintEffectNoExecute})),
			wantErr: refusedUntolerated,
		},
		{
			name:    "with the option off a tolerating request is refused, its device not published",
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration)),
			wantErr: refusedOff,
		},
		{
			name:    "after a dropped taint a tolerating request is refused",
			taint:   true,
			dropped: true,
			claim:   degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration)),
			wantErr: refusedDropped,
		},
		{
			name:  "a healthy partition's device is not checked",
			taint: true,
			claim: degradedClaim("claim-0", "cpudevnuma000-vms", "vcpu", exactlyTolerating("dra.cpu.vms")),
		},
		{
			name:     "a share on the degraded default entry, tolerating",
			declared: defaultDegraded,
			taint:    true,
			claim:    degradedClaim("claim-0", "cpudevnuma000", "vcpu", exactlyTolerating("dra.cpu", degradedToleration)),
		},
		{
			name:     "a share on the degraded default entry, not tolerating",
			declared: defaultDegraded,
			taint:    true,
			claim:    degradedClaim("claim-0", "cpudevnuma000", "vcpu", exactlyTolerating("dra.cpu")),
			wantErr:  `request "vcpu": partition "default" is degraded (SiblingThreadsOnline); tolerate dra.cpu/degraded=SiblingThreadsOnline:NoSchedule, or take its sibling threads offline`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declared := tc.declared
			if declared == nil {
				declared = spdkDegraded
			}
			d := newDegradedDriver(t, nil, nil, declared, tc.taint)
			d.deviceTaintsDropped.Store(tc.dropped)

			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{tc.claim})
			require.NoError(t, err)
			if tc.wantErr == "" {
				require.NoError(t, results[tc.claim.UID].Err)
				return
			}
			require.EqualError(t, results[tc.claim.UID].Err, tc.wantErr)
			_, held := d.allocationState.GetClaimRecord(tc.claim.UID)
			require.False(t, held, "a refused claim holds no CPUs")
		})
	}
}

// A claim the store holds is reused before the degraded partitions are
// checked, so its replay is answered whatever its tolerations, the taint
// dropped or not.
func TestPrepareReusesAClaimOnADegradedPartition(t *testing.T) {
	d := newDegradedDriver(t, nil, nil, spdkDegraded, true)
	prepare := func(claim *resourceapi.ResourceClaim) error {
		t.Helper()
		results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
		require.NoError(t, err)
		return results[claim.UID].Err
	}
	require.NoError(t, prepare(degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration))))

	require.NoError(t, prepare(degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk"))))
	d.deviceTaintsDropped.Store(true)
	require.NoError(t, prepare(degradedClaim("claim-0", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk"))))
	require.EqualError(t, prepare(degradedClaim("claim-1", "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration))),
		`request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline) and its devices are not published; take its sibling threads offline`)
}

func TestNewTaintsDegradedPartitions(t *testing.T) {
	for _, tc := range []struct {
		name         string
		declared     []device.Partition
		taint        bool
		wantSlices   [][]string
		wantTainted  []string
		wantDegraded map[string]string
		wantShared   map[string]cpuset.CPUSet
	}{
		{
			name:         "the option on publishes the degraded partition's devices tainted",
			declared:     spdkDegraded,
			taint:        true,
			wantSlices:   [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}, {"cpudevnuma000-spdk"}},
			wantTainted:  []string{"cpudevnuma000-spdk"},
			wantDegraded: map[string]string{"cpudevnuma000-spdk": "spdk"},
			wantShared:   map[string]cpuset.CPUSet{"cpudevnuma000": cpuset.New(2, 3)},
		},
		{
			name:         "the option off withholds them and keeps their names",
			declared:     spdkDegraded,
			wantSlices:   [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}},
			wantDegraded: map[string]string{"cpudevnuma000-spdk": "spdk"},
			wantShared:   map[string]cpuset.CPUSet{"cpudevnuma000": cpuset.New(2, 3)},
		},
		{
			name:         "the degraded default entry's shared device tainted",
			declared:     defaultDegraded,
			taint:        true,
			wantSlices:   [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}},
			wantTainted:  []string{"cpudevnuma000"},
			wantDegraded: map[string]string{"cpudevnuma000": "default"},
			wantShared:   map[string]cpuset.CPUSet{"cpudevnuma000": cpuset.New(0, 2, 3)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newDegradedDriver(t, nil, nil, tc.declared, tc.taint)
			var gotSlices [][]string
			var gotTainted []string
			for _, slice := range d.topology.deviceSlices {
				var names []string
				for _, dev := range slice {
					names = append(names, dev.Name)
					if len(dev.Taints) > 0 {
						gotTainted = append(gotTainted, dev.Name)
					}
				}
				gotSlices = append(gotSlices, names)
			}
			require.Equal(t, tc.wantSlices, gotSlices)
			require.Equal(t, tc.wantTainted, gotTainted)
			require.Equal(t, tc.wantDegraded, d.topology.degradedDevices)
			require.Equal(t, tc.wantShared, d.topology.sharedDevices)
		})
	}
}

func droppedTaintSlice(tainted, mapped bool) *resourceapi.ResourceSlice {
	slice := testMappedSlice(mapped, 0)
	if tainted {
		slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{device.DegradedTaint()}
	}
	return slice
}

func TestHandleErrorReportsADroppedTaint(t *testing.T) {
	const (
		libraryMsg = "publishing ResourceSlices failed"
		taintMsg   = "API server dropped the taint of degraded partitions' devices from a ResourceSlice, so they are withheld and every claim on them is refused; enable DRADeviceTaints on the API server, then restart the driver pod"
		mappingMsg = "API server dropped nodeAllocatableResources from a ResourceSlice, so dra.cpu claims are not counted against node cpu; enable DRANodeAllocatableResources on the API server, then restart the driver pod"
	)
	for _, tc := range []struct {
		name        string
		err         error
		wantTaints  bool
		wantMapping bool
		wantMsgs    []string
	}{
		{
			name:       "the stored slice lost the taint",
			err:        &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: droppedTaintSlice(true, false), ActualSlice: droppedTaintSlice(false, false)},
			wantTaints: true,
			wantMsgs:   []string{taintMsg},
		},
		{
			name:        "one error carrying both drops reports each",
			err:         &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: droppedTaintSlice(true, true), ActualSlice: droppedTaintSlice(false, false)},
			wantTaints:  true,
			wantMapping: true,
			wantMsgs:    []string{mappingMsg, taintMsg},
		},
		{
			name:     "another field dropped, the taint kept",
			err:      &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: testMappedSlice(false, 1), ActualSlice: testMappedSlice(false, 0)},
			wantMsgs: []string{libraryMsg},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			d := newDegradedDriver(t, fake.NewClientset(testConditionNode(testReadyCond)), reg, spdkDegraded, true)
			plugin := &mockKubeletPlugin{stopFunc: func() { t.Error("HandleError stopped the DRA plugin") }}
			d.draPlugin = plugin
			logs := &logLines{}
			ctx := logr.NewContext(t.Context(), logs.logger())

			d.stateMu.Lock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				d.HandleError(ctx, asTheLibraryReports(tc.err), libraryMsg)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("HandleError waited on the driver's state lock")
			}
			d.stateMu.Unlock()

			require.Nil(t, plugin.publishedResources, "HandleError publishes nothing")
			require.Equal(t, tc.wantTaints, d.deviceTaintsDropped.Load())
			require.Equal(t, tc.wantMapping, d.allocatableMappingDropped.Load())
			wantKeys := 0
			if tc.wantTaints || tc.wantMapping {
				wantKeys = 1
			}
			require.Equal(t, wantKeys, d.nodeConditions.queue.Len())
			require.Zero(t, metricValue(t, reg, "dra_cpu_device_taints_dropped_total", nil), "the worker counts the withholding, not HandleError")
			require.Len(t, logs.lines, len(tc.wantMsgs))
			for _, msg := range tc.wantMsgs {
				require.Lenf(t, logs.matching(msg), 1, "log line %q", msg)
			}
		})
	}
}

func TestADroppedTaintWithholdsTheDegradedDevices(t *testing.T) {
	for _, tc := range []struct {
		name          string
		declared      []device.Partition
		wantBefore    [][]string
		wantAfter     [][]string
		wantCondition string
	}{
		{
			name:          "the degraded partition's slices go, the others stay",
			declared:      spdkDegraded,
			wantBefore:    [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}, {"cpudevnuma000-spdk"}},
			wantAfter:     [][]string{{"cpudevnuma000"}, {"cpudevnuma000-vms"}},
			wantCondition: `partitions are degraded while the kernel contradicts their smt: partition "spdk" expects 1 online thread per core, and CPUs 5 of its cores are online too; take those CPUs offline at boot, then restart the driver`,
		},
		{
			name:          "a node whose every partition is degraded publishes its pool empty",
			declared:      []device.Partition{{Name: device.DefaultPartitionName, ThreadsPerCore: 1}, spdkDegraded[2]},
			wantBefore:    [][]string{{"cpudevnuma000"}, {"cpudevnuma000-spdk"}},
			wantCondition: `partitions are degraded while the kernel contradicts their smt: partition "default" expects 1 online thread per core, and CPUs 4 of its cores are online too; partition "spdk" expects 1 online thread per core, and CPUs 5 of its cores are online too; take those CPUs offline at boot, then restart the driver`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			client := fake.NewClientset(testConditionNode(testReadyCond))
			d := newDegradedDriver(t, client, reg, tc.declared, true)
			plugin := &mockKubeletPlugin{}
			d.draPlugin = plugin
			published := func() [][]string {
				t.Helper()
				pool, ok := plugin.publishedResources.Pools[testNodeName]
				require.True(t, ok, "the node's pool is published")
				var names [][]string
				for _, slice := range pool.Slices {
					var slice1 []string
					for _, dev := range slice.Devices {
						slice1 = append(slice1, dev.Name)
					}
					names = append(names, slice1)
				}
				return names
			}
			condition := func() string {
				t.Helper()
				node, err := client.CoreV1().Nodes().Get(t.Context(), testNodeName, metav1.GetOptions{})
				require.NoError(t, err)
				_, cond := nodeutil.GetNodeCondition(&node.Status, conditionPartitionDegraded)
				require.NotNil(t, cond)
				return cond.Message
			}

			d.PublishResources(t.Context())
			require.Equal(t, tc.wantBefore, published())
			require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
			require.Equal(t, tc.wantCondition, condition())

			d.HandleError(t.Context(), asTheLibraryReports(&resourceslice.DroppedFieldsError{
				PoolName: testNodeName, DesiredSlice: droppedTaintSlice(true, false), ActualSlice: droppedTaintSlice(false, false),
			}), "publishing ResourceSlices failed")
			require.Equal(t, tc.wantBefore, published(), "HandleError publishes nothing")
			require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
			require.Equal(t, tc.wantAfter, published())
			require.Equal(t, tc.wantCondition, condition(), "the condition's message stays")
			require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_device_taints_dropped_total", nil))

			plugin.publishedResources = nil
			require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
			require.Nil(t, plugin.publishedResources, "the worker withholds once")
			require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_device_taints_dropped_total", nil))
			d.PublishResources(t.Context())
			require.Equal(t, tc.wantAfter, published(), "every later publish withholds them")
		})
	}
}

// HandleError takes no lock while Prepare reads the flag it sets; run with
// -race.
func TestHandleErrorBesidePrepare(t *testing.T) {
	d := newDegradedDriver(t, fake.NewClientset(testConditionNode(testReadyCond)), nil, spdkDegraded, true)
	drop := asTheLibraryReports(&resourceslice.DroppedFieldsError{
		PoolName: testNodeName, DesiredSlice: droppedTaintSlice(true, false), ActualSlice: droppedTaintSlice(false, false),
	})
	prepare := func(uid types.UID) (error, error) {
		claim := degradedClaim(uid, "cpudevnuma000-spdk", "vcpu", exactlyTolerating("dra.cpu.spdk", degradedToleration))
		results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
		return err, results[uid].Err
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	callErrs, claimErrs := make([]error, 8), make([]error, 8)
	for i := range claimErrs {
		wg.Go(func() {
			<-start
			callErrs[i], claimErrs[i] = prepare(types.UID(fmt.Sprintf("claim-%d", i)))
		})
	}
	wg.Go(func() {
		<-start
		d.HandleError(t.Context(), drop, "publishing ResourceSlices failed")
	})
	close(start)
	wg.Wait()

	require.True(t, d.deviceTaintsDropped.Load())
	prepared := 0
	for i := range claimErrs {
		require.NoError(t, callErrs[i])
		if claimErrs[i] == nil {
			prepared++
		}
	}
	require.LessOrEqual(t, prepared, 1, "the partition has one CPU")
	_, err := prepare("claim-after")
	require.EqualError(t, err, `request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline) and its devices are not published; take its sibling threads offline`)
}

func TestPartitionDegradedConditionWithTheTaint(t *testing.T) {
	client := fake.NewClientset(testConditionNode(testReadyCond))
	d := newDegradedDriver(t, client, nil, spdkDegraded, true)
	d.nodeConditions.now = func() time.Time { return testClock }
	require.NoError(t, d.nodeConditions.sync(t.Context(), testr.New(t)))
	node, err := client.CoreV1().Nodes().Get(t.Context(), testNodeName, metav1.GetOptions{})
	require.NoError(t, err)
	_, got := nodeutil.GetNodeCondition(&node.Status, conditionPartitionDegraded)
	require.NotNil(t, got)
	require.Equal(t, corev1.ConditionTrue, got.Status)
	require.Equal(t, "SiblingThreadsOnline", got.Reason)
	require.Equal(t, `partitions are degraded while the kernel contradicts their smt: partition "spdk" expects 1 online thread per core, and CPUs 5 of its cores are online too; take those CPUs offline at boot, then restart the driver`, got.Message)
}
