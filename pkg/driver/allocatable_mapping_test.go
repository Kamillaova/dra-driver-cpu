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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/util/workqueue"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceslice"
)

type mappingDropCounter struct {
	Recorder
	dropped atomic.Int32
}

func (r *mappingDropCounter) RecordNodeAllocatableMappingDropped() { r.dropped.Add(1) }

func testMappedSlice(mapped bool, taints int) *resourceapi.ResourceSlice {
	dev := resourceapi.Device{Name: "cpudevnuma000"}
	if mapped {
		dev.NodeAllocatableResources = map[corev1.ResourceName]resourceapi.NodeAllocatableResource{
			corev1.ResourceCPU: {Mapping: &resourceapi.NodeAllocatableMapping{DeviceMultiplier: new(resource.MustParse("1"))}},
		}
	}
	for range taints {
		dev.Taints = append(dev.Taints, resourceapi.DeviceTaint{Key: "example.com/taint", Effect: resourceapi.DeviceTaintEffectNoSchedule})
	}
	return &resourceapi.ResourceSlice{Spec: resourceapi.ResourceSliceSpec{Devices: []resourceapi.Device{dev}}}
}

// asTheLibraryReports wraps err as the plugin library hands every
// ResourceSlice error to HandleError: recoverable.
func asTheLibraryReports(err error) error {
	return fmt.Errorf("%w: %w", kubeletplugin.ErrRecoverable, err)
}

func newMappingConditionDriver(t *testing.T, client *fake.Clientset, recorder Recorder) *CPUDriver {
	t.Helper()
	cp := &CPUDriver{nodeName: testNodeName, metrics: recorder}
	cp.nodeConditions = newNodeConditionWriter(client, testNodeName, &cp.stateMu, []nodeConditionSource{cp.allocatableMappingCondition()},
		workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Millisecond, 10*time.Millisecond))
	cp.nodeConditions.now = func() time.Time { return testClock }
	t.Cleanup(cp.nodeConditions.queue.ShutDown)
	return cp
}

func TestHandleErrorReportsDroppedMapping(t *testing.T) {
	const libraryMsg = "publishing ResourceSlices failed"
	tests := []struct {
		name        string
		err         error
		wantDropped bool
		wantMsg     string
		wantFields  []string
	}{
		{
			name:        "the stored slice lost the mapping",
			err:         &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: testMappedSlice(true, 0), ActualSlice: testMappedSlice(false, 0)},
			wantDropped: true,
			wantMsg:     "API server dropped nodeAllocatableResources from a ResourceSlice, so dra.cpu claims are not counted against node cpu; enable DRANodeAllocatableResources on the API server, then restart the driver pod",
			wantFields:  []string{`"message"="publishing ResourceSlices failed"`, `"pool"="test-node"`, `"slice"=0`, `"condition"="CPUAllocatableMappingDropped"`},
		},
		{
			name:    "another field dropped, the mapping kept",
			err:     &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: testMappedSlice(true, 1), ActualSlice: testMappedSlice(true, 0)},
			wantMsg: libraryMsg,
		},
		{
			name:    "another field dropped, no mapping published",
			err:     &resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: testMappedSlice(false, 1), ActualSlice: testMappedSlice(false, 0)},
			wantMsg: libraryMsg,
		},
		{
			name:    "not a dropped field",
			err:     errors.New("update ResourceSlice: the server was unable to return a response in the time allotted"),
			wantMsg: libraryMsg,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCtx, cancelRun := context.WithCancelCause(t.Context())
			defer cancelRun(nil)
			recorder := &mappingDropCounter{Recorder: cpumetrics.Noop()}
			cp := newMappingConditionDriver(t, fake.NewClientset(), recorder)
			cp.cancelRun = cancelRun
			plugin := &mockKubeletPlugin{stopFunc: func() { t.Error("HandleError stopped the DRA plugin") }}
			cp.draPlugin = plugin
			logs := &logLines{}
			ctx := logr.NewContext(t.Context(), logs.logger())
			reported := asTheLibraryReports(tc.err)

			cp.stateMu.Lock()
			done := make(chan struct{})
			go func() {
				defer close(done)
				cp.HandleError(ctx, reported, libraryMsg)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("HandleError waited on the driver's state lock")
			}
			cp.stateMu.Unlock()

			require.NoError(t, runCtx.Err(), "a dropped field must not stop the driver")
			require.Nil(t, plugin.publishedResources, "HandleError publishes nothing")
			require.Equal(t, tc.wantDropped, cp.allocatableMappingDropped.Load())
			wantCount, wantKeys := 0, 0
			if tc.wantDropped {
				wantCount, wantKeys = 1, 1
			}
			require.EqualValues(t, wantCount, recorder.dropped.Load())
			require.Equal(t, wantKeys, cp.nodeConditions.queue.Len(), "a drop puts one key on the writer's queue")

			require.Len(t, logs.lines, 1, "HandleError logs the error once")
			require.Len(t, logs.matching(tc.wantMsg), 1)
			line := logs.lines[0]
			require.Contains(t, line, fmt.Sprintf("%q=%q", "error", reported.Error()))
			for _, field := range tc.wantFields {
				require.Contains(t, line, field)
			}
			if tc.wantFields == nil {
				require.NotContains(t, line, `"message"=`, "any other error is logged with the library's message alone")
			}
		})
	}
}

func TestAllocatableMappingConditionAfterADrop(t *testing.T) {
	now := metav1.NewTime(testClock)
	published := corev1.NodeCondition{Type: conditionAllocatableMappingDropped, Status: corev1.ConditionFalse, Reason: "MappingPublished",
		Message: "nodeAllocatableResources kept in dra.cpu ResourceSlices since the driver started", LastHeartbeatTime: now, LastTransitionTime: now}
	dropped := corev1.NodeCondition{Type: conditionAllocatableMappingDropped, Status: corev1.ConditionTrue, Reason: "DroppedByAPIServer",
		Message:           "API server dropped nodeAllocatableResources from dra.cpu ResourceSlices, so dra.cpu claims are not counted against node cpu; enable DRANodeAllocatableResources on the API server, then restart the driver pod",
		LastHeartbeatTime: now, LastTransitionTime: now}
	stopped := corev1.NodeCondition{Type: conditionAllocatableMappingDropped, Status: corev1.ConditionUnknown, Reason: "DriverStopped",
		Message: "driver stopped; mapping not checked", LastHeartbeatTime: now, LastTransitionTime: now}
	drop := asTheLibraryReports(&resourceslice.DroppedFieldsError{PoolName: testNodeName, DesiredSlice: testMappedSlice(true, 0), ActualSlice: testMappedSlice(false, 0)})

	tests := []struct {
		name            string
		dropBeforeEntry bool
		want            []corev1.NodeCondition
	}{
		{
			name: "a drop after the first write turns the condition True",
			want: []corev1.NodeCondition{published, dropped, stopped},
		},
		{
			name:            "a drop before the driver is in service is the first write",
			dropBeforeEntry: true,
			want:            []corev1.NodeCondition{dropped, stopped},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset(testConditionNode(testReadyCond))
			cp := newMappingConditionDriver(t, client, cpumetrics.Noop())
			h := &conditionHarness{t: t, client: client}
			entered := make(chan struct{})
			cp.nodeConditions.run(t.Context(), entered)
			t.Cleanup(cp.nodeConditions.stop)

			if tc.dropBeforeEntry {
				cp.HandleError(t.Context(), drop, "publishing ResourceSlices failed")
			}
			close(entered)
			require.Eventually(t, func() bool { _, patched := h.calls(); return len(patched) == 1 }, 5*time.Second, 5*time.Millisecond)
			if !tc.dropBeforeEntry {
				cp.HandleError(t.Context(), drop, "publishing ResourceSlices failed")
				require.Eventually(t, func() bool { _, patched := h.calls(); return len(patched) == 2 }, 5*time.Second, 5*time.Millisecond,
					"no informer runs, so only the drop's key brings the worker back")
			}
			cp.nodeConditions.stop()
			cp.nodeConditions.writeStopped()

			gets, patched := h.calls()
			require.Equal(t, 1, gets)
			require.Equal(t, tc.want, patched)
		})
	}
}

func TestNewKeepsTheMappingConditionWhereTheMappingIsPublished(t *testing.T) {
	for _, publish := range []bool{false, true} {
		t.Run(fmt.Sprintf("publishNodeAllocatableResourceMapping=%t", publish), func(t *testing.T) {
			client := fake.NewClientset(testConditionNode(testReadyCond))
			prov := Providers{
				CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: mockCPUInfos_SingleSocket_4CPUS_HT},
				SysFS:     testSysFS(mockCPUInfos_SingleSocket_4CPUS_HT),
				K8SClient: client,
			}
			cp, err := New(testr.New(t), prov, &Config{DriverName: testDriverName, NodeName: testNodeName, PublishNodeAllocatableResourceMapping: publish})
			require.NoError(t, err)
			entered := make(chan struct{})
			close(entered)
			cp.nodeConditions.run(t.Context(), entered)
			h := &conditionHarness{t: t, client: client}

			if !publish {
				time.Sleep(100 * time.Millisecond)
				cp.nodeConditions.stop()
				cp.nodeConditions.writeStopped()
				require.Empty(t, client.Actions(), "without the mapping the driver never reads or writes its Node")
				return
			}
			t.Cleanup(cp.nodeConditions.stop)
			require.Eventually(t, func() bool {
				_, cond := nodeutil.GetNodeCondition(&corev1.NodeStatus{Conditions: h.storedConditions()}, conditionAllocatableMappingDropped)
				return cond != nil && cond.Status == corev1.ConditionFalse
			}, 5*time.Second, 5*time.Millisecond)
		})
	}
}
