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
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/workqueue"
	nodeutil "k8s.io/component-helpers/node/util"
)

const testConditionType corev1.NodeConditionType = "ExampleCondition"

var (
	testClock      = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	testEarlier    = metav1.NewTime(testClock.Add(-time.Hour))
	testReadyCond  = corev1.NodeCondition{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", Message: "kubelet is posting ready status", LastHeartbeatTime: testEarlier, LastTransitionTime: testEarlier}
	testConditionV = nodeConditionValue{status: corev1.ConditionFalse, reason: "AllWell", message: "nothing to report"}
)

func testConditionNode(conds ...corev1.NodeCondition) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testNodeName},
		Status:     corev1.NodeStatus{Conditions: conds},
	}
}

func testCondition(status corev1.ConditionStatus, reason, message string, transition metav1.Time) corev1.NodeCondition {
	return corev1.NodeCondition{Type: testConditionType, Status: status, Reason: reason, Message: message, LastHeartbeatTime: testEarlier, LastTransitionTime: transition}
}

// conditionHarness runs a writer with one source over a fake API server. The
// source's value is set under the same lock the writer reads it under.
type conditionHarness struct {
	t       *testing.T
	client  *fake.Clientset
	writer  *nodeConditionWriter
	state   *sync.Mutex
	value   nodeConditionValue
	entered chan struct{}
}

func newConditionHarness(t *testing.T, node *corev1.Node) *conditionHarness {
	h := &conditionHarness{t: t, client: fake.NewClientset(node), state: &sync.Mutex{}, value: testConditionV, entered: make(chan struct{})}
	source := nodeConditionSource{
		conditionType: testConditionType,
		evaluate: func() nodeConditionValue {
			if h.state.TryLock() {
				h.state.Unlock()
				t.Error("the writer evaluated its condition without the state lock")
			}
			return h.value
		},
		stoppedMessage: "driver stopped; nothing checked",
	}
	h.client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !h.state.TryLock() {
			t.Error("the writer patched its node with the state lock held")
			return false, nil, nil
		}
		h.state.Unlock()
		return false, nil, nil
	})
	h.writer = newNodeConditionWriter(h.client, testNodeName, h.state, []nodeConditionSource{source},
		workqueue.NewTypedItemExponentialFailureRateLimiter[string](time.Millisecond, 10*time.Millisecond))
	h.writer.now = func() time.Time { return testClock }
	t.Cleanup(h.writer.queue.ShutDown)
	return h
}

func (h *conditionHarness) setValue(v nodeConditionValue) {
	h.state.Lock()
	defer h.state.Unlock()
	h.value = v
}

func (h *conditionHarness) sync() {
	h.t.Helper()
	require.NoError(h.t, h.writer.sync(h.t.Context(), testr.New(h.t)))
}

// calls counts the writer's GETs of its node and decodes each status patch,
// requiring every patch to carry exactly one condition.
func (h *conditionHarness) calls() (int, []corev1.NodeCondition) {
	h.t.Helper()
	gets := 0
	var patched []corev1.NodeCondition
	for _, action := range h.client.Actions() {
		if action.GetResource().Resource != "nodes" {
			continue
		}
		switch action.GetVerb() {
		case "get":
			require.Equal(h.t, testNodeName, action.(k8stesting.GetAction).GetName())
			gets++
		case "patch":
			a := action.(k8stesting.PatchAction)
			require.Equal(h.t, "status", a.GetSubresource())
			require.Equal(h.t, testNodeName, a.GetName())
			var body struct {
				Status struct {
					Conditions []corev1.NodeCondition `json:"conditions"`
				} `json:"status"`
			}
			require.NoError(h.t, json.Unmarshal(a.GetPatch(), &body))
			require.Len(h.t, body.Status.Conditions, 1, "a patch carries one condition alone: %s", a.GetPatch())
			patched = append(patched, inUTC(body.Status.Conditions)...)
		}
	}
	return gets, patched
}

func (h *conditionHarness) storedConditions() []corev1.NodeCondition {
	h.t.Helper()
	node, err := h.client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("nodes"), "", testNodeName)
	require.NoError(h.t, err)
	return inUTC(node.(*corev1.Node).Status.Conditions)
}

// inUTC reads every time in UTC, as JSON decoding gives the local zone.
func inUTC(conds []corev1.NodeCondition) []corev1.NodeCondition {
	out := make([]corev1.NodeCondition, len(conds))
	for i, c := range conds {
		c.LastHeartbeatTime = metav1.NewTime(c.LastHeartbeatTime.UTC())
		c.LastTransitionTime = metav1.NewTime(c.LastTransitionTime.UTC())
		out[i] = c
	}
	return out
}

func TestNodeConditionWriterComparesBeforePatching(t *testing.T) {
	now := metav1.NewTime(testClock)
	tests := []struct {
		name       string
		onServer   []corev1.NodeCondition
		fromWatch  *corev1.Node
		value      nodeConditionValue
		wantPatch  *corev1.NodeCondition
		wantStored []corev1.NodeCondition
	}{
		{
			name:       "a missing condition is patched alone",
			onServer:   []corev1.NodeCondition{testReadyCond},
			value:      testConditionV,
			wantPatch:  &corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: now},
			wantStored: []corev1.NodeCondition{testReadyCond, {Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: now}},
		},
		{
			name:       "a matching condition is left alone",
			onServer:   []corev1.NodeCondition{testReadyCond, testCondition(corev1.ConditionFalse, "AllWell", "nothing to report", testEarlier)},
			value:      testConditionV,
			wantStored: []corev1.NodeCondition{testReadyCond, testCondition(corev1.ConditionFalse, "AllWell", "nothing to report", testEarlier)},
		},
		{
			name:      "a changed message keeps the transition time",
			onServer:  []corev1.NodeCondition{testCondition(corev1.ConditionFalse, "AllWell", "an older message", testEarlier)},
			value:     testConditionV,
			wantPatch: &corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: testEarlier},
		},
		{
			name:      "a changed reason keeps the transition time",
			onServer:  []corev1.NodeCondition{testCondition(corev1.ConditionFalse, "OlderReason", "nothing to report", testEarlier)},
			value:     testConditionV,
			wantPatch: &corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: testEarlier},
		},
		{
			name:      "a changed status moves the transition time",
			onServer:  []corev1.NodeCondition{testCondition(corev1.ConditionTrue, "AllWell", "nothing to report", testEarlier)},
			value:     testConditionV,
			wantPatch: &corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: now},
		},
		{
			name:      "an informer's Node before the writer's own read is not compared",
			onServer:  nil,
			fromWatch: testConditionNode(testCondition(corev1.ConditionFalse, "AllWell", "nothing to report", testEarlier)),
			value:     testConditionV,
			wantPatch: &corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: now},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newConditionHarness(t, testConditionNode(tc.onServer...))
			h.setValue(tc.value)
			if tc.fromWatch != nil {
				h.writer.nodeChanged(tc.fromWatch)
			}

			h.sync()

			gets, patched := h.calls()
			require.Equal(t, 1, gets, "the first comparison is against the writer's own GET")
			if tc.wantPatch == nil {
				require.Empty(t, patched)
			} else {
				require.Equal(t, []corev1.NodeCondition{*tc.wantPatch}, patched)
			}
			if tc.wantStored != nil {
				require.ElementsMatch(t, tc.wantStored, h.storedConditions())
			}
		})
	}
}

func TestNodeConditionWriterComparesLaterNodes(t *testing.T) {
	now := metav1.NewTime(testClock)
	written := corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionFalse, Reason: "AllWell", Message: "nothing to report", LastHeartbeatTime: now, LastTransitionTime: now}
	tests := []struct {
		name      string
		fromWatch *corev1.Node
		wantPatch bool
	}{
		{
			name:      "without an informer, the Node the patch returned",
			wantPatch: false,
		},
		{
			name:      "an informer's update that carries the condition",
			fromWatch: testConditionNode(testReadyCond, written),
			wantPatch: false,
		},
		{
			name:      "an informer's recreated Node without the condition",
			fromWatch: testConditionNode(testReadyCond),
			wantPatch: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newConditionHarness(t, testConditionNode(testReadyCond))
			h.sync()
			if tc.fromWatch != nil {
				h.writer.nodeChanged(tc.fromWatch)
			}

			h.sync()

			gets, patched := h.calls()
			require.Equal(t, 1, gets, "the writer reads the Node once")
			want := []corev1.NodeCondition{written}
			if tc.wantPatch {
				want = append(want, written)
			}
			require.Equal(t, want, patched)
		})
	}
}

func TestNodeConditionWriterWorker(t *testing.T) {
	tests := []struct {
		name        string
		failGets    int32
		failPatches int32
	}{
		{name: "writes once the driver is in service"},
		{name: "retries a failed read", failGets: 2},
		{name: "retries a failed patch", failPatches: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newConditionHarness(t, testConditionNode(testReadyCond))
			var gets, patches atomic.Int32
			h.client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				if gets.Add(1) <= tc.failGets {
					return true, nil, errors.New("injected read failure")
				}
				return false, nil, nil
			})
			h.client.PrependReactor("patch", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
				if patches.Add(1) <= tc.failPatches {
					return true, nil, errors.New("injected patch failure")
				}
				return false, nil, nil
			})
			h.writer.run(t.Context(), h.entered)
			t.Cleanup(h.writer.stop)

			time.Sleep(50 * time.Millisecond)
			require.Empty(t, h.client.Actions(), "nothing is read or written before the driver is in service")

			close(h.entered)
			require.Eventually(t, func() bool {
				_, cond := nodeutil.GetNodeCondition(&corev1.NodeStatus{Conditions: h.storedConditions()}, testConditionType)
				return cond != nil
			}, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, tc.failGets+1, gets.Load())
			require.Equal(t, tc.failPatches+1, patches.Load())
		})
	}
}

func TestNodeConditionWriterWritesUnknownWhenStopped(t *testing.T) {
	now := metav1.NewTime(testClock)
	stopped := corev1.NodeCondition{Type: testConditionType, Status: corev1.ConditionUnknown, Reason: "DriverStopped", Message: "driver stopped; nothing checked", LastHeartbeatTime: now, LastTransitionTime: now}
	tests := []struct {
		name      string
		enter     bool
		failGets  bool
		wantLast  *corev1.NodeCondition
		wantCalls bool
	}{
		{name: "after the first write", enter: true, wantLast: &stopped, wantCalls: true},
		{name: "after a read that never answered", enter: true, failGets: true, wantLast: &stopped, wantCalls: true},
		{name: "before the driver was in service", enter: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newConditionHarness(t, testConditionNode(testReadyCond))
			if tc.failGets {
				h.client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("injected read failure")
				})
			}
			h.writer.run(t.Context(), h.entered)
			if tc.enter {
				close(h.entered)
				require.Eventually(t, func() bool {
					gets, patched := h.calls()
					return len(patched) > 0 || (tc.failGets && gets > 0)
				}, 5*time.Second, 5*time.Millisecond)
			}

			h.writer.stop()
			h.writer.writeStopped()

			_, patched := h.calls()
			if !tc.wantCalls {
				require.Empty(t, h.client.Actions())
				return
			}
			require.NotEmpty(t, patched, "the shutdown writes the condition")
			require.Equal(t, *tc.wantLast, patched[len(patched)-1])
		})
	}
}

func TestNodeConditionWriterWithoutConditionsCallsNothing(t *testing.T) {
	client := fake.NewClientset(testConditionNode())
	w := newNodeConditionWriter(client, testNodeName, &sync.Mutex{}, nil, workqueue.DefaultTypedControllerRateLimiter[string]())
	entered := make(chan struct{})
	w.run(t.Context(), entered)
	close(entered)
	w.nodeChanged(testConditionNode())
	w.stop()
	w.writeStopped()
	require.Empty(t, client.Actions())
}
