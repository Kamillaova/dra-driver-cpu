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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNewEventRecorder(t *testing.T) {
	tests := []struct {
		name      string
		object    runtime.Object
		posts     int
		namespace string
		wantKind  string
		wantCount int32
	}{
		{
			name:      "an event on a pod",
			object:    &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vm", UID: "pod-uid"}},
			posts:     1,
			namespace: "ns",
			wantKind:  "Pod",
			wantCount: 1,
		},
		{
			name:      "an event on the node",
			object:    &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName, UID: "node-uid"}},
			posts:     1,
			namespace: metav1.NamespaceDefault,
			wantKind:  "Node",
			wantCount: 1,
		},
		{
			name:      "an event repeated, which the recorder patches",
			object:    &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "vm", UID: "pod-uid"}},
			posts:     2,
			namespace: "ns",
			wantKind:  "Pod",
			wantCount: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset()
			broadcaster, recorder := newEventRecorder(t.Context(), client, testDriverName, testNodeName)
			t.Cleanup(broadcaster.Shutdown)

			var events *corev1.EventList
			for range tc.posts {
				recorder.Event(tc.object, corev1.EventTypeWarning, "TestReason", "a test event")
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					var err error
					events, err = client.CoreV1().Events(tc.namespace).List(t.Context(), metav1.ListOptions{})
					if !assert.NoError(c, err) || !assert.Len(c, events.Items, 1) {
						return
					}
					assert.GreaterOrEqual(c, events.Items[0].Count, int32(1))
				}, 5*time.Second, 10*time.Millisecond, "the event reaches the API server")
			}
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				var err error
				events, err = client.CoreV1().Events(tc.namespace).List(t.Context(), metav1.ListOptions{})
				if assert.NoError(c, err) && assert.Len(c, events.Items, 1) {
					assert.Equal(c, tc.wantCount, events.Items[0].Count)
				}
			}, 5*time.Second, 10*time.Millisecond, "the event reaches the API server %d times", tc.posts)

			event := events.Items[0]
			require.Equal(t, corev1.EventSource{Component: testDriverName, Host: testNodeName}, event.Source, "the policy admits the event by its source host")
			require.Equal(t, testNodeName, event.ReportingInstance)
			require.Equal(t, tc.wantKind, event.InvolvedObject.Kind)
			require.Equal(t, corev1.EventTypeWarning, event.Type)
		})
	}
}
