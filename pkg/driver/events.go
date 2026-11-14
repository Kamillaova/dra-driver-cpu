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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
)

// newEventRecorder returns a recorder whose events the broadcaster sends to
// the API server from its own goroutine, so that no hook waits on the API
// server to post one. The broadcaster logs through ctx's logger and runs until
// Shutdown, not until ctx is cancelled.
//
// Every event names the node as its source host: the chart's admission policy
// admits an event from the driver's service account only on its own node.
func newEventRecorder(ctx context.Context, client kubernetes.Interface, driverName, nodeName string) (record.EventBroadcaster, record.EventRecorder) {
	broadcaster := record.NewBroadcaster(record.WithContext(context.WithoutCancel(ctx)))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	return broadcaster, broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: driverName, Host: nodeName})
}
