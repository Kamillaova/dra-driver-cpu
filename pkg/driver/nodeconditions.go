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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/workqueue"
	nodeutil "k8s.io/component-helpers/node/util"
)

const (
	nodeConditionGetTimeout   = 10 * time.Second
	nodeConditionPatchTimeout = 10 * time.Second
	// nodeConditionStopTimeout is the one budget every Unknown write of the
	// shutdown shares.
	nodeConditionStopTimeout   = 2 * time.Second
	nodeConditionStoppedReason = "DriverStopped"
)

// nodeConditionSource is one condition the driver keeps on its Node.
type nodeConditionSource struct {
	conditionType corev1.NodeConditionType
	// evaluate runs with stateMu held.
	evaluate func() nodeConditionValue
	// stoppedMessage is written with status Unknown when the driver stops.
	stoppedMessage string
}

type nodeConditionValue struct {
	status  corev1.ConditionStatus
	reason  string
	message string
}

// nodeConditionWriter keeps the driver's conditions on its Node. One worker
// makes every API call, so that no hook or handler waits on the API server:
// its first Node is a GET of its own, made once the driver is registered and
// connected to NRI; after it, the Nodes its patches return and those an
// informer hands to nodeChanged. A condition is patched alone, and only when
// its status, reason or message differ from the Node's copy.
type nodeConditionWriter struct {
	client   kubernetes.Interface
	nodeName string
	sources  []nodeConditionSource
	state    sync.Locker
	now      func() time.Time
	queue    workqueue.TypedRateLimitingInterface[string]

	mu      sync.Mutex
	node    *corev1.Node
	started bool

	base   context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// newNodeConditionWriter returns a writer that reads the sources under state.
// With no sources it starts nothing and never calls the API server.
func newNodeConditionWriter(client kubernetes.Interface, nodeName string, state sync.Locker, sources []nodeConditionSource, limiter workqueue.TypedRateLimiter[string]) *nodeConditionWriter {
	w := &nodeConditionWriter{
		client:   client,
		nodeName: nodeName,
		sources:  sources,
		state:    state,
		now:      time.Now,
	}
	if len(sources) > 0 {
		w.queue = workqueue.NewTypedRateLimitingQueue(limiter)
	}
	return w
}

// run starts the worker, which waits until entered is closed. The worker runs
// until stop, not until ctx is cancelled, so that it stops in the shutdown's
// order.
func (w *nodeConditionWriter) run(ctx context.Context, entered <-chan struct{}) {
	if w == nil || len(w.sources) == 0 {
		return
	}
	w.base = context.WithoutCancel(ctx)
	ctx, w.cancel = context.WithCancel(w.base)
	w.done = make(chan struct{})
	go func() {
		defer close(w.done)
		select {
		case <-ctx.Done():
			return
		case <-entered:
		}
		w.mu.Lock()
		w.started = true
		w.mu.Unlock()
		w.queue.Add(w.nodeName)
		for w.processNext(ctx) {
		}
	}()
}

// nodeChanged hands the writer a Node an informer saw. It is ignored until the
// writer's own GET has answered.
func (w *nodeConditionWriter) nodeChanged(node *corev1.Node) {
	if w == nil || len(w.sources) == 0 || node.Name != w.nodeName {
		return
	}
	w.mu.Lock()
	if w.node == nil {
		w.mu.Unlock()
		return
	}
	w.node = node
	w.mu.Unlock()
	w.queue.Add(w.nodeName)
}

// stop stops the worker and waits for it, ending a call in flight.
func (w *nodeConditionWriter) stop() {
	if w == nil || w.done == nil {
		return
	}
	w.cancel()
	w.queue.ShutDown()
	<-w.done
}

// writeStopped writes every condition Unknown, within one budget, once the
// worker has stopped. A driver that never got as far as the worker's first
// write leaves the Node as it was.
func (w *nodeConditionWriter) writeStopped() {
	if w == nil || w.done == nil {
		return
	}
	w.mu.Lock()
	started, node := w.started, w.node
	w.mu.Unlock()
	if !started {
		return
	}
	ctx, cancel := context.WithTimeout(w.base, nodeConditionStopTimeout)
	defer cancel()
	logger := ctxlog.FromContext(ctx)
	for _, src := range w.sources {
		value := nodeConditionValue{status: corev1.ConditionUnknown, reason: nodeConditionStoppedReason, message: src.stoppedMessage}
		cond, write := w.conditionFor(node, src.conditionType, value)
		if !write {
			continue
		}
		patched, err := w.patch(ctx, logger, cond)
		if err != nil {
			logger.Error(err, "node condition left as it was at shutdown", "node", w.nodeName, "condition", src.conditionType)
			continue
		}
		node = patched
	}
}

func (w *nodeConditionWriter) processNext(ctx context.Context) bool {
	key, quit := w.queue.Get()
	if quit {
		return false
	}
	defer w.queue.Done(key)
	logger := ctxlog.FromContext(ctx)
	if err := w.sync(ctx, logger); err != nil {
		if ctx.Err() != nil {
			return true
		}
		logger.Error(err, "node conditions not written, retrying with backoff", "node", w.nodeName, "retries", w.queue.NumRequeues(key))
		w.queue.AddRateLimited(key)
		return true
	}
	w.queue.Forget(key)
	return true
}

func (w *nodeConditionWriter) sync(ctx context.Context, logger logr.Logger) error {
	node, err := w.currentNode(ctx)
	if err != nil {
		return err
	}
	values := w.evaluate()
	for i, src := range w.sources {
		cond, write := w.conditionFor(node, src.conditionType, values[i])
		if !write {
			continue
		}
		if node, err = w.patch(ctx, logger, cond); err != nil {
			return err
		}
	}
	return nil
}

// currentNode is the last Node the writer saw, or its own GET of the Node
// before it has seen one.
func (w *nodeConditionWriter) currentNode(ctx context.Context) (*corev1.Node, error) {
	w.mu.Lock()
	node := w.node
	w.mu.Unlock()
	if node != nil {
		return node, nil
	}
	ctx, cancel := context.WithTimeout(ctx, nodeConditionGetTimeout)
	defer cancel()
	node, err := w.client.CoreV1().Nodes().Get(ctx, w.nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get node %q: %w", w.nodeName, err)
	}
	w.mu.Lock()
	if w.node == nil {
		w.node = node
	}
	w.mu.Unlock()
	return node, nil
}

func (w *nodeConditionWriter) evaluate() []nodeConditionValue {
	w.state.Lock()
	defer w.state.Unlock()
	values := make([]nodeConditionValue, len(w.sources))
	for i, src := range w.sources {
		values[i] = src.evaluate()
	}
	return values
}

// conditionFor reports the condition to write, if node's copy differs in
// status, reason or message. lastTransitionTime stays while the status holds,
// as the kubelet keeps it for its own conditions. A nil node is one the writer
// never read, so the condition is written.
func (w *nodeConditionWriter) conditionFor(node *corev1.Node, conditionType corev1.NodeConditionType, value nodeConditionValue) (corev1.NodeCondition, bool) {
	now := metav1.NewTime(w.now())
	cond := corev1.NodeCondition{
		Type:               conditionType,
		Status:             value.status,
		Reason:             value.reason,
		Message:            value.message,
		LastHeartbeatTime:  now,
		LastTransitionTime: now,
	}
	if node == nil {
		return cond, true
	}
	_, current := nodeutil.GetNodeCondition(&node.Status, conditionType)
	if current == nil {
		return cond, true
	}
	if current.Status == value.status && current.Reason == value.reason && current.Message == value.message {
		return corev1.NodeCondition{}, false
	}
	if current.Status == value.status {
		cond.LastTransitionTime = current.LastTransitionTime
	}
	return cond, true
}

// patch sends the body nodeutil.SetNodeCondition sends, a strategic merge
// patch of the status subresource that leaves every other condition alone,
// under a deadline of its own: SetNodeCondition takes no context.
func (w *nodeConditionWriter) patch(ctx context.Context, logger logr.Logger, cond corev1.NodeCondition) (*corev1.Node, error) {
	body, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"conditions": []corev1.NodeCondition{cond},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encode node condition %s: %w", cond.Type, err)
	}
	ctx, cancel := context.WithTimeout(ctx, nodeConditionPatchTimeout)
	defer cancel()
	node, err := w.client.CoreV1().Nodes().PatchStatus(ctx, w.nodeName, body)
	if err != nil {
		return nil, fmt.Errorf("patch condition %s of node %q: %w", cond.Type, w.nodeName, err)
	}
	w.mu.Lock()
	w.node = node
	w.mu.Unlock()
	logger.V(2).Info("node condition written", "node", w.nodeName, "condition", cond.Type, "status", cond.Status, "reason", cond.Reason)
	return node, nil
}
