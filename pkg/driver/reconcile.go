/*
Copyright 2026 The Kubernetes Authors.

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
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"k8s.io/apimachinery/pkg/types"
)

// containerUpdater pushes container updates the runtime did not ask for. It is
// the one method this driver needs from the NRI stub, narrowed so tests can
// substitute it.
type containerUpdater interface {
	UpdateContainers([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error)
}

// runReconcileWorker services reconcile requests until ctx is done.
//
// Reconciling happens here rather than inline in the hook that notices the need,
// for two reasons: kubelet's UnprepareResourceClaims must not block on an NRI
// round trip, and issuing an unsolicited update from inside a hook is how the
// runtime-side lock inversion behind containerd/nri#301 deadlocks - the runtime
// may be holding the lock our update needs in order to call us.
func (cp *CPUDriver) runReconcileWorker(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	logger.V(2).Info("reconcile worker started")
	defer logger.V(2).Info("reconcile worker stopped")

	// CCX-FORK: the worker also runs defragmentation passes, on the same trigger
	// plus a ticker for a node nothing else disturbs.
	var tick <-chan time.Time
	if cp.defrag.enabled && cp.defrag.interval > 0 {
		ticker := time.NewTicker(cp.defrag.interval)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.reconcileTrigger:
			// A pass narrows shared containers off the CPUs it is moving claims
			// onto, so it needs them widened again once the claims those CPUs
			// belonged to have vacated. That makes the widening part of finishing
			// a move, not only a service the unprepare reconcile offers.
			if cp.reconcileSharedOnUnprepare || cp.defrag.enabled {
				cp.reconcileSharedContainers(ctx)
			}
			cp.defragPass(ctx)
		case <-tick:
			cp.defragPass(ctx)
		}
	}
}

// requestReconcile asks the worker for a pass. Requests coalesce: the trigger
// holds a single pending token, so a burst of releases costs one pass rather than
// one per release. Never blocks, so it is safe to call from a kubelet or NRI hook.
func (cp *CPUDriver) requestReconcile() {
	if cp.reconcileTrigger == nil {
		return
	}
	select {
	case cp.reconcileTrigger <- struct{}{}:
	default:
	}
}

// reconcileSharedContainers widens running shared containers onto every CPU the
// shared pool currently holds.
//
// Releasing a claim returns its CPUs to the pool immediately, but the containers
// entitled to them keep the narrower cpuset until their next CreateContainer or a
// driver restart. This closes that window.
//
// Widening is the safest possible unsolicited update: it only ever adds CPUs a
// container is already entitled to, so a partial failure leaves that container on
// a narrower-than-entitled mask, which is exactly the behaviour without this
// reconcile. Nothing is released on the strength of the reply, so no exclusivity
// guarantee rests on it.
func (cp *CPUDriver) reconcileSharedContainers(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	if cp.containerUpdater == nil {
		return
	}
	// A static pool never changes, so there is never anything to converge onto:
	// shared containers were pinned to it at creation and stay there.
	if !cp.sharedPool.IsEmpty() {
		return
	}

	// Read the pool under the lock and let go of it before calling out: holding
	// it across the update is the deadlock applyMu's contract forbids.
	cp.applyMu.Lock()
	updates, err := cp.getSharedContainerUpdates(logger, types.UID(""))
	cp.applyMu.Unlock()
	if err != nil {
		logger.Error(err, "cannot reconcile shared containers")
		return
	}
	if len(updates) == 0 {
		return
	}

	logger.V(2).Info("reconciling shared containers after claim release", "numContainers", len(updates))
	failed, err := cp.containerUpdater.UpdateContainers(updates)
	if err != nil {
		// Not fatal: the containers stay on their current, narrower cpuset and
		// pick up the wider one at their next lifecycle event.
		logger.Error(err, "shared container reconcile did not complete", "numFailed", len(failed))
		return
	}
	for _, update := range failed {
		logger.Info("runtime refused a shared container update", "containerID", update.GetContainerId())
	}
}
