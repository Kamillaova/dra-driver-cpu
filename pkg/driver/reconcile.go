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
	"maps"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"k8s.io/apimachinery/pkg/types"
)

// containerUpdater sends the runtime container updates it did not ask for.
type containerUpdater interface {
	UpdateContainers([]*api.ContainerUpdate) ([]*api.ContainerUpdate, error)
}

// requestReconcile asks the worker to widen containers without claims onto
// the CPUs an Unprepare freed. Requests coalesce, and it never blocks. A hook
// must not send the update itself: a runtime can hold the lock the update
// needs while it waits for the hook (containerd/nri#301).
func (cp *CPUDriver) requestReconcile() {
	if cp.reconcileTrigger == nil {
		return
	}
	select {
	case cp.reconcileTrigger <- struct{}{}:
	default:
	}
}

func (cp *CPUDriver) runReconcileWorker(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.reconcileTrigger:
			cp.reconcileSharedContainers(logger)
		}
	}
}

// reconcileSharedContainers sends each container without claims the shared
// CPUs, reading them under stateMu and calling the runtime without it. A
// claim's first container can narrow the shared CPUs while an update is on its
// way, so the CPUs are read again after each update and sent again until they
// stand as sent.
func (cp *CPUDriver) reconcileSharedContainers(logger logr.Logger) {
	var sent map[string]string
	for {
		updates, ok := cp.sharedContainerUpdates(logger)
		if !ok || len(updates) == 0 {
			return
		}
		cpusByContainer := make(map[string]string, len(updates))
		for _, update := range updates {
			cpusByContainer[update.GetContainerId()] = update.GetLinux().GetResources().GetCpu().GetCpus()
		}
		if maps.Equal(cpusByContainer, sent) {
			return
		}
		logger.V(2).Info("updating containers without claims", "numContainers", len(updates))
		failed, err := cp.containerUpdater.UpdateContainers(updates)
		if err != nil {
			logger.Error(err, "containers without claims keep their CPUs until their next update", "numFailed", len(failed))
			return
		}
		for _, update := range failed {
			logger.V(2).Info("the runtime did not update a container without claims", "containerID", update.GetContainerId())
		}
		sent = cpusByContainer
	}
}

// sharedContainerUpdates reads the updates under stateMu. It reports none
// while NRI is disconnected: the reconnect's Synchronize sends them in its reply.
func (cp *CPUDriver) sharedContainerUpdates(logger logr.Logger) ([]*api.ContainerUpdate, bool) {
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if !cp.hasSynchronized || cp.nriDisconnected.Load() {
		return nil, false
	}
	updates, err := sharedContainerUpdatesFor(logger, cp.allocationState, cp.podState, types.UID(""))
	if err != nil {
		logger.Error(err, "leaving containers without claims on their current CPUs")
		return nil, false
	}
	holderUpdates, err := shareHolderUpdatesFor(logger, cp.allocationState, cp.podState, types.UID(""))
	if err != nil {
		logger.Error(err, "leaving the containers without claims and those holding shares on their current CPUs")
		return nil, false
	}
	cp.rewriteThreadedShares(logger)
	return append(updates, holderUpdates...), true
}
