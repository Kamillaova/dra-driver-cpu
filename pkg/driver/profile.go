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
	"slices"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// unconfiguredWindow is the driver's NRI state before the node's profile is
// read: no stores and no devices, only the containers the driver will judge
// once it has them.
type unconfiguredWindow struct {
	// synced is closed by the first Synchronize.
	synced chan struct{}
	kept   []keptContainer
}

type keptContainer struct {
	pod *api.PodSandbox
	ctr *api.Container
}

func newUnconfiguredWindow() *unconfiguredWindow {
	return &unconfiguredWindow{synced: make(chan struct{})}
}

// keep replaces the kept containers with the ones a Synchronize lists.
func (w *unconfiguredWindow) keep(pods []*api.PodSandbox, containers []*api.Container) {
	w.kept = w.kept[:0]
	for _, ctr := range containers {
		i := slices.IndexFunc(pods, func(pod *api.PodSandbox) bool { return pod.GetId() == ctr.GetPodSandboxId() })
		if i < 0 {
			continue
		}
		w.kept = append(w.kept, keptContainer{pod: pods[i], ctr: ctr})
	}
	select {
	case <-w.synced:
	default:
		close(w.synced)
	}
}

// admit lets a host-network pod's container without claims start on the
// runtime's own cpuset and keeps it, and refuses every other container, which
// the kubelet creates again later.
func (w *unconfiguredWindow) admit(label string, pod *api.PodSandbox, ctr *api.Container, claims int) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	if claims > 0 {
		return nil, nil, fmt.Errorf("cannot create container %q in pod %q: it names claims, which the driver serves only once it has read the node's %s label; the kubelet creates it again, and the driver's log says why the label is not read yet",
			ctr.GetName(), pod.GetUid(), label)
	}
	if !usesHostNetwork(pod) {
		return nil, nil, fmt.Errorf("cannot create container %q in pod %q: until the driver has read the node's %s label only the containers without claims of host-network pods start; the kubelet creates it again, and the driver's log says why the label is not read yet",
			ctr.GetName(), pod.GetUid(), label)
	}
	w.kept = append(w.kept, keptContainer{pod: pod, ctr: ctr})
	return &api.ContainerAdjustment{}, nil, nil
}

// drop forgets a container that stopped or was removed.
func (w *unconfiguredWindow) drop(id string) {
	w.kept = slices.DeleteFunc(w.kept, func(k keptContainer) bool { return k.ctr.GetId() == id })
}

// lists returns the kept containers and their pods, each pod once, in the
// order they were kept.
func (w *unconfiguredWindow) lists() ([]*api.PodSandbox, []*api.Container) {
	var pods []*api.PodSandbox
	containers := make([]*api.Container, 0, len(w.kept))
	for _, k := range w.kept {
		if !slices.ContainsFunc(pods, func(pod *api.PodSandbox) bool { return pod.GetId() == k.pod.GetId() }) {
			pods = append(pods, k.pod)
		}
		containers = append(containers, k.ctr)
	}
	return pods, containers
}

// usesHostNetwork reports whether the pod shares the host's network namespace.
// containerd lists a sandbox's namespaces from its OCI spec and leaves out a
// network namespace whose path is empty or gone (containerd v2.4.0
// internal/cri/nri/nri_api_linux.go:621-642), so a host-network pod lists none.
func usesHostNetwork(pod *api.PodSandbox) bool {
	return !slices.ContainsFunc(pod.GetLinux().GetNamespaces(), func(ns *api.LinuxNamespace) bool {
		return ns.GetType() == "network"
	})
}

// configureFromProfile waits for the first Synchronize, reads the node's
// profile label, builds the driver from the profile it names and switches the
// NRI hooks out of the window. It returns leaveWindow's updates, or the error
// of an NRI plugin that failed for good before that, read from nriErr.
func (cp *CPUDriver) configureFromProfile(ctx context.Context, nriErr <-chan error) ([]*api.ContainerUpdate, error) {
	logger := ctxlog.FromContext(ctx)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-nriErr:
		return nil, fmt.Errorf("NRI plugin failed before it synchronized: %w", err)
	case <-cp.window.synced:
	}

	name, _, err := cp.awaitProfileLabel(ctx, nriErr)
	if err != nil {
		return nil, err
	}
	config, err := cp.selectProfile(name)
	if err != nil {
		return nil, err
	}
	logger.Info("selected the config profile the node's label names", "label", cp.profileLabel, "profile", name)
	if err := cp.configure(logger, cp.providers, config); err != nil {
		return nil, fmt.Errorf("driver failed to initialize: %w", err)
	}
	cp.seedAllocationStoreFromDisk(logger)
	cp.eventBroadcaster, cp.eventRecorder = newEventRecorder(ctx, cp.kubeClient, cp.driverName, cp.nodeName)
	return cp.leaveWindow(ctx)
}

// refuseProfileLabel waits for the first Synchronize and refuses a node whose
// profile label names a profile the configuration does not declare, before the
// driver registers or publishes anything.
func (cp *CPUDriver) refuseProfileLabel(ctx context.Context, nriErr <-chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-nriErr:
		return fmt.Errorf("NRI plugin failed before it synchronized: %w", err)
	case <-cp.initialSync:
	}
	if cp.profileLabel == "" {
		return nil
	}
	value, labelled, err := cp.awaitProfileLabel(ctx, nriErr)
	if err != nil {
		return err
	}
	if labelled {
		return fmt.Errorf("node label %s=%s names a profile, and the config declares no profiles: declare the profiles the nodes' labels name, or remove the label with kubectl label node %s %s-",
			cp.profileLabel, value, cp.nodeName, cp.profileLabel)
	}
	return nil
}

// awaitProfileLabel reads the node's profile label, waiting for an API server
// that does not answer yet, and reports whether the node carries it.
//
// A DaemonSet starts before the CNI and kube-proxy are ready, so the API
// server's ClusterIP refuses connections for the first seconds of a node's
// life. Every error is retried, because none of the ones this call can return
// is provably permanent during a boot: a 401 while the projected service
// account token is still being written, a 403 while the binding from the same
// manifest bundle has not been applied yet, and a 404 while the node object is
// being recreated all clear on their own. A misconfiguration that does not
// clear is reported by the readiness probe, which stays red for as long as this
// waits, and by a log line per attempt naming the node.
func (cp *CPUDriver) awaitProfileLabel(ctx context.Context, nriErr <-chan error) (string, bool, error) {
	logger := ctxlog.FromContext(ctx)
	for {
		value, labelled, err := cp.readProfileLabel(ctx)
		if err == nil {
			return value, labelled, nil
		}
		logger.Error(err, "waiting for the API server to read the node's profile label", "node", cp.nodeName, "label", cp.profileLabel)
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case err := <-nriErr:
			return "", false, fmt.Errorf("NRI plugin failed before the node's %s label was read: %w", cp.profileLabel, err)
		case <-time.After(cp.profileReadInterval):
		}
	}
}

// readProfileLabel reads the node's profile label with one GET of its own,
// under its own deadline.
func (cp *CPUDriver) readProfileLabel(ctx context.Context) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, cp.profileReadTimeout)
	defer cancel()
	node, err := cp.kubeClient.CoreV1().Nodes().Get(ctx, cp.nodeName, metav1.GetOptions{})
	if err != nil {
		return "", false, fmt.Errorf("read node %q for its %s label: %w", cp.nodeName, cp.profileLabel, err)
	}
	value, labelled := node.Labels[cp.profileLabel]
	return value, labelled, nil
}

// leaveWindow switches the NRI hooks to the configured driver and judges the
// containers kept in the window as the first Synchronize would, in one hold of
// stateMu, so that no container created in between goes unjudged. It returns
// the updates pinning the kept containers that hold claims; the containers
// without claims are left to narrowWindowContainers.
func (cp *CPUDriver) leaveWindow(ctx context.Context) ([]*api.ContainerUpdate, error) {
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	logger := ctxlog.FromContext(ctx)
	pods, containers := cp.window.lists()
	cp.window = nil
	logger.Info("judging the containers kept before the node's profile was read", "numPods", len(pods), "numContainers", len(containers))
	updates, err := cp.judgeContainersLocked(ctx, logger, pods, containers)
	if err != nil {
		return nil, fmt.Errorf("judge the containers kept before the node's profile was read: %w", err)
	}
	claimless := sets.New(cp.podState.GetContainersWithSharedCPUs()...)
	return slices.DeleteFunc(updates, func(update *api.ContainerUpdate) bool {
		return claimless.Has(types.UID(update.GetContainerId()))
	}), nil
}

// narrowWindowContainers sends the kept containers the judgment's cpusets and
// narrows every container without claims to the default partition, before the
// driver registers. Where unsolicited updates are safe it sends them; elsewhere
// it closes the NRI connection once, and the runtime's Synchronize on the
// reconnect judges the containers it lists and carries the updates in its
// reply. It returns the error of an NRI plugin that failed for good meanwhile,
// read from nriErr.
func (cp *CPUDriver) narrowWindowContainers(ctx context.Context, nriErr <-chan error, bound []*api.ContainerUpdate) error {
	logger := ctxlog.FromContext(ctx)
	if cp.containerUpdater != nil {
		if len(bound) > 0 {
			failed, err := cp.containerUpdater.UpdateContainers(bound)
			if err != nil {
				logger.Error(err, "containers holding claims keep the CPUs the runtime gave them until their next update", "numFailed", len(failed))
			}
			for _, update := range failed {
				logger.V(2).Info("the runtime did not update a container holding claims", "containerID", update.GetContainerId())
			}
		}
		cp.reconcileSharedContainers(logger)
		return nil
	}

	cp.stateMu.Lock()
	resynced := make(chan struct{})
	cp.resynced = resynced
	select {
	case <-cp.nriClosed:
	default:
	}
	cp.reconnecting.Store(true)
	cp.stateMu.Unlock()
	logger.Info("reconnecting to NRI so that the runtime's Synchronize narrows the containers without claims")
	cp.nriPlugin.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-nriErr:
		return fmt.Errorf("NRI plugin failed before it synchronized again: %w", err)
	case <-resynced:
		return nil
	}
}
