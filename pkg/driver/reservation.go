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
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/cpuset"
)

const (
	conditionReservationMissing corev1.NodeConditionType = "CPUReservationMissing"

	// reservationReadTimeout bounds each read of the Node the check makes
	// before the registration, and reservationReadInterval spaces the reads
	// while the API server does not answer.
	reservationReadTimeout  = 10 * time.Second
	reservationReadInterval = 2 * time.Second
)

// reservationRequirement is the kubelet CPU reservation a node's partitions
// need: every online CPU outside the default partition.
type reservationRequirement struct {
	// cpus is the count of CPUs the kubelet must reserve, 0 where the node
	// keeps no CPU from pods without claims.
	cpus int
	// offline is the online threads each partition's smt expects offline, in
	// the partitions' order, which cpus counts while they are online.
	offline []expectedOffline
}

type expectedOffline struct {
	partition string
	cpus      cpuset.CPUSet
}

// requiredReservation counts the online CPUs no pod without a claim runs on:
// every online thread of the cores a named partition lists, and the threads of
// the other cores beyond what the default partition's smt keeps online.
func requiredReservation(details cpuinfo.CPUDetails, partitions []device.Partition, partitionsUsed bool) reservationRequirement {
	var req reservationRequirement
	if !partitionsUsed {
		return req
	}
	named := cpuset.New()
	for _, partition := range partitions {
		if partition.Name == device.DefaultPartitionName {
			continue
		}
		cores := cpuset.New()
		for _, cpuID := range partition.CPUs.List() {
			cores = cores.Union(details.SiblingsOf(cpuID))
		}
		named = named.Union(cores)
		if partition.ThreadsPerCore > 0 {
			if surplus := cores.Difference(partition.CPUs); !surplus.IsEmpty() {
				req.offline = append(req.offline, expectedOffline{partition: partition.Name, cpus: surplus})
			}
		}
	}
	counted := named
	for _, partition := range partitions {
		if partition.Name != device.DefaultPartitionName || partition.ThreadsPerCore == 0 {
			continue
		}
		surplus := cpuset.New()
		kept := make(map[cpuinfo.CoreLocation]int)
		for _, cpuID := range details.CPUs().Difference(named).List() {
			loc, ok := details.CoreOf(cpuID)
			if !ok {
				continue
			}
			if kept[loc] == partition.ThreadsPerCore {
				surplus = surplus.Union(cpuset.New(cpuID))
				continue
			}
			kept[loc]++
		}
		if !surplus.IsEmpty() {
			counted = counted.Union(surplus)
			req.offline = append(req.offline, expectedOffline{partition: partition.Name, cpus: surplus})
		}
	}
	req.cpus = counted.Size()
	return req
}

// afterOffline is the reservation to set once the threads the partitions'
// smt expects offline have gone: the kubelet then counts them in neither its
// capacity nor its reservation.
func (r reservationRequirement) afterOffline() int {
	n := r.cpus
	for _, o := range r.offline {
		n -= o.cpus.Size()
	}
	return n
}

// missingMessage is CPUReservationMissing's message while the reservation is
// below the count, and the error a refused Prepare returns.
func (r reservationRequirement) missingMessage(driverName string) string {
	toSet := strconv.Itoa(r.afterOffline())
	settings := fmt.Sprintf(`kubeReserved: {cpu: %q} or systemReserved: {cpu: %q}`, toSet, toSet)
	if len(r.offline) == 0 {
		return fmt.Sprintf("kubelet CPU reservation below %d: drain the node, then set %s; no %s devices are published until then",
			r.cpus, settings, driverName)
	}
	counting := make([]string, 0, len(r.offline))
	for _, o := range r.offline {
		counting = append(counting, fmt.Sprintf("CPUs %s that partition %q expects offline", o.cpus.String(), o.partition))
	}
	return fmt.Sprintf("kubelet CPU reservation below %d, counting %s: drain the node, take them offline at boot and set %s; no %s devices are published until then",
		r.cpus, strings.Join(counting, " and "), settings, driverName)
}

// reservedMilliCPU is the kubelet's CPU reservation as the Node shows it,
// capacity minus allocatable, in millicores.
func reservedMilliCPU(node *corev1.Node) (int64, error) {
	capacity, ok := node.Status.Capacity[corev1.ResourceCPU]
	if !ok {
		return 0, fmt.Errorf("node %q reports no cpu capacity yet", node.Name)
	}
	allocatable, ok := node.Status.Allocatable[corev1.ResourceCPU]
	if !ok {
		return 0, fmt.Errorf("node %q reports no allocatable cpu yet", node.Name)
	}
	return capacity.MilliValue() - allocatable.MilliValue(), nil
}

// formatMilliCPU writes millicores as CPUs, without trailing zeros.
func formatMilliCPU(milli int64) string {
	return strconv.FormatFloat(float64(milli)/1000, 'f', -1, 64)
}

// checkReservation decides from node whether the kubelet reserves what the
// partitions need, and records the result. It reports whether the result
// changed.
func (cp *CPUDriver) checkReservation(logger logr.Logger, node *corev1.Node) (bool, error) {
	reserved, err := reservedMilliCPU(node)
	if err != nil {
		return false, err
	}
	missing := reserved < int64(cp.reservation.cpus)*1000
	cp.stateMu.Lock()
	cp.reservedMilliCPU = reserved
	changed := cp.reservationMissing.Swap(missing) != missing || !cp.reservationChecked
	cp.reservationChecked = true
	cp.stateMu.Unlock()
	cp.metrics.SetNodeReservationOK(!missing)
	if changed {
		if missing {
			logger.Info("node misses its kubelet CPU reservation: no devices are published and every Prepare is refused until it is set",
				"node", cp.nodeName, "reservedCPUs", formatMilliCPU(reserved), "requiredCPUs", cp.reservation.cpus)
		} else {
			logger.Info("node has its kubelet CPU reservation", "node", cp.nodeName,
				"reservedCPUs", formatMilliCPU(reserved), "requiredCPUs", cp.reservation.cpus)
		}
	}
	return changed, nil
}

// awaitReservation reads the Node with a GET of its own, waiting for an API
// server that does not answer yet, and decides from it whether the node is
// healthy or degraded, before the driver registers. Every error is retried, as
// awaitProfileLabel retries its own read.
func (cp *CPUDriver) awaitReservation(ctx context.Context, nriErr <-chan error) error {
	if cp.reservation.cpus == 0 {
		return nil
	}
	logger := ctxlog.FromContext(ctx)
	for {
		err := cp.readReservation(ctx, logger)
		if err == nil {
			return nil
		}
		logger.Error(err, "waiting for the API server to check the node's kubelet CPU reservation", "node", cp.nodeName)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-nriErr:
			return fmt.Errorf("NRI plugin failed before the node's kubelet CPU reservation was checked: %w", err)
		case <-time.After(cp.reservationReadInterval):
		}
	}
}

func (cp *CPUDriver) readReservation(ctx context.Context, logger logr.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, cp.reservationReadTimeout)
	defer cancel()
	node, err := cp.kubeClient.CoreV1().Nodes().Get(ctx, cp.nodeName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read node %q for its kubelet CPU reservation: %w", cp.nodeName, err)
	}
	_, err = cp.checkReservation(logger, node)
	return err
}

// reservationCondition reports the last check of the kubelet CPU reservation.
// observe makes the check from each Node the condition writer handles, and
// republishes when its result changes.
func (cp *CPUDriver) reservationCondition() nodeConditionSource {
	return nodeConditionSource{
		conditionType: conditionReservationMissing,
		observe: func(ctx context.Context, logger logr.Logger, node *corev1.Node) {
			changed, err := cp.checkReservation(logger, node)
			if err != nil {
				logger.Error(err, "kubelet CPU reservation not checked", "node", cp.nodeName)
				return
			}
			if changed {
				cp.PublishResources(ctx)
			}
		},
		evaluate: func() nodeConditionValue {
			if cp.reservationMissing.Load() {
				return nodeConditionValue{status: corev1.ConditionTrue, reason: "InsufficientCPUReservation", message: cp.reservation.missingMessage(cp.driverName)}
			}
			return nodeConditionValue{
				status:  corev1.ConditionFalse,
				reason:  "SufficientCPUReservation",
				message: fmt.Sprintf("kubelet reserves %s CPUs, %d required", formatMilliCPU(cp.reservedMilliCPU), cp.reservation.cpus),
			}
		},
		stoppedMessage: "driver stopped; reservation not checked",
	}
}

// watchNode hands every change of the driver's own Node to the condition
// writer, until ctx is done. Without list and watch on nodes it stops after
// the first refusal: the driver then keeps the result of its start until it
// restarts.
func (cp *CPUDriver) watchNode(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	selector := fields.OneTermEqualSelector("metadata.name", cp.nodeName).String()
	nodes := cp.kubeClient.CoreV1().Nodes()
	informer := cache.NewSharedIndexInformer(cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = selector
			return nodes.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			options.FieldSelector = selector
			return nodes.Watch(ctx, options)
		},
	}, cp.kubeClient), &corev1.Node{}, 0, cache.Indexers{})
	handle := func(obj any) {
		if node, ok := obj.(*corev1.Node); ok {
			cp.nodeConditions.nodeChanged(node)
		}
	}
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handle,
		UpdateFunc: func(_, obj any) { handle(obj) },
	}); err != nil {
		logger.Error(err, "node changes not watched: the kubelet CPU reservation is checked again only when the driver restarts", "node", cp.nodeName)
		cancel()
		return
	}
	if err := informer.SetWatchErrorHandlerWithContext(func(ctx context.Context, r *cache.Reflector, err error) {
		if apierrors.IsForbidden(err) {
			logger.Error(err, "node changes not watched: grant the driver list and watch on nodes; the kubelet CPU reservation is checked again only when the driver restarts", "node", cp.nodeName)
			cancel()
			return
		}
		cache.DefaultWatchErrorHandler(ctx, r, err)
	}); err != nil {
		logger.Error(err, "node changes not watched: the kubelet CPU reservation is checked again only when the driver restarts", "node", cp.nodeName)
		cancel()
		return
	}
	done := make(chan struct{})
	cp.nodeWatchDone = done
	cp.stopNodeWatch = func() {
		cancel()
		<-done
	}
	go func() {
		defer close(done)
		defer cancel()
		informer.RunWithContext(ctx)
	}()
}
