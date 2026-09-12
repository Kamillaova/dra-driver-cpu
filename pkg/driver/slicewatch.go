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
	"fmt"
	"maps"
	"slices"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
)

// storedSliceReader reports the ResourceSlices the API server holds for this
// driver on this node.
//
// The driver publishes through a controller that queues the work and writes
// later, and nothing tells the caller when a write landed, so the only way to
// know that a capacity has been stored is to read it back. Narrowed to one
// method, like containerUpdater, so a test can answer it directly.
type storedSliceReader interface {
	StoredSlices() ([]*resourceapi.ResourceSlice, error)
}

// storedSliceInformerReader answers from an informer on this node's own slices.
// The publishing controller keeps one of its own for the same objects; this is
// separate because that one is not exposed.
type storedSliceInformerReader struct {
	lister resourcelisters.ResourceSliceLister
}

func (r storedSliceInformerReader) StoredSlices() ([]*resourceapi.ResourceSlice, error) {
	return r.lister.List(labels.Everything())
}

// watchStoredSlices starts an informer on the ResourceSlices this driver
// published for its own node, and returns a reader over it once its cache has
// filled.
//
// The field selector is the one the publishing controller uses for the same
// objects, driver and node name, both of which the API server has always
// supported for ResourceSlices. The pool is selected in code instead: the pool
// name selector is newer, and a server that does not know it rejects the whole
// list rather than ignoring the term.
//
// The informer runs for the driver's life; only the wait for its first list is
// bounded, because a cache that never fills would otherwise hold startup here
// with the plugin already registered and nothing else of the driver running.
// A driver that cannot read its own slices back cannot move a claim safely, so
// the bound is an error rather than a warning, and restarting is the answer.
func watchStoredSlices(ctx context.Context, cp *CPUDriver) (storedSliceReader, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(cp.kubeClient, 0,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.Set{
				resourceapi.ResourceSliceSelectorDriver:   cp.driverName,
				resourceapi.ResourceSliceSelectorNodeName: cp.nodeName,
			}.String()
		}))
	informer := factory.Resource().V1().ResourceSlices()
	reader := storedSliceInformerReader{lister: informer.Lister()}

	factory.Start(ctx.Done())
	syncCtx, cancel := context.WithTimeout(ctx, storedSliceSyncTimeout)
	defer cancel()
	for typ, synced := range factory.WaitForCacheSync(syncCtx.Done()) {
		if !synced {
			return nil, fmt.Errorf("cache for %s did not sync within %s", typ, storedSliceSyncTimeout)
		}
	}
	return reader, nil
}

// storedPool is this driver's own pool as the API server holds it: the devices
// of the newest generation, and whether every slice of that generation is
// present.
//
// Only the newest generation is read, and only when it is whole, because that is
// what a consumer of the pool does: an incomplete pool is not allocated from at
// all, so a capacity published across several slices means nothing until the
// last of them is stored.
type storedPool struct {
	devices  map[string]resourceapi.Device
	complete bool
}

func (cp *CPUDriver) storedPool() (storedPool, error) {
	stored, err := cp.storedSlices.StoredSlices()
	if err != nil {
		return storedPool{}, err
	}

	newest := int64(-1)
	var slicesInPool []*resourceapi.ResourceSlice
	for _, slice := range stored {
		if slice.Spec.Driver != cp.driverName || slice.Spec.Pool.Name != cp.nodeName {
			continue
		}
		if slice.Spec.NodeName == nil || *slice.Spec.NodeName != cp.nodeName {
			continue
		}
		switch {
		case slice.Spec.Pool.Generation > newest:
			newest, slicesInPool = slice.Spec.Pool.Generation, []*resourceapi.ResourceSlice{slice}
		case slice.Spec.Pool.Generation == newest:
			slicesInPool = append(slicesInPool, slice)
		}
	}
	if len(slicesInPool) == 0 {
		return storedPool{}, nil
	}

	pool := storedPool{
		devices:  map[string]resourceapi.Device{},
		complete: int64(len(slicesInPool)) == slicesInPool[0].Spec.Pool.ResourceSliceCount,
	}
	for _, slice := range slicesInPool {
		for _, dev := range slice.Spec.Devices {
			if _, twice := pool.devices[dev.Name]; twice {
				// One device in two slices of one generation is a pool being
				// replaced under the reader, whichever copy is the newer.
				return storedPool{}, fmt.Errorf("device %q is in two slices of pool generation %d", dev.Name, newest)
			}
			pool.devices[dev.Name] = dev
		}
	}
	return pool, nil
}

// storedDevicesAgree reports whether the API server holds the named devices as
// the driver now means them.
//
// It compares the whole device rather than its capacity alone. The publishing
// controller adopts fields the API server dropped and carries on, so a device
// whose capacity is right and whose taint was dropped reads as stored while it
// is over-advertising exactly the CPUs the taint was there to withdraw.
//
// The comparison is against what the driver means now, not against a snapshot
// taken when the round was planned: another hook may have changed the inventory
// meanwhile, and a stored state the driver no longer means is not one to move a
// claim on.
func (cp *CPUDriver) storedDevicesAgree(logger logr.Logger, names []string) bool {
	pool, err := cp.storedPool()
	if err != nil {
		logger.Error(err, "cannot read this node's stored ResourceSlices")
		return false
	}
	if !pool.complete {
		logger.V(2).Info("this node's pool is not whole yet")
		return false
	}

	cp.applyMu.Lock()
	intended := cp.intendedDevices()
	cp.applyMu.Unlock()

	for _, name := range names {
		want, published := intended[name]
		if !published {
			// The device went with a configuration change, and a claim cannot be
			// moved onto CPUs no device offers.
			logger.V(2).Info("device is no longer published", "device", name)
			return false
		}
		got, ok := pool.devices[name]
		if !ok {
			logger.V(2).Info("device is not in this node's stored pool yet", "device", name)
			return false
		}
		if reason := deviceDiffers(want, got); reason != "" {
			logger.V(2).Info("stored device is not the one the driver means", "device", name, "reason", reason)
			return false
		}
	}
	return true
}

// deviceDiffers says how a stored device falls short of the intended one, or
// returns the empty string when it does not.
//
// Everything the mirror and the fence rest on is compared, not the capacity
// value alone: the whole capacity, its request policy included, since the value
// is only meaningful against the range that is validated with it; the taints,
// which are what withdraws a value the driver could not publish low enough; and
// whether the device is still shareable at all, without which none of the
// arithmetic holds. Each of those is a field the API server may drop when a
// feature gate is off, and the publishing controller adopts what it dropped and
// carries on.
//
// Quantities are compared for their numeric value rather than their formatting,
// which is what the API's own semantic equality does.
func deviceDiffers(want, got resourceapi.Device) string {
	switch {
	case !apiequality.Semantic.DeepEqual(want.Capacity, got.Capacity):
		wantCPUs, gotCPUs := want.Capacity[device.CPUResourceQualifiedName], got.Capacity[device.CPUResourceQualifiedName]
		return fmt.Sprintf("stored CPU capacity is %s, not %s", gotCPUs.Value.String(), wantCPUs.Value.String())
	case !apiequality.Semantic.DeepEqual(want.Taints, got.Taints):
		return "stored taints are not the ones the driver published"
	case !apiequality.Semantic.DeepEqual(want.AllowMultipleAllocations, got.AllowMultipleAllocations):
		return "stored device is not shareable as published"
	}
	return ""
}

// intendedDevices is every device the driver would publish now, by name. Called
// with applyMu held.
func (cp *CPUDriver) intendedDevices() map[string]resourceapi.Device {
	frontier, frontierInput := cp.frontier()
	chunks, _ := cp.chunkDevices(cp.occupiedDevices(), cp.poisonedNUMANodes(), cp.capacityMirror(), frontier, frontierInput)
	devices := map[string]resourceapi.Device{}
	for _, chunk := range chunks {
		for _, dev := range chunk {
			devices[dev.Name] = dev
		}
	}
	return devices
}

// awaitStoredShrink publishes what the round has already reserved and waits
// until the API server holds it, so that no container is told to take CPUs a
// scheduler may still be offering to someone else.
//
// A driver publishing nothing waits for nothing: with no plugin to publish
// through, or no way to read a slice back, there is no inventory for a shrink to
// be missing from.
//
// Called with applyMu released.
func (cp *CPUDriver) awaitStoredShrink(ctx context.Context, round *defragRound) error {
	logger := ctxlog.FromContext(ctx)
	if cp.draPlugin == nil || cp.storedSlices == nil {
		return nil
	}
	cp.applyMu.Lock()
	names := cp.roundTargetDevices(round)
	cp.applyMu.Unlock()
	if len(names) == 0 {
		return nil
	}

	round.sliceWrites++
	start := time.Now()
	if err := cp.publishResources(ctx); err != nil {
		return fmt.Errorf("cannot publish the capacity this round shrinks: %w", err)
	}

	deadline := time.NewTimer(cp.defrag.publishTimeout)
	defer deadline.Stop()
	poll := time.NewTicker(storedSlicePollInterval)
	defer poll.Stop()
	for {
		if cp.storedDevicesAgree(logger, names) {
			delay := time.Since(start).Seconds()
			cp.metrics.RecordStoreToDriverDelay(delay)
			cp.metrics.RecordStoreToSchedulerDelay(delay)
			return nil
		}
		select {
		case <-poll.C:
		case <-deadline.C:
			return fmt.Errorf("the shrunken capacity of %v was not stored within %s", names, cp.defrag.publishTimeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// roundTargetDevices is the devices whose capacity this round shrinks: the ones
// holding the CPUs its claims are moving onto. A move inside one device shrinks
// nothing and the wait over it ends at once.
//
// Called with applyMu held.
func (cp *CPUDriver) roundTargetDevices(round *defragRound) []string {
	names := map[string]struct{}{}
	for name, cpus := range cp.mirroredDevices() {
		for _, move := range round.moves {
			if !cpus.Intersection(move.To).IsEmpty() {
				names[name] = struct{}{}
				break
			}
		}
	}
	return slices.Sorted(maps.Keys(names))
}
