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
	"maps"
	"slices"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/cpuset"
)

// mirrorTerms is what one device's published capacity is corrected by.
//
// A claim's allocation is immutable, so the device it was charged to stays the
// device a scheduler subtracts it from however the driver later places the
// claim. The published capacity carries the difference, so that
// value − consumed is the CPUs really free on that device:
//
//	value(D) = size(D) + departed(D) − squatters(D)
//
// The two terms are separate because they are two different facts, and because
// they stop being true at different moments: a claim has departed once the CPUs
// it is charged for are free for someone else, and a claim squats for as long as
// it occupies CPUs whose capacity nobody has subtracted.
type mirrorTerms struct {
	// departed is the CPUs charged to this device by claims no longer on it.
	departed int
	// squatters is the CPUs occupied on this device by claims charged elsewhere.
	squatters int
}

func (t mirrorTerms) correction() int {
	return t.departed - t.squatters
}

// capacityMirror is the correction of every device that has one.
type capacityMirror map[string]mirrorTerms

// corrections is the mirror as the published slices carry it, with the devices
// published at their own size left out. It is what a later publication compares
// against, so two mirrors that publish the same capacities have to compare
// equal: a device that lost a departure and a squatter of the same size is
// publishing the number it published before.
func (m capacityMirror) corrections() map[string]int {
	corrections := make(map[string]int, len(m))
	for name, terms := range m {
		if terms.correction() == 0 {
			continue
		}
		corrections[name] = terms.correction()
	}
	return corrections
}

// capacityMirror computes the correction of every device a claim takes CPUs of
// its own from.
//
// It is a pure function of the allocation store, which is what makes it safe
// across a crash and across a round the runtime never answered: whatever state
// the driver comes back in, the capacity it publishes follows from the same
// ledger, and a reservation the driver is still holding is still a shrink.
//
// Called with applyMu held.
func (cp *CPUDriver) capacityMirror() capacityMirror {
	devices := cp.mirroredDevices()
	if len(devices) == 0 {
		return nil
	}

	mirror := capacityMirror{}
	for claimUID, holding := range cp.cpuAllocationStore.ClaimHoldings() {
		if len(holding.Recorded) == 0 {
			// A claim whose record was written before the driver kept the devices
			// its allocation charged. Read as "charged nothing" it would count as a
			// squatter wherever it sits and shrink every device it occupies by its
			// own size, while a scheduler still subtracts it at the device it really
			// was charged to -- so a driver upgrade would withdraw a node's whole
			// allocated capacity. Unknown is left uncorrected instead, which is the
			// capacity the device had before the mirror existed.
			continue
		}
		for name, cpus := range devices {
			charged, occupied := holding.Recorded[name], cpus.Intersection(holding.Held).Size()
			terms := mirror[name]
			switch {
			case charged > occupied:
				if cp.claimReader == nil || !cp.claimReader.IsProjectedDeallocated(claimUID) {
					terms.departed += charged - occupied
				}
			case occupied > charged:
				terms.squatters += occupied - charged
			default:
				continue
			}
			mirror[name] = terms
		}
	}
	return mirror
}

// mirroredDevices is the devices the mirror applies to, with their own CPUs: a
// pool is left out for the reason deviceIsPool gives, and individual mode is
// left out by having no such devices at all, since there the scheduler names one
// CPU per device and publishes no capacity to correct.
//
// Called with applyMu held.
func (cp *CPUDriver) mirroredDevices() map[string]cpuset.CPUSet {
	devices := make(map[string]cpuset.CPUSet, len(cp.topology.deviceNameToCPUs))
	for name, cpus := range cp.topology.deviceNameToCPUs {
		if cp.topology.deviceIsPool(name) {
			continue
		}
		devices[name] = cpus
	}
	return devices
}

// applyCapacityMirror publishes each device's corrected capacity, and reports
// how many devices it could not publish the truth for.
//
// A device whose corrected capacity would fall below what its own request policy
// allows is published at that floor and tainted. The alternative is worse than
// over-stating one device: a shareable device's range is re-validated against a
// changed value, min + step above the value invalidates the ResourceSlice, and a
// slice the API server rejects leaves every device of the node stale while the
// publishing controller retries it.
func applyCapacityMirror(devices []resourceapi.Device, mirror capacityMirror) ([]resourceapi.Device, int) {
	if len(mirror) == 0 {
		return devices, 0
	}
	mirrored, floored := slices.Clone(devices), 0
	for i, dev := range mirrored {
		correction := mirror[dev.Name].correction()
		if correction == 0 {
			continue
		}
		capacity, ok := dev.Capacity[device.CPUResourceQualifiedName]
		if !ok {
			continue
		}
		computed := capacity.Value.Value() + int64(correction)
		published := max(computed, capacityFloor(capacity))
		capacity.Value = *resource.NewQuantity(published, resource.DecimalSI)
		dev.Capacity = maps.Clone(dev.Capacity)
		dev.Capacity[device.CPUResourceQualifiedName] = capacity
		if published > computed {
			dev.Taints = append(slices.Clone(dev.Taints), resourceapi.DeviceTaint{
				Key:    device.FloorTaintKey,
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			})
			floored++
		}
		mirrored[i] = dev
	}
	return mirrored, floored
}

// capacityFloor is the lowest amount a device may publish for a capacity
// without contradicting the request policy published beside it: at least the
// minimum a request may ask for, and at least one step above that minimum, since
// both comparisons are made against the value on every update. A capacity with
// no range has neither and floors at nothing.
func capacityFloor(capacity resourceapi.DeviceCapacity) int64 {
	if capacity.RequestPolicy == nil || capacity.RequestPolicy.ValidRange == nil {
		return 0
	}
	floor := int64(0)
	if min := capacity.RequestPolicy.ValidRange.Min; min != nil {
		floor += min.Value()
	}
	if step := capacity.RequestPolicy.ValidRange.Step; step != nil {
		floor += step.Value()
	}
	return floor
}
