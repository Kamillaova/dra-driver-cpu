/*
Copyright 2025 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/utils/cpuset"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
)

// PublishResources publishes ResourceSlice for CPU resources.
func (cp *CPUDriver) PublishResources(ctx context.Context) {
	ctx, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "deviceMode", cp.cpuDeviceMode, "groupBy", cp.cpuDeviceGroupBy)

	logger.V(4).Info("begin: publishing resources")
	defer logger.V(4).Info("end: publishing resources")

	if cp.topology.deviceSlices == nil {
		logger.Info("no devices to publish or error occurred")
		return
	}

	slices := make([]resourceslice.Slice, 0, len(cp.topology.deviceSlices))
	for _, chunk := range cp.topology.deviceSlices {
		slices = append(slices, resourceslice.Slice{Devices: chunk})
	}

	resources := resourceslice.DriverResources{
		Pools: map[string]resourceslice.Pool{
			// All slices are published under the same pool for this node.
			cp.nodeName: {Slices: slices},
		},
	}

	err := cp.draPlugin.PublishResources(ctx, resources)
	if err != nil {
		logger.Error(err, "error publishing resources")
	}
}

// PrepareResourceClaims is called by the kubelet to prepare a resource claim.
func (cp *CPUDriver) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	defer cp.watchdog.track("PrepareResourceClaims")()
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))

	logger.V(4).Info("begin: preparing resource claims", "numClaims", len(claims))
	defer logger.V(4).Info("end: preparing resource claims", "numClaims", len(claims))

	cp.waitForInitialSync(ctx, logger)

	result := make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		start := time.Now()
		cLogger := logger.WithValues("claim", ctxlog.KObj(claim), "claimUID", claim.UID)
		result[claim.UID] = cp.prepareResourceClaim(ctx, cLogger, claim)
		prepareResult := cpumetrics.ResultSuccess
		if result[claim.UID].Err != nil {
			prepareResult = cpumetrics.ResultError
		}
		cp.metrics.RecordPrepare(prepareResult, time.Since(start))
	}
	return result, nil
}

// waitForInitialSync gives the first Synchronize after a restart a short window
// to complete, so the common restart path prepares on the first attempt. It must
// not hold stateMu while waiting: Synchronize needs it to close initialSync.
// prepareResourceClaim still enforces the gate if the wait times out.
func (cp *CPUDriver) waitForInitialSync(ctx context.Context, logger logr.Logger) {
	cp.stateMu.Lock()
	synced, initialSync := cp.hasSynchronized, cp.initialSync
	cp.stateMu.Unlock()
	if synced || initialSync == nil {
		return
	}
	logger.V(2).Info("waiting for initial NRI synchronization", "timeout", cp.initialSyncWait)
	timer := time.NewTimer(cp.initialSyncWait)
	defer timer.Stop()
	select {
	case <-initialSync:
	case <-ctx.Done():
	case <-timer.C:
	}
}

func getCDIDeviceName(uid types.UID) string {
	return fmt.Sprintf("claim-%s", uid)
}

// getCDIRequestDeviceName names the device of one request of a claim. Neither
// a claim UID nor a request name, a DNS label, holds the underscore.
func getCDIRequestDeviceName(uid types.UID, request string) string {
	return fmt.Sprintf("claim-%s_%s", uid, request)
}

func (cp *CPUDriver) checkAdminAccess(claim *resourceapi.ResourceClaim) error {
	if claim.Status.Allocation == nil {
		return nil // nothing to do
	}
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		if alloc.AdminAccess != nil && *alloc.AdminAccess {
			return fmt.Errorf("admin access is not supported for device %q", alloc.Device)
		}
	}
	return nil
}

// reserveResourceClaimAllocation records a new claim allocation while applying
// the shared-pool guard for currently running shared containers. A shared
// container from the same pod may not have been created yet when this DRA hook
// runs, so that case is detected later by the NRI CreateContainer check.
func (cp *CPUDriver) reserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, record store.ClaimRecord) error {
	hasSharedContainers := len(cp.podState.GetContainersWithSharedCPUs()) > 0
	return cp.allocationState.Reserve(logger, claimUID, record, hasSharedContainers)
}

func claimRecord(claim *resourceapi.ResourceClaim, byRequest map[string]cpuset.CPUSet) store.ClaimRecord {
	record := store.ClaimRecord{Namespace: claim.Namespace, Name: claim.Name}
	for _, request := range slices.Sorted(maps.Keys(byRequest)) {
		record.Requests = append(record.Requests, store.RequestAllocation{Request: request, CPUs: byRequest[request]})
	}
	return record
}

// addRequestCPUs records cpus under the request a result belongs to. A pod
// names a request, never the subrequest the scheduler picked for it.
func addRequestCPUs(byRequest map[string]cpuset.CPUSet, alloc resourceapi.DeviceRequestAllocationResult, cpus cpuset.CPUSet) {
	request := resourceclaim.BaseRequestRef(alloc.Request)
	byRequest[request] = byRequest[request].Union(cpus)
}

func (cp *CPUDriver) prepareGroupedResourceClaimLocked(logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing grouped resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	if cp.cpuDeviceGroupBy == device.GROUP_BY_MACHINE {
		if err := checkDeviceRequestCountUpTo(claim, 1, cp.driverName); err != nil {
			return kubeletplugin.PrepareResult{
				Err: err,
			}
		}
	}

	if existing, ok := cp.allocationState.GetClaimRecord(claim.UID); ok {
		logger.V(2).Info("claim already has allocated CPUs in store, reusing assignment", "requests", existing.Requests)
		// Even if the claim is already allocated in our in-memory store (which happens when a duplicate prepare
		// call is invoked without an intermediate unprepare), we must call prepareDevices and return the result back to Kubelet.
		// If the CDI file is already created on disk, the CDI manager will safely overwrite it with the same configuration.
		// This ensures that the CDI specification file is written/recreated on disk (for example, if the driver
		// pod restarted and synchronized its memory store from the runtime but did not recreate the CDI files on disk).
		return cp.prepareDevices(logger, claim, existing)
	}

	var assignedCPUs cpuset.CPUSet
	byRequest := make(map[string]cpuset.CPUSet)
	allocatableCPUs := cp.allocationState.GetSharedCPUs()

	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		claimCPUCount, err := device.ConsumedCPUCount(alloc, cp.driverName)
		if err != nil {
			return kubeletplugin.PrepareResult{Err: err}
		}
		if err := cp.checkWholeCoreAmount(claim, alloc, claimCPUCount); err != nil {
			return kubeletplugin.PrepareResult{Err: err}
		}
		logger.V(4).Info("found CPU request", "numCPUs", claimCPUCount, "device", alloc.Device)

		topo := cp.topology.CPUTopology
		// TODO: what if `claimCPUCount==0`?

		// The preferred hint comes from the request's opaque config (if any); the
		// group-specific branches below pick the actual CPUs out of it.
		preferredCPUs, err := cp.cpuAllocator.GetPreferredCPUs(logger, claim.Status.Allocation, alloc)
		if err != nil {
			return kubeletplugin.PrepareResult{Err: err}
		}

		var cur cpuset.CPUSet
		switch cp.cpuDeviceGroupBy {
		case device.GROUP_BY_SOCKET:
			socketID, ok := cp.topology.NameToSocketID[alloc.Device]
			if !ok {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("no valid socket ID found for device %s", alloc.Device)}
			}
			socketCPUs := topo.CPUDetails.CPUsInSockets(socketID)
			availableCPUsForDevice := allocatableCPUs.Difference(assignedCPUs).Intersection(socketCPUs)
			logger.V(4).Info("socket CPU availability", "socketID", socketID, "socketCPUs", socketCPUs.String(), "availableCPUs", availableCPUsForDevice.String())
			cur, err = cp.takeCPUsForDevice(logger, alloc, socketCPUs, availableCPUsForDevice, preferredCPUs, claimCPUCount)
			err = cp.shortageHeldBy(err, alloc.Device, socketCPUs, availableCPUsForDevice, claimCPUCount)
		case device.GROUP_BY_NUMA_NODE:
			numaNodeID, ok := cp.topology.NameToNUMANodeID[alloc.Device]
			if !ok {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("no valid NUMA node ID found for device %s", alloc.Device)}
			}
			numaCPUs := topo.CPUDetails.CPUsInNUMANodes(numaNodeID)
			availableCPUsForDevice := allocatableCPUs.Difference(assignedCPUs).Intersection(numaCPUs)
			logger.V(4).Info("NUMA node CPU availability", "numaNodeID", numaNodeID, "numaCPUs", numaCPUs.String(), "availableCPUs", availableCPUsForDevice.String())
			cur, err = cp.takeCPUsForDevice(logger, alloc, numaCPUs, availableCPUsForDevice, preferredCPUs, claimCPUCount)
			err = cp.shortageHeldBy(err, alloc.Device, numaCPUs, availableCPUsForDevice, claimCPUCount)
		case device.GROUP_BY_UNCORE_CACHE:
			cacheCPUs, ok := cp.topology.NameToCPUs[alloc.Device]
			if !ok {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("no valid uncore cache found for device %s", alloc.Device)}
			}
			availableCPUsForDevice := allocatableCPUs.Difference(assignedCPUs).Intersection(cacheCPUs)
			logger.V(4).Info("uncore cache CPU availability", "cacheCPUs", cacheCPUs.String(), "availableCPUs", availableCPUsForDevice.String())
			cur, err = cp.takeCPUsForDevice(logger, alloc, cacheCPUs, availableCPUsForDevice, preferredCPUs, claimCPUCount)
			err = cp.shortageHeldBy(err, alloc.Device, cacheCPUs, availableCPUsForDevice, claimCPUCount)
		case device.GROUP_BY_MACHINE:
			// no mapping needed in machine mode - just one device = the whole machine
			availableCPUs := topo.CPUDetails.CPUs().Difference(cp.topology.ReservedCPUs)
			logger.V(4).Info("Machine CPU availability", "availableCPUs", availableCPUs.String())
			cur, err = cp.cpuAllocator.Allocate(logger, availableCPUs, preferredCPUs, claimCPUCount)
			logger.V(2).Info("using opaque config CPU assignment", "device", alloc.Device, "assigned", cur.String())
		}

		if err != nil {
			return kubeletplugin.PrepareResult{Err: err}
		}
		if err := cp.cpuAllocator.Validate(cur, assignedCPUs, cp.allocationState.GetPreparedCPUs()); err != nil {
			if cur.Intersection(assignedCPUs).IsEmpty() {
				err = heldBy(err, cp.allocationState.Holders(cur))
			}
			return kubeletplugin.PrepareResult{Err: err}
		}
		assignedCPUs = assignedCPUs.Union(cur)
		addRequestCPUs(byRequest, alloc, cur)
		logger.V(2).Info("CPU assignment for device", "device", alloc.Device, "assigned", cur.String(), "allAssigned", assignedCPUs.String())
	}

	if assignedCPUs.Size() == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	record := claimRecord(claim, byRequest)
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, record); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, record)
	if result.Err != nil {
		cp.allocationState.Release(logger, claim.UID)
		return result
	}
	cp.metrics.RecordClaimAllocatedCPUs(assignedCPUs.Size())
	cp.refreshAllocationMetrics()
	return result
}

// checkWholeCoreAmount refuses an amount that whole cores of the result's
// device cannot make. The scheduler rounds a request up to the device's
// request policy, and gives the amount as written where the policy is missing.
func (cp *CPUDriver) checkWholeCoreAmount(claim *resourceapi.ResourceClaim, alloc resourceapi.DeviceRequestAllocationResult, consumed int) error {
	if !cp.fullPhysicalCPUsOnly {
		return nil
	}
	threads := cp.topology.threadsPerCore[alloc.Device]
	if threads <= 1 {
		return nil
	}
	written, ok := writtenCPUAmount(claim, alloc.Request, cp.driverName)
	if !ok {
		written = *resource.NewQuantity(int64(consumed), resource.DecimalSI)
	}
	if written.CmpInt64(int64(consumed)) == 0 && consumed%threads == 0 {
		return nil
	}
	return exclusiveRefusal(alloc.Request, "amount "+written.String(),
		fmt.Sprintf("exclusive request (class %s)", requestDeviceClass(claim, alloc.Request)), wholeCoresAdvice(threads))
}

// exclusiveRefusal refuses an amount or a limit an exclusive request asks for,
// saying where the request is exclusive and what to ask for instead.
func exclusiveRefusal(request, subject, where, advice string) error {
	return fmt.Errorf("request %q: %s on %s; %s", request, subject, where, advice)
}

func wholeCoresAdvice(threads int) string {
	return fmt.Sprintf("ask for whole cores of %d threads (`fullPhysicalCPUsOnly: true`)", threads)
}

// writtenCPUAmount returns the CPUs the claim's request, or subrequest, names
// for a result, if it names any.
func writtenCPUAmount(claim *resourceapi.ResourceClaim, request, driverName string) (resource.Quantity, bool) {
	base := resourceclaim.BaseRequestRef(request)
	for _, req := range claim.Spec.Devices.Requests {
		if req.Name != base {
			continue
		}
		if req.Exactly != nil {
			return capacityRequest(req.Exactly.Capacity, driverName)
		}
		for _, sub := range req.FirstAvailable {
			if resourceclaim.CreateSubRequestRef(base, sub.Name) == request {
				return capacityRequest(sub.Capacity, driverName)
			}
		}
	}
	return resource.Quantity{}, false
}

// requestDeviceClass returns the device class of the claim's request, or
// subrequest, a result belongs to.
func requestDeviceClass(claim *resourceapi.ResourceClaim, request string) string {
	base := resourceclaim.BaseRequestRef(request)
	for _, req := range claim.Spec.Devices.Requests {
		if req.Name != base {
			continue
		}
		if req.Exactly != nil {
			return req.Exactly.DeviceClassName
		}
		for _, sub := range req.FirstAvailable {
			if resourceclaim.CreateSubRequestRef(base, sub.Name) == request {
				return sub.DeviceClassName
			}
		}
	}
	return ""
}

func capacityRequest(capacity *resourceapi.CapacityRequirements, driverName string) (resource.Quantity, bool) {
	if capacity == nil {
		return resource.Quantity{}, false
	}
	return device.LookupConsumedCapacity(capacity.Requests, driverName)
}

// takeCPUsForDevice takes whole cores of either size under fullPhysicalCPUsOnly
// on a device whose cores have two threads or differ, and leaves every other
// take to the configured allocator.
func (cp *CPUDriver) takeCPUsForDevice(logger logr.Logger, alloc resourceapi.DeviceRequestAllocationResult, deviceCPUs, available, preferred cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	threads, ok := cp.topology.threadsPerCore[alloc.Device]
	if !cp.wholeCorePicker || !ok || threads == 1 {
		return cp.cpuAllocator.Allocate(logger, available, preferred, numCPUs)
	}
	cpus, err := coreselect.TakeWholeCores(cp.topology.CPUTopology, available, numCPUs)
	var noSingle *coreselect.NoFreeSingleThreadCoreError
	if errors.As(err, &noSingle) {
		err = fmt.Errorf("request %q: %d exclusive CPUs need a free core of 1 thread on device %q, which has none (%d free cores of 2 threads; fullPhysicalCPUsOnly)",
			alloc.Request, noSingle.CPUs, alloc.Device, noSingle.FreeTwoThreadCores)
		if holders := cp.allocationState.Holders(singleThreadCPUs(cp.topology.CPUTopology.CPUDetails, deviceCPUs)); len(holders) > 0 {
			err = fmt.Errorf("%w: claims holding the device's cores of 1 thread: %s", err, holders)
		}
		return cpuset.New(), err
	}
	return cpus, err
}

func singleThreadCPUs(details cpuinfo.CPUDetails, cpus cpuset.CPUSet) cpuset.CPUSet {
	var single []int
	for _, cpu := range cpus.List() {
		if details.SiblingsOf(cpu).Size() == 1 {
			single = append(single, cpu)
		}
	}
	return cpuset.New(single...)
}

// heldBy adds to a refusal the prepared claims holding the CPUs it refused.
func heldBy(err error, holders store.ClaimHolders) error {
	if len(holders) == 0 {
		return err
	}
	return fmt.Errorf("%w: %s", err, holders)
}

// shortageHeldBy adds to a device's shortage the prepared claims holding CPUs
// of the device. On a healthy node the scheduler leaves free the CPUs a request
// asks of a device, so in a shortage one of these claims holds CPUs the
// scheduler counts free: a claim the kubelet no longer prepares, or a device
// allocated twice.
func (cp *CPUDriver) shortageHeldBy(err error, deviceName string, deviceCPUs, available cpuset.CPUSet, numCPUs int) error {
	var wholeCores *coreselect.NotEnoughWholeCoresError
	if err == nil || (available.Size() >= numCPUs && !errors.As(err, &wholeCores)) {
		return err
	}
	holders := cp.allocationState.Holders(deviceCPUs)
	if len(holders) == 0 {
		return err
	}
	return fmt.Errorf("%w: the CPUs the scheduler counts free on device %q are held by one of these claims: %s", err, deviceName, holders)
}

var errNRIDisconnected = errors.New("NRI connection to the container runtime is down; retry preparation")

func (cp *CPUDriver) prepareResourceClaim(ctx context.Context, logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	// DRA registration precedes NRI startup. Until the first recovery commits,
	// the empty allocation store does not describe CPUs held by live containers.
	if !cp.hasSynchronized {
		return kubeletplugin.PrepareResult{Err: fmt.Errorf("initial NRI synchronization has not completed; retry preparation")}
	}
	if cp.nriDisconnected.Load() {
		logger.V(2).Info("refusing to prepare claim while NRI is disconnected")
		return kubeletplugin.PrepareResult{Err: errNRIDisconnected}
	}
	if err := cp.checkAdminAccess(claim); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	if cp.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
		return cp.prepareGroupedResourceClaimLocked(logger, claim)
	}
	return cp.prepareIndividualResourceClaimLocked(logger, claim)
}

func (cp *CPUDriver) prepareIndividualResourceClaimLocked(logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing individual resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	claimCPUIDs := []int{}
	byRequest := make(map[string]cpuset.CPUSet)
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		cpuID, ok := cp.topology.NameToCPUID[alloc.Device]
		if !ok {
			return kubeletplugin.PrepareResult{
				Err: fmt.Errorf("device %q not found in device to CPU ID map", alloc.Device),
			}
		}
		claimCPUIDs = append(claimCPUIDs, cpuID)
		addRequestCPUs(byRequest, alloc, cpuset.New(cpuID))
	}

	if len(claimCPUIDs) == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	claimCPUSet := cpuset.New(claimCPUIDs...)
	if existing, ok := cp.allocationState.GetClaimRecord(claim.UID); ok {
		existingCPUs := existing.CPUs()
		logger.V(2).Info("claim already has allocated CPUs in store, reusing assignment", "cpus", existingCPUs.String())
		if !existingCPUs.Equals(claimCPUSet) {
			// This should realistically never happen as the claim is immutable.
			return kubeletplugin.PrepareResult{
				Err: fmt.Errorf("claim %s/%s is already prepared with different CPUs %s (requested %s)", claim.Namespace, claim.Name, existingCPUs.String(), claimCPUSet.String()),
			}
		}
		return cp.prepareDevices(logger, claim, existing)
	}

	// All the CPUs allocated to a claim must not be prepared for another claim.
	allocatableCPUs := cp.allocationState.GetSharedCPUs()
	if !claimCPUSet.IsSubsetOf(allocatableCPUs) {
		return kubeletplugin.PrepareResult{
			Err: heldBy(fmt.Errorf("claim %s/%s has overlapping device assignment with other claims", claim.Namespace, claim.Name), cp.allocationState.Holders(claimCPUSet)),
		}
	}

	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	record := claimRecord(claim, byRequest)
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, record); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, record)
	if result.Err != nil {
		cp.allocationState.Release(logger, claim.UID)
		return result
	}
	cp.metrics.RecordClaimAllocatedCPUs(claimCPUSet.Size())
	cp.refreshAllocationMetrics()
	return result
}

func (cp *CPUDriver) prepareDevices(logger logr.Logger, claim *resourceapi.ResourceClaim, record store.ClaimRecord) kubeletplugin.PrepareResult {
	deviceByRequest, err := cp.writeClaimDevices(logger, claim, record)
	if err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}

	preparedDevices := []kubeletplugin.Device{}
	cpusKnown := requestCPUsKnown(claim, record, cp.driverName)
	for _, allocResult := range claim.Status.Allocation.Devices.Results {
		if allocResult.Driver != cp.driverName {
			continue
		}
		preparedDevice := kubeletplugin.Device{
			PoolName:     allocResult.Pool,
			DeviceName:   allocResult.Device,
			CDIDeviceIDs: []string{deviceByRequest[resourceclaim.BaseRequestRef(allocResult.Request)]},
		}
		if allocResult.Request != "" {
			preparedDevice.Requests = []string{allocResult.Request}
		}
		if attrs, ok := getDeviceAttributes(cp.topology.deviceSlices, allocResult.Device); ok && len(attrs) > 0 {
			metadataAttrs := make(map[string]resourceapi.DeviceAttribute, len(attrs))
			for k, v := range attrs {
				metadataAttrs[string(k)] = v
			}
			ref := metadataRef(claim.UID, record, allocResult.Request)
			exclusive := cp.allocationState.HoldsExclusiveCPUsOf(ref)
			if quantity, ok := device.LookupConsumedCapacity(allocResult.ConsumedCapacity, cp.driverName); ok && exclusive {
				allocatedCount := quantity.Value()
				metadataAttrs[string(device.AttributeAllocatedNumCPUs)] = resourceapi.DeviceAttribute{
					IntValue: &allocatedCount,
				}
			}
			if quantity, ok := device.LookupConsumedCapacity(allocResult.ConsumedCapacity, cp.driverName); ok {
				metadataAttrs[string(device.AttributeCPURequest)] = resourceapi.DeviceAttribute{
					StringValue: new(quantity.String()),
				}
				// cpu.max's word for no limit: no request is capped.
				metadataAttrs[string(device.AttributeCPULimit)] = resourceapi.DeviceAttribute{
					StringValue: new("max"),
				}
			}
			if cpus := requestCPUsOn(record, ref.Request, cp.deviceCPUs(allocResult.Device)); exclusive && cpusKnown && !cpus.IsEmpty() {
				metadataAttrs[string(device.AttributeCPUSet)] = resourceapi.DeviceAttribute{
					StringValue: new(cpus.String()),
				}
			}
			preparedDevice.Metadata = &kubeletplugin.DeviceMetadata{
				Attributes: metadataAttrs,
			}
			logger.V(6).Info("added device metadata", "device", allocResult.Device, "numAttrs", len(metadataAttrs))
		}
		preparedDevices = append(preparedDevices, preparedDevice)
	}

	logger.V(4).Info("prepared devices for resource claim", "preparedDevices", preparedDevices)
	return kubeletplugin.PrepareResult{
		Devices: preparedDevices,
	}
}

// metadataRef names the request a result's metadata describes, or the whole
// claim for a claim restored from upstream's spec, which records no requests.
func metadataRef(claimUID types.UID, record store.ClaimRecord, request string) store.ClaimRequestRef {
	if len(record.Requests) == 1 && record.Requests[0].Request == "" {
		return store.ClaimRequestRef{ClaimUID: claimUID}
	}
	return store.ClaimRequestRef{ClaimUID: claimUID, Request: resourceclaim.BaseRequestRef(request)}
}

// requestCPUsKnown reports whether the record names the CPUs of each request.
// A claim restored from upstream's spec records one set for the whole claim,
// which is a request's own only where the claim has one request of this driver.
func requestCPUsKnown(claim *resourceapi.ResourceClaim, record store.ClaimRecord, driverName string) bool {
	if len(record.Requests) != 1 || record.Requests[0].Request != "" {
		return true
	}
	requests := sets.New[string]()
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver == driverName {
			requests.Insert(resourceclaim.BaseRequestRef(result.Request))
		}
	}
	return requests.Len() == 1 && !requests.Has("")
}

// requestCPUsOn returns the CPUs the record gives a request on a device: a
// request spanning several devices holds CPUs of each.
func requestCPUsOn(record store.ClaimRecord, request string, deviceCPUs cpuset.CPUSet) cpuset.CPUSet {
	for _, held := range record.Requests {
		if held.Request == request {
			return held.CPUs.Intersection(deviceCPUs)
		}
	}
	return cpuset.New()
}

// deviceCPUs returns the CPUs a device groups, reserved CPUs included.
func (cp *CPUDriver) deviceCPUs(deviceName string) cpuset.CPUSet {
	details := cp.topology.CPUTopology.CPUDetails
	if cpuID, ok := cp.topology.NameToCPUID[deviceName]; ok {
		return cpuset.New(cpuID)
	}
	if socketID, ok := cp.topology.NameToSocketID[deviceName]; ok {
		return details.CPUsInSockets(socketID)
	}
	if numaNodeID, ok := cp.topology.NameToNUMANodeID[deviceName]; ok {
		return details.CPUsInNUMANodes(numaNodeID)
	}
	if cpus, ok := cp.topology.NameToCPUs[deviceName]; ok {
		return cpus
	}
	if deviceName == device.CPUDeviceMachineGrouped {
		return details.CPUs()
	}
	return cpuset.New()
}

// writeClaimDevices writes the CDI spec of a claim and returns the qualified
// name of the device each request is given. The kubelet injects a device only
// into the containers that name its request, or name no request at all.
func (cp *CPUDriver) writeClaimDevices(logger logr.Logger, claim *resourceapi.ResourceClaim, record store.ClaimRecord) (map[string]string, error) {
	deviceByRequest := make(map[string]string)
	if len(record.Requests) == 1 && record.Requests[0].Request == "" {
		// Restored from a spec upstream's driver wrote, the claim keeps that
		// spec, and its containers their variable, until it is unprepared.
		deviceName := getCDIDeviceName(claim.UID)
		envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claim.UID, record.Requests[0].CPUs.String())
		if err := cp.cdiMgr.AddDevice(logger, deviceName, envVar); err != nil {
			return nil, err
		}
		qualifiedName := cdiparser.QualifiedName(cdiVendor, cdiClass, deviceName)
		for _, allocResult := range claim.Status.Allocation.Devices.Results {
			deviceByRequest[resourceclaim.BaseRequestRef(allocResult.Request)] = qualifiedName
		}
		logger.V(6).Info("prepared CDI device", "cdiDeviceName", deviceName, "envVar", envVar, "qualifiedName", qualifiedName)
		return deviceByRequest, nil
	}

	envByDevice := make(map[string]string, len(record.Requests))
	cdiRecord := cdiClaimRecord{Namespace: record.Namespace, Name: record.Name}
	for _, request := range record.Requests {
		deviceName := getCDIRequestDeviceName(claim.UID, request.Request)
		envByDevice[deviceName] = fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim.UID, request.Request, cdiEnvDynamicValue)
		deviceByRequest[request.Request] = cdiparser.QualifiedName(cdiVendor, cdiClass, deviceName)
		cdiRecord.Requests = append(cdiRecord.Requests, cdiRequestRecord{
			Request: request.Request,
			CPUs:    request.CPUs,
			Results: cp.resultRecords(claim, request.Request),
		})
	}
	for _, allocResult := range claim.Status.Allocation.Devices.Results {
		if request := resourceclaim.BaseRequestRef(allocResult.Request); allocResult.Driver == cp.driverName && deviceByRequest[request] == "" {
			return nil, fmt.Errorf("claim %s/%s was prepared without request %q, which its allocation names; the claim must be unprepared before it is prepared again", claim.Namespace, claim.Name, request)
		}
	}
	if err := cp.cdiMgr.AddClaimDevices(logger, claim.UID, envByDevice, cdiRecord); err != nil {
		return nil, err
	}
	logger.V(6).Info("prepared CDI devices", "devices", deviceByRequest)
	return deviceByRequest, nil
}

func (cp *CPUDriver) resultRecords(claim *resourceapi.ResourceClaim, request string) []cdiResultRecord {
	var results []cdiResultRecord
	for _, allocResult := range claim.Status.Allocation.Devices.Results {
		if allocResult.Driver != cp.driverName || resourceclaim.BaseRequestRef(allocResult.Request) != request {
			continue
		}
		result := cdiResultRecord{Device: allocResult.Device}
		if quantity, ok := device.LookupConsumedCapacity(allocResult.ConsumedCapacity, cp.driverName); ok {
			result.Consumed = &quantity
		}
		results = append(results, result)
	}
	return results
}

// UnprepareResourceClaims is called by the kubelet to unprepare the resources for a claim.
func (cp *CPUDriver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	defer cp.watchdog.track("UnprepareResourceClaims")()
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))

	logger.V(4).Info("begin: unpreparing resource claims", "numClaims", len(claims))
	defer logger.V(4).Info("end: unpreparing resource claims", "numClaims", len(claims))

	result := make(map[types.UID]error)

	if len(claims) == 0 {
		return result, nil
	}

	for _, claim := range claims {
		// note kubeletplugin.NamespacedObject doesn't implement KMetadata
		cLogger := logger.WithValues("claim", claim.String(), "claimUID", claim.UID)
		cLogger.V(2).Info("unpreparing resource claim")
		start := time.Now()
		err := cp.unprepareResourceClaim(ctx, cLogger, claim)
		result[claim.UID] = err
		if err != nil {
			cLogger.Error(err, "error unpreparing resources for claim")
			cp.metrics.RecordUnprepare(cpumetrics.ResultError, time.Since(start))
		} else {
			cp.metrics.RecordUnprepare(cpumetrics.ResultSuccess, time.Since(start))
		}
	}
	return result, nil
}

func (cp *CPUDriver) unprepareResourceClaim(ctx context.Context, logger logr.Logger, claim kubeletplugin.NamespacedObject) error {
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// Remove the CDI spec first. If that fails, keep the allocation recorded so
	// the driver does not make those CPUs available while stale CDI state remains.
	if err := cp.cdiMgr.RemoveClaimDevices(logger, claim.UID); err != nil {
		return err
	}
	cp.allocationState.Release(logger, claim.UID)
	cp.refreshAllocationMetrics()
	cp.requestReconcile()
	return nil
}

// ErrFatal marks the cause with which HandleError cancels the run context.
var ErrFatal = errors.New("fatal error in the DRA driver")

// HandleError is called by the kubelet plugin framework when an error occurs in the background,
// for example while publishing ResourceSlices.
func (cp *CPUDriver) HandleError(ctx context.Context, err error, msg string) {
	logger := ctxlog.FromContext(ctx)

	// Log the error using the standard Kubernetes error handler
	if !cp.handleDroppedMapping(ctx, err, msg) {
		runtime.HandleErrorWithContext(ctx, err, msg)
	}

	// For unrecoverable errors, shut down and exit with a clear error message.
	// This fail-fast behavior is intentional for early project maturity to surface
	// issues quickly rather than silently continuing in a broken state.
	if !errors.Is(err, kubeletplugin.ErrRecoverable) {
		logger.Error(err, "fatal unrecoverable error in DRA driver, exiting",
			"driver", cp.driverName,
			"node", cp.nodeName,
			"message", msg,
		)
		cp.cancelRun(fmt.Errorf("%w: %s: %w", ErrFatal, msg, err))
	}
}

func checkDeviceRequestCountUpTo(claim *resourceapi.ResourceClaim, limit int64, driverName string) error {
	// we need to backtrack the requests from the allocations,
	// because driverName (from allocation result) is a parameter we get,
	// while deviceClass (from allocation request) is not something we can
	// safely infer.
	requestsForDriver := sets.New[string]()
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver == driverName {
			requestsForDriver.Insert(result.Request)
		}
	}

	for _, req := range claim.Spec.Devices.Requests {
		if req.Exactly != nil {
			if !requestsForDriver.Has(req.Name) {
				continue
			}
			if req.Exactly.Count > limit {
				return fmt.Errorf("claim %s/%s: driver grouping mode %q supports exact device request up to %d", claim.Namespace, claim.Name, device.GROUP_BY_MACHINE, limit)
			}
			// safe to continue here: the apiserver guarantees only one between Exactly and FirstAvailable will be set
			continue
		}
		for _, subrequest := range req.FirstAvailable {
			requestName := req.Name + "/" + subrequest.Name
			if !requestsForDriver.Has(requestName) {
				continue
			}
			if subrequest.Count > limit {
				return fmt.Errorf("claim %s/%s: driver grouping mode %q supports first available device request up to %d", claim.Namespace, claim.Name, device.GROUP_BY_MACHINE, limit)
			}
		}
	}
	return nil
}
