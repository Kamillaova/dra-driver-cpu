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
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	opaqueapi "github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpumanager"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
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

	// CCX-FORK: upstream publishes the slices New cut, once and for all. The
	// order of cache devices tracks which caches hold a claim, so it is rebuilt
	// here; publishMu keeps a slower publisher from leaving the controller with
	// an order a faster one has already superseded.
	cp.publishMu.Lock()
	defer cp.publishMu.Unlock()
	cp.applyMu.Lock()
	chunks := cp.refreshDeviceOrder()
	cp.applyMu.Unlock()

	if chunks == nil {
		logger.Info("no devices to publish or error occurred")
		return
	}

	slices := make([]resourceslice.Slice, 0, len(chunks))
	for _, chunk := range chunks {
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
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))

	logger.V(4).Info("begin: preparing resource claims", "numClaims", len(claims))
	defer logger.V(4).Info("end: preparing resource claims", "numClaims", len(claims))

	result := make(map[types.UID]kubeletplugin.PrepareResult)

	if len(claims) == 0 {
		return result, nil
	}

	// Held across the whole batch: a claim's CPUs are chosen from what the store
	// says is free and then written to its CDI spec, and the two must not be
	// separated. Both prepare paths also reuse an existing allocation when they
	// find one, so the read belongs inside as well.
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	for _, claim := range claims {
		start := time.Now()
		cLogger := logger.WithValues("claim", ctxlog.KObj(claim), "claimUID", claim.UID)
		// CCX-FORK: upstream dispatches on the device mode here, having nothing
		// to check before it.
		result[claim.UID] = cp.prepareClaim(cLogger, claim)
		prepareResult := cpumetrics.ResultSuccess
		if result[claim.UID].Err != nil {
			prepareResult = cpumetrics.ResultError
		} else {
			// The API server's reservation cannot be forged by a pod spec the
			// way a DRA_CPUSET_* env var can; CreateContainer falls back to
			// this when the runtime reports no CDI devices at all.
			cp.claimTracker.SetReservedFor(claim.UID, reservedForPodUIDs(claim))
		}
		cp.metricsRecorder().RecordPrepare(prepareResult, time.Since(start))
		// CCX-FORK: a claim that just landed split across caches is exactly what a
		// pass exists to repair, and it is cheapest to move while the container is
		// still starting. Only defragmentation wants this: after a prepare there is
		// nothing for the shared reconcile to do that CreateContainer will not.
		if cp.defrag.enabled && result[claim.UID].Err == nil {
			cp.requestReconcile()
		}
	}
	// CCX-FORK: upstream's slices never change after startup, so it publishes
	// them once and nothing here republishes.
	cp.republishOnCacheOrderChange(ctx)
	return result, nil
}

// republishOnCacheOrderChange sends the slices out again when this batch left a
// cache holding a claim that held none, or the other way round: the published
// order says which cache the allocator meets first, and it has just changed.
//
// The publication runs on its own, because it must not be held under applyMu
// and the kubelet's call is over as soon as this returns. It neither blocks nor
// uses the context for anything but logging.
//
// Called with applyMu held.
func (cp *CPUDriver) republishOnCacheOrderChange(ctx context.Context) {
	if !cp.cacheOrderIsStale() {
		return
	}
	go cp.PublishResources(context.WithoutCancel(ctx))
}

// prepareClaim checks what a claim says about its own placement and then places
// it. The check comes first because the answer does not depend on the device
// mode, and a claim that contradicts itself is the template's error rather than
// the node's.
func (cp *CPUDriver) prepareClaim(logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	placement, err := cp.claimConfig(claim)
	if err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	if cp.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
		return cp.prepareGroupedResourceClaim(logger, claim, placement)
	}
	return cp.prepareResourceClaim(logger, claim, placement)
}

// claimConfig is what a claim says about its own placement, folded from the
// configurations its requests carry. Mobility and alignment describe the claim
// and not one of its requests, so configurations that disagree about them are
// refused rather than reconciled.
//
// Every configuration this driver is named in has to be readable, wherever it
// came from, or the claim is refused. Only the claim's own are then read for
// what they say: one attached to a DeviceClass is the cluster administrator's,
// and whether a workload survives having its CPUs changed is for whoever writes
// its template to state.
func (cp *CPUDriver) claimConfig(claim *resourceapi.ResourceClaim) (opaqueapi.ClaimPlacement, error) {
	placement := opaqueapi.ClaimPlacement{Alignment: v1alpha1.AlignmentBestEffort}
	if claim.Status.Allocation == nil {
		return placement, nil
	}

	folded := false
	for _, entry := range claim.Status.Allocation.Devices.Config {
		if entry.Opaque == nil || entry.Opaque.Driver != cp.driverName || len(entry.Opaque.Parameters.Raw) == 0 {
			continue
		}
		parsed, err := opaqueapi.ParseOpaqueConfig(entry.Opaque.Parameters.Raw)
		if err != nil {
			return opaqueapi.ClaimPlacement{}, err
		}
		if entry.Source != resourceapi.AllocationConfigSourceClaim {
			continue
		}
		if folded && (parsed.Relocatable != placement.Relocatable || parsed.Alignment != placement.Alignment) {
			return opaqueapi.ClaimPlacement{}, fmt.Errorf("claim %s/%s carries configurations that disagree about cpuConfig.relocatable or cpuConfig.alignment, which describe the claim rather than one of its requests",
				claim.Namespace, claim.Name)
		}
		placement.Relocatable = parsed.Relocatable
		placement.Alignment = parsed.Alignment
		placement.AlignmentSet = placement.AlignmentSet || parsed.AlignmentSet
		folded = true
	}

	if placement.AlignmentSet && !claimOffersSplitAlternatives(claim) {
		return opaqueapi.ClaimPlacement{}, fmt.Errorf("claim %s/%s sets cpuConfig.alignment, but none of its requests offers the allocator alternatives, so it can only be placed whole",
			claim.Namespace, claim.Name)
	}
	return placement, nil
}

// claimOffersSplitAlternatives reports whether any request of a claim leaves the
// allocator a choice between placements of different shapes, which is what
// cpuConfig.alignment answers.
func claimOffersSplitAlternatives(claim *resourceapi.ResourceClaim) bool {
	for _, request := range claim.Spec.Devices.Requests {
		if len(request.FirstAvailable) > 1 {
			return true
		}
	}
	return false
}

// reservedForPodUIDs returns the pod UIDs claim.Status.ReservedFor names,
// ignoring any non-pod consumer reference: this driver only ever compares
// against a requesting pod's own UID.
func reservedForPodUIDs(claim *resourceapi.ResourceClaim) []types.UID {
	var podUIDs []types.UID
	for _, consumer := range claim.Status.ReservedFor {
		if consumer.Resource != "pods" {
			continue
		}
		podUIDs = append(podUIDs, consumer.UID)
	}
	return podUIDs
}

func getCDIDeviceName(uid types.UID) string {
	return fmt.Sprintf("claim-%s", uid)
}

// claimUIDFromDeviceName reverses getCDIDeviceName, and reports whether name
// has the shape this driver generates at all.
func claimUIDFromDeviceName(name string) (types.UID, bool) {
	uid, ok := strings.CutPrefix(name, "claim-")
	return types.UID(uid), ok
}

// reserveResourceClaimAllocation records a new claim allocation while applying
// the shared-pool guard for currently running shared containers. A shared
// container from the same pod may not have been created yet when this DRA hook
// runs, so that case is detected later by the NRI CreateContainer check.
//
// CCX-FORK: upstream passes one cpuset for the whole claim.
func (cp *CPUDriver) reserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, record store.ClaimRecord) error {
	hasSharedContainers := len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0
	return cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, record, hasSharedContainers)
}

// requestAllocations orders what each request of a claim was given by request
// name, so the store and the records it writes never depend on map order.
func requestAllocations(byRequest map[string]store.RequestAllocation) []store.RequestAllocation {
	names := make([]string, 0, len(byRequest))
	for name := range byRequest {
		names = append(names, name)
	}
	sort.Strings(names)
	requests := make([]store.RequestAllocation, 0, len(names))
	for _, name := range names {
		requests = append(requests, byRequest[name])
	}
	return requests
}

// addRequestCPUs merges one allocation result into the request it belongs to. A
// request satisfied by several devices holds their union, and every device of
// one request has the same role, since a device class selects one partition.
func addRequestCPUs(byRequest map[string]store.RequestAllocation, name string, cpus cpuset.CPUSet, role store.Role) {
	existing := byRequest[name]
	byRequest[name] = store.RequestAllocation{
		Request: name,
		CPUs:    existing.CPUs.Union(cpus),
		Role:    role,
	}
}

// CCX-FORK: upstream takes the logger and the claim alone. Whether the claim's
// CPUs may later change is the claim's own answer, read once above.
func (cp *CPUDriver) prepareGroupedResourceClaim(logger logr.Logger, claim *resourceapi.ResourceClaim, placement opaqueapi.ClaimPlacement) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing grouped resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	if existing, ok := cp.cpuAllocationStore.GetClaimRecord(claim.UID); ok {
		logger.V(2).Info("claim already has allocated CPUs in store, reusing assignment", "cpus", store.UnionOf(existing.Requests).String())
		// Even if the claim is already allocated in our in-memory store (which happens when a duplicate prepare
		// call is invoked without an intermediate unprepare), we must call prepareDevices and return the result back to Kubelet.
		// If the CDI file is already created on disk, the CDI manager will safely overwrite it with the same configuration.
		// This ensures that the CDI specification file is written/recreated on disk (for example, if the driver
		// pod restarted and synchronized its memory store from the runtime but did not recreate the CDI files on disk).
		return cp.prepareDevices(logger, claim, existing)
	}

	var cpuAssignment cpuset.CPUSet
	byRequest := map[string]store.RequestAllocation{}
	allocatableCPUs := cp.cpuAllocationStore.GetSharedCPUs()
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		// CCX-FORK: upstream takes every result's CPUs out of the device's
		// capacity, since each of its devices is held by one claim. A pool
		// device is claimed, not carved up: the claim is given its whole CPU
		// set, and the amount the allocator charged bounds how much work lands
		// there rather than naming a number of CPUs to take.
		if cp.topology.deviceNameToRole[alloc.Device] == device.PARTITION_ROLE_SHARED {
			poolCPUs, published := cp.topology.deviceNameToCPUs[alloc.Device]
			if !published {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("device %q was not published by this driver", alloc.Device)}
			}
			addRequestCPUs(byRequest, alloc.Request, poolCPUs, store.RoleShared)
			logger.V(2).Info("claimed a CPU pool", "device", alloc.Device, "cpus", poolCPUs.String())
			continue
		}
		quantity, ok := alloc.ConsumedCapacity[device.CPUResourceQualifiedName]
		if !ok {
			return kubeletplugin.PrepareResult{Err: fmt.Errorf("CPU capacity %q for device %q is missing", device.CPUResourceQualifiedName, alloc.Device)}
		}
		if quantity.Sign() <= 0 {
			return kubeletplugin.PrepareResult{Err: fmt.Errorf("CPU capacity for device %q must be positive, got %s", alloc.Device, quantity.String())}
		}
		claimCPUCount := quantity.Value()
		if quantity.CmpInt64(claimCPUCount) != 0 {
			return kubeletplugin.PrepareResult{Err: fmt.Errorf("CPU capacity for device %q must be a whole number, got %s", alloc.Device, quantity.String())}
		}
		// The device's requestPolicy makes the scheduler round a request up to
		// whole cores, so an amount that is not a multiple means the policy was
		// not applied -- most likely DRAConsumableCapacity is enabled on the
		// apiserver but not on kube-scheduler. Say so, because the alternative
		// is an obscure allocation failure below. Read per device: another
		// device's non-uniform cores must not excuse this one's mismatch.
		threadsPerCore := cp.topology.deviceThreadsPerCore[alloc.Device]
		if threadsPerCore > 1 && claimCPUCount%int64(threadsPerCore) != 0 {
			return kubeletplugin.PrepareResult{Err: fmt.Errorf("device %q consumed %d CPUs, which is not a multiple of the %d-CPU core size: the scheduler did not honour the capacity requestPolicy, check that the DRAConsumableCapacity feature gate is enabled on kube-scheduler",
				alloc.Device, claimCPUCount, threadsPerCore)}
		}
		logger.V(4).Info("found CPU request", "numCPUs", claimCPUCount, "device", alloc.Device)

		topo := cp.topology.cpuTopology

		var cur cpuset.CPUSet
		var err error

		// CCX-FORK: upstream takes the CPUs of the socket or NUMA node the device
		// groups. A device answers for its partition's share of that group, so
		// its own published CPUs are the set a claim on it may take from, and
		// that holds however small the group is -- a whole cache included.
		deviceCPUs, published := cp.topology.deviceNameToCPUs[alloc.Device]

		switch cp.cpuDeviceGroupBy {
		case device.GROUP_BY_SOCKET, device.GROUP_BY_NUMA_NODE, device.GROUP_BY_UNCORE_CACHE:
			if !published {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("device %q was not published by this driver", alloc.Device)}
			}
			availableCPUsForDevice := allocatableCPUs.Difference(cpuAssignment).Intersection(deviceCPUs)
			logger.V(4).Info("device CPU availability", "device", alloc.Device, "deviceCPUs", deviceCPUs.String(), "availableCPUs", availableCPUsForDevice.String())
			cur, err = cp.takeCPUsForDevice(logger, topo, availableCPUsForDevice, int(claimCPUCount), threadsPerCore)
		case device.GROUP_BY_MACHINE:
			if !published {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("device %q was not published by this driver", alloc.Device)}
			}
			opaqueCPUSet, ok, err := cp.getOpaqueCPUSet(logger, claim.Status.Allocation, alloc)
			if err != nil {
				return kubeletplugin.PrepareResult{Err: err}
			}
			if !ok {
				return kubeletplugin.PrepareResult{Err: fmt.Errorf("no opaque cpuset configuration found for allocation request %q", alloc.Request)}
			}

			if err := cp.validateOpaqueCPUSet(opaqueCPUSet, cp.topology.onlineCPUs, cpuAssignment, claimCPUCount, deviceCPUs); err != nil {
				return kubeletplugin.PrepareResult{Err: err}
			}
			cur = opaqueCPUSet
			logger.V(2).Info("using opaque config CPU assignment", "device", alloc.Device, "assigned", cur.String())
		}

		if err != nil {
			return kubeletplugin.PrepareResult{Err: err}
		}
		cpuAssignment = cpuAssignment.Union(cur)
		addRequestCPUs(byRequest, alloc.Request, cur, store.RoleExclusive)
		logger.V(2).Info("CPU assignment for device", "device", alloc.Device, "assigned", cur.String(), "allAssigned", cpuAssignment.String())
	}

	if len(byRequest) == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	record := store.ClaimRecord{Requests: requestAllocations(byRequest), Relocatable: placement.Relocatable}
	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, record); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, record)
	if result.Err != nil {
		cp.cpuAllocationStore.RemoveResourceClaimAllocation(logger, claim.UID)
		return result
	}
	cp.metricsRecorder().RecordClaimAllocatedCPUs(cpuAssignment.Size())
	cp.refreshAllocationMetrics()
	return result
}

// takeCPUsForDevice picks the CPUs backing one device's share of a claim.
// threadsPerCore is this specific device's own effective allocation step (0
// when whole-core allocation is off or this device has no single thread
// count), not a node-wide answer: a different device's non-uniform cores must
// not change what this call does.
//
// With whole-core allocation in effect it takes complete physical cores, which
// also keeps the claim inside as few uncore caches as possible. Otherwise it uses
// the CPU-granular allocator, so behaviour is unchanged when the option is off.
func (cp *CPUDriver) takeCPUsForDevice(logger logr.Logger, topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int, threadsPerCore int) (cpuset.CPUSet, error) {
	if threadsPerCore > 1 {
		return coreselect.TakeWholeCoresPolicy(topo, available, numCPUs, cp.placementPolicy, cp.topology.reservedCPUs)
	}
	if cp.placementPolicy == coreselect.Spread {
		return coreselect.TakeSpreadCPUs(topo, available, numCPUs, cp.topology.reservedCPUs)
	}
	return cpumanager.TakeByTopologyNUMAPacked(logger, topo, available, numCPUs, cpumanager.CPUSortingStrategyPacked, true)
}

// CCX-FORK: upstream takes the logger and the claim alone, as above.
func (cp *CPUDriver) prepareResourceClaim(logger logr.Logger, claim *resourceapi.ResourceClaim, placement opaqueapi.ClaimPlacement) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing individual resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	claimCPUIDs := []int{}
	byRequest := map[string]store.RequestAllocation{}
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
			continue
		}
		cpuID, ok := cp.topology.deviceNameToCPUID[alloc.Device]
		if !ok {
			return kubeletplugin.PrepareResult{
				Err: fmt.Errorf("device %q not found in device to CPU ID map", alloc.Device),
			}
		}
		claimCPUIDs = append(claimCPUIDs, cpuID)
		addRequestCPUs(byRequest, alloc.Request, cpuset.New(cpuID), store.RoleExclusive)
	}

	if len(claimCPUIDs) == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	claimCPUSet := cpuset.New(claimCPUIDs...)
	if existing, ok := cp.cpuAllocationStore.GetClaimRecord(claim.UID); ok {
		existingCPUs := store.UnionOf(existing.Requests)
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
	allocatableCPUs := cp.cpuAllocationStore.GetSharedCPUs()
	if !claimCPUSet.IsSubsetOf(allocatableCPUs) {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has overlapping device assignment with other claims", claim.Namespace, claim.Name),
		}
	}

	record := store.ClaimRecord{Requests: requestAllocations(byRequest), Relocatable: placement.Relocatable}
	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, record); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, record)
	if result.Err != nil {
		cp.cpuAllocationStore.RemoveResourceClaimAllocation(logger, claim.UID)
		return result
	}
	cp.metricsRecorder().RecordClaimAllocatedCPUs(claimCPUSet.Size())
	cp.refreshAllocationMetrics()
	return result
}

// cdiEnvValue is what the injected variable says about a claim's placement: the
// cpuset when the claim's CPUs are fixed for the life of its containers, and
// cdiEnvDynamicValue when the claim permits them to change.
//
// Keyed on the claim rather than on whether defragmentation is enabled now,
// because the variable cannot be rewritten once the container exists: a claim
// that permits moves would be handed a cpuset that becomes a lie the moment the
// feature is switched on, and an immobile claim would be denied a cpuset that
// is true for its whole life.
//
// CCX-FORK: upstream always writes the cpuset.
func (cp *CPUDriver) cdiEnvValue(record store.ClaimRecord) string {
	if record.Relocatable {
		return cdiEnvDynamicValue
	}
	return store.UnionOf(record.Requests).String()
}

// CCX-FORK: upstream is handed the claim's cpuset and nothing else about the
// claim.
func (cp *CPUDriver) prepareDevices(logger logr.Logger, claim *resourceapi.ResourceClaim, record store.ClaimRecord) kubeletplugin.PrepareResult {
	deviceName := getCDIDeviceName(claim.UID)
	byRequest := make(map[string]store.RequestAllocation, len(record.Requests))
	for _, request := range record.Requests {
		byRequest[request.Request] = request
	}
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claim.UID, cp.cdiEnvValue(record))
	if err := cp.cdiMgr.AddDevice(logger, deviceName, envVar, record); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}

	qualifiedName := cdiparser.QualifiedName(cdiVendor, cdiClass, deviceName)
	logger.V(6).Info("prepared CDI device", "cdiDeviceName", deviceName, "envVar", envVar, "qualifiedName", qualifiedName)
	preparedDevices := []kubeletplugin.Device{}
	for _, allocResult := range claim.Status.Allocation.Devices.Results {
		if allocResult.Driver != cp.driverName {
			continue
		}
		preparedDevice := kubeletplugin.Device{
			PoolName:     allocResult.Pool,
			DeviceName:   allocResult.Device,
			CDIDeviceIDs: []string{qualifiedName},
		}
		if allocResult.Request != "" {
			preparedDevice.Requests = []string{allocResult.Request}
		}
		if attrs, ok := getDeviceAttributes(cp.topology.deviceSlices, allocResult.Device); ok && len(attrs) > 0 {
			metadataAttrs := make(map[string]resourceapi.DeviceAttribute, len(attrs))
			for k, v := range attrs {
				metadataAttrs[string(k)] = v
			}
			if quantity, ok := allocResult.ConsumedCapacity[device.CPUResourceQualifiedName]; ok {
				allocatedCount := quantity.Value()
				metadataAttrs[string(device.AttributeAllocatedNumCPUs)] = resourceapi.DeviceAttribute{
					IntValue: &allocatedCount,
				}
			}
			// CCX-FORK: upstream's metadata file carries the device attributes
			// and the allocated count alone. A pool never moves, so its
			// request's file may name the CPUs too; the file is written before
			// the container starts and cannot be rewritten afterwards, which is
			// why a claim whose placement may change reads its CPUs from the
			// kernel instead.
			if request, ok := byRequest[allocResult.Request]; ok && request.Role == store.RoleShared {
				metadataAttrs[string(device.AttributeCPUSet)] = resourceapi.DeviceAttribute{
					StringValue: new(request.CPUs.String()),
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

// UnprepareResourceClaims is called by the kubelet to unprepare the resources for a claim.
func (cp *CPUDriver) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))

	logger.V(4).Info("begin: unpreparing resource claims", "numClaims", len(claims))
	defer logger.V(4).Info("end: unpreparing resource claims", "numClaims", len(claims))

	result := make(map[types.UID]error)

	if len(claims) == 0 {
		return result, nil
	}

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	for _, claim := range claims {
		// note kubeletplugin.NamespacedObject doesn't implement KMetadata
		cLogger := logger.WithValues("claim", claim.String(), "claimUID", claim.UID)
		cLogger.V(2).Info("unpreparing resource claim")
		err := cp.unprepareResourceClaim(cLogger, claim)
		result[claim.UID] = err
		if err != nil {
			cLogger.Error(err, "error unpreparing resources for claim")
			cp.metricsRecorder().RecordUnprepare(cpumetrics.ResultError)
		} else {
			cp.metricsRecorder().RecordUnprepare(cpumetrics.ResultSuccess)
			cp.refreshAllocationMetrics()
		}
	}
	// CCX-FORK: a released claim can leave a cache empty, which the published
	// order depends on; upstream's order depends on nothing.
	cp.republishOnCacheOrderChange(ctx)
	return result, nil
}

func (cp *CPUDriver) metricsRecorder() cpumetrics.Recorder {
	if cp.metrics == nil {
		return cpumetrics.Noop()
	}
	return cp.metrics
}

func (cp *CPUDriver) refreshAllocationMetrics() {
	if cp.cpuAllocationStore == nil {
		return
	}
	snapshot := cp.cpuAllocationStore.Snapshot()
	cp.metricsRecorder().SetAllocationState(cpumetrics.AllocationState{
		AllocatedCPUs:        snapshot.AllocatedCPUs,
		AvailableCPUs:        snapshot.AvailableCPUs,
		ReservedCPUs:         snapshot.ReservedCPUs,
		ActiveResourceClaims: snapshot.ActiveResourceClaims,
	})
}

func (cp *CPUDriver) unprepareResourceClaim(logger logr.Logger, claim kubeletplugin.NamespacedObject) error {
	// Remove the CDI spec first. If that fails, keep the allocation recorded so
	// the driver does not make those CPUs available while stale CDI state remains.
	if err := cp.cdiMgr.RemoveDevice(logger, getCDIDeviceName(claim.UID)); err != nil {
		return err
	}
	cp.cpuAllocationStore.RemoveResourceClaimAllocation(logger, claim.UID)
	cp.claimTracker.Cleanup(claim.UID)
	// The released CPUs are back in the shared pool now, but the containers
	// entitled to them still hold the narrower cpuset. Hand that off to the
	// worker rather than doing it here: kubelet must not wait on an NRI round
	// trip, and an unsolicited update issued from inside a hook is what
	// deadlocks a runtime with a pre-nri#301 Adaptation.
	cp.requestReconcile()
	return nil
}

// HandleError is called by the kubelet plugin framework when an error occurs in the background,
// for example while publishing ResourceSlices.
func (cp *CPUDriver) HandleError(ctx context.Context, err error, msg string) {
	logger := ctxlog.FromContext(ctx)

	// Log the error using the standard Kubernetes error handler
	runtime.HandleErrorWithContext(ctx, err, msg)

	// For unrecoverable errors, exit immediately with a clear error message.
	// This fail-fast behavior is intentional for early project maturity to surface
	// issues quickly rather than silently continuing in a broken state.
	if !errors.Is(err, kubeletplugin.ErrRecoverable) {
		logger.Error(err, "fatal unrecoverable error in DRA driver, exiting",
			"driver", cp.driverName,
			"node", cp.nodeName,
			"message", msg,
		)
		ctxlog.Flush()
		os.Exit(1)
	}
}

func (cp *CPUDriver) getOpaqueCPUSet(logger logr.Logger, allocation *resourceapi.AllocationResult, alloc resourceapi.DeviceRequestAllocationResult) (cpuset.CPUSet, bool, error) {
	if allocation == nil {
		return cpuset.CPUSet{}, false, nil
	}

	configs := resourceclaim.ConfigForResult(allocation.Devices.Config, alloc)
	var matchedConfig *resourceapi.DeviceAllocationConfiguration
	matchCount := 0

	for i := range configs {
		config := &configs[i]
		if config.Opaque.Driver != cp.driverName {
			continue
		}
		if config.Source != resourceapi.AllocationConfigSourceClaim {
			return cpuset.CPUSet{}, false, fmt.Errorf("opaque config: configuration from DeviceClass is not supported by this driver, custom cpusets must be defined per ResourceClaim request")
		}
		matchedConfig = config
		matchCount++
	}

	if matchCount != 1 {
		return cpuset.CPUSet{}, false, fmt.Errorf("opaque config: request %q is targeted by %d configurations, must be targeted by exactly 1", alloc.Request, matchCount)
	}

	// Return the matched config if found
	if matchedConfig != nil && len(matchedConfig.Opaque.Parameters.Raw) > 0 {
		parsed, err := opaqueapi.ParseOpaqueConfig(matchedConfig.Opaque.Parameters.Raw)
		if err != nil {
			return cpuset.CPUSet{}, false, err
		}
		// CCX-FORK: upstream's parse refuses a configuration that names no
		// cpuset, since to it a configuration is nothing else. One may now say
		// only what the claim tolerates, so the demand belongs here, where a
		// cpuset is what is being asked for.
		if !parsed.HasCPUs {
			return cpuset.CPUSet{}, false, fmt.Errorf("opaque config: cpuConfig.cpuset is empty or missing")
		}
		logger.V(4).Info("found cpuset override in opaque config", "request", alloc.Request, "cpuset", parsed.CPUs.String())
		return parsed.CPUs, true, nil
	}

	return cpuset.CPUSet{}, false, nil
}

// CCX-FORK: upstream checks the named CPUs against the node, which has one
// device; here they are checked against the device they were allocated on.
func (cp *CPUDriver) validateOpaqueCPUSet(opaqueCPUSet cpuset.CPUSet, onlineCPUs cpuset.CPUSet, cpuAssignment cpuset.CPUSet, claimCPUCount int64, deviceCPUs cpuset.CPUSet) error {
	// Verify core count matches requested capacity
	if int64(opaqueCPUSet.Size()) != claimCPUCount {
		return fmt.Errorf("opaque config cpuset size %d does not match requested capacity %d", opaqueCPUSet.Size(), claimCPUCount)
	}

	// Verify CPUs are online
	if !opaqueCPUSet.IsSubsetOf(onlineCPUs) {
		offlineCPUs := opaqueCPUSet.Difference(onlineCPUs)
		return fmt.Errorf("requested CPUs %s from opaque config contain offline cores: %s", opaqueCPUSet.String(), offlineCPUs.String())
	}

	// Verify CPUs are not part of --reserved-cpus config passed to the driver
	reservedCPUs := cp.cpuAllocationStore.GetReservedCPUs()
	reservedOverlap := opaqueCPUSet.Intersection(reservedCPUs)
	if reservedOverlap.Size() > 0 {
		return fmt.Errorf("requested CPUs %s from opaque config contain reserved cores: %s", opaqueCPUSet.String(), reservedOverlap.String())
	}

	// Verify cores do not overlap with other claims prepared in this same batch
	currentClaimCPUs := opaqueCPUSet.Intersection(cpuAssignment)
	if currentClaimCPUs.Size() > 0 {
		return fmt.Errorf("requested CPUs %s from opaque config are already assigned to another device in this claim", opaqueCPUSet.String())
	}

	// Verify cores do not overlap with other active claims on this node
	existingClaimCPUs := cp.cpuAllocationStore.GetPreparedCPUs()
	if opaqueCPUSet.Intersection(existingClaimCPUs).Size() > 0 {
		return fmt.Errorf("requested CPUs %s from opaque config conflict with already allocated claims", opaqueCPUSet.String())
	}

	// CCX-FORK: the claim names its own CPUs here, so nothing else keeps them
	// inside the partition whose device it was allocated on. Last of the
	// membership checks, so that a CPU which is offline, reserved or already
	// taken is reported as such rather than as belonging elsewhere.
	if outside := opaqueCPUSet.Difference(deviceCPUs); !outside.IsEmpty() {
		return fmt.Errorf("requested CPUs %s from opaque config are outside the device's own CPUs: %s belong to another partition",
			opaqueCPUSet.String(), outside.String())
	}

	// Under whole-core allocation an explicit cpuset must not split a core
	// either, or one tenant's SMT sibling ends up serving another workload.
	// This is a promise about the machine-grouped device as a whole, so it
	// only needs to know the option is requested, unlike the per-device
	// checks above: CompleteCores is correct on any subset regardless of
	// whether every core on the node shares one thread count.
	if cp.fullPhysicalCPUsOnly {
		complete := cp.topology.cpuTopology.CPUDetails.CompleteCores(opaqueCPUSet)
		if !complete.Equals(opaqueCPUSet) {
			return fmt.Errorf("requested CPUs %s from opaque config split a physical core, which fullPhysicalCPUsOnly forbids: %s have no sibling in the set",
				opaqueCPUSet.String(), opaqueCPUSet.Difference(complete).String())
		}
	}

	return nil
}
