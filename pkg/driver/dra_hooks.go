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
		if cp.cpuDeviceMode == device.CPU_DEVICE_MODE_GROUPED {
			result[claim.UID] = cp.prepareGroupedResourceClaim(cLogger, claim)
		} else {
			result[claim.UID] = cp.prepareResourceClaim(cLogger, claim)
		}
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
	return result, nil
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
func (cp *CPUDriver) reserveResourceClaimAllocation(logger logr.Logger, claimUID types.UID, requests []store.RequestAllocation) error {
	hasSharedContainers := len(cp.podConfigStore.GetContainersWithSharedCPUs()) > 0
	return cp.cpuAllocationStore.ReserveResourceClaimAllocation(logger, claimUID, requests, hasSharedContainers)
}

// Every device this driver publishes grants CPUs its claim holds alone, so
// every request of a claim it prepares is exclusive.
func requestAllocations(byRequest map[string]cpuset.CPUSet) []store.RequestAllocation {
	names := make([]string, 0, len(byRequest))
	for name := range byRequest {
		names = append(names, name)
	}
	sort.Strings(names)
	requests := make([]store.RequestAllocation, 0, len(names))
	for _, name := range names {
		requests = append(requests, store.RequestAllocation{
			Request: name,
			CPUs:    byRequest[name],
			Role:    store.RoleExclusive,
		})
	}
	return requests
}

func (cp *CPUDriver) prepareGroupedResourceClaim(logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing grouped resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	if existing, ok := cp.cpuAllocationStore.GetResourceClaimRequests(claim.UID); ok {
		logger.V(2).Info("claim already has allocated CPUs in store, reusing assignment", "cpus", store.UnionOf(existing).String())
		// Even if the claim is already allocated in our in-memory store (which happens when a duplicate prepare
		// call is invoked without an intermediate unprepare), we must call prepareDevices and return the result back to Kubelet.
		// If the CDI file is already created on disk, the CDI manager will safely overwrite it with the same configuration.
		// This ensures that the CDI specification file is written/recreated on disk (for example, if the driver
		// pod restarted and synchronized its memory store from the runtime but did not recreate the CDI files on disk).
		return cp.prepareDevices(logger, claim, existing)
	}

	var cpuAssignment cpuset.CPUSet
	byRequest := map[string]cpuset.CPUSet{}
	allocatableCPUs := cp.cpuAllocationStore.GetSharedCPUs()
	for _, alloc := range claim.Status.Allocation.Devices.Results {
		if alloc.Driver != cp.driverName {
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
		// its own published CPUs are the set a claim on it may take from.
		deviceCPUs, published := cp.topology.deviceNameToCPUs[alloc.Device]

		switch cp.cpuDeviceGroupBy {
		case device.GROUP_BY_SOCKET, device.GROUP_BY_NUMA_NODE:
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
		byRequest[alloc.Request] = byRequest[alloc.Request].Union(cur)
		logger.V(2).Info("CPU assignment for device", "device", alloc.Device, "assigned", cur.String(), "allAssigned", cpuAssignment.String())
	}

	if cpuAssignment.Size() == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	requests := requestAllocations(byRequest)
	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, requests); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, requests)
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

func (cp *CPUDriver) prepareResourceClaim(logger logr.Logger, claim *resourceapi.ResourceClaim) kubeletplugin.PrepareResult {
	logger.V(4).Info("preparing individual resource claim")

	if claim.Status.Allocation == nil {
		return kubeletplugin.PrepareResult{
			Err: fmt.Errorf("claim %s/%s has no allocation", claim.Namespace, claim.Name),
		}
	}

	claimCPUIDs := []int{}
	byRequest := map[string]cpuset.CPUSet{}
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
		byRequest[alloc.Request] = byRequest[alloc.Request].Union(cpuset.New(cpuID))
	}

	if len(claimCPUIDs) == 0 {
		logger.V(6).Info("claim has no CPU allocations for this driver")
		return kubeletplugin.PrepareResult{}
	}

	claimCPUSet := cpuset.New(claimCPUIDs...)
	if existing, ok := cp.cpuAllocationStore.GetResourceClaimRequests(claim.UID); ok {
		existingCPUs := store.UnionOf(existing)
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

	requests := requestAllocations(byRequest)
	// Reserve before CDI I/O so concurrent Prepare calls cannot select the same CPUs.
	if err := cp.reserveResourceClaimAllocation(logger, claim.UID, requests); err != nil {
		return kubeletplugin.PrepareResult{Err: err}
	}
	result := cp.prepareDevices(logger, claim, requests)
	if result.Err != nil {
		cp.cpuAllocationStore.RemoveResourceClaimAllocation(logger, claim.UID)
		return result
	}
	cp.metricsRecorder().RecordClaimAllocatedCPUs(claimCPUSet.Size())
	cp.refreshAllocationMetrics()
	return result
}

// cdiEnvValue is what the injected variable says about a claim's placement: the
// cpuset while placement is fixed for the claim's lifetime, and cdiEnvDynamicValue
// once a pass may move it.
//
// CCX-FORK: upstream always writes the cpuset.
func (cp *CPUDriver) cdiEnvValue(cpus cpuset.CPUSet) string {
	if cp.defrag.enabled {
		return cdiEnvDynamicValue
	}
	return cpus.String()
}

func (cp *CPUDriver) prepareDevices(logger logr.Logger, claim *resourceapi.ResourceClaim, requests []store.RequestAllocation) kubeletplugin.PrepareResult {
	deviceName := getCDIDeviceName(claim.UID)
	envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, claim.UID, cp.cdiEnvValue(store.UnionOf(requests)))
	// CCX-FORK: the requests argument is the fork's placement record.
	if err := cp.cdiMgr.AddDevice(logger, deviceName, envVar, requests); err != nil {
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
		parsedCPUSet, err := opaqueapi.ParseOpaqueConfig(matchedConfig.Opaque.Parameters.Raw)
		if err != nil {
			return cpuset.CPUSet{}, false, err
		}
		logger.V(4).Info("found cpuset override in opaque config", "request", alloc.Request, "cpuset", parsedCPUSet.String())
		return parsedCPUSet, true, nil
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
