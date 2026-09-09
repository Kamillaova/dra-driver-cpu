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
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"

	v1alpha1 "github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
)

type oracleTopologyConfig struct {
	numaNodes     int
	cachesPerNUMA int
	cpusPerCache  []int
	partitions    []devattr.Partition
}

type oracleClaim struct {
	uid           types.UID
	namespace     string
	name          string
	size          int
	relocatable   bool
	alignment     v1alpha1.Alignment
	flexible      bool
	podUID        types.UID
	containerName string
	containerUID        types.UID
	sharedContainerUIDs []types.UID
	chargedCache        string

	isAllocated   bool
	isPrepared    bool
	isDeallocated bool
	isDeleted     bool
	isUnprepared  bool

	initialCPUs cpuset.CPUSet
	currentCPUs cpuset.CPUSet
}

func (c *oracleClaim) allContainers() []types.UID {
	res := []types.UID{c.containerUID}
	return append(res, c.sharedContainerUIDs...)
}

type oracleClaimReader struct {
	mu          sync.RWMutex
	claims      []*resourceapi.ResourceClaim
	deallocated map[types.UID]bool
	projected   *v1alpha1.ProjectedClaims
}

func (r *oracleClaimReader) AllocatedClaims() ([]*resourceapi.ResourceClaim, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return slices.Clone(r.claims), nil
}

func (r *oracleClaimReader) IsProjectedDeallocated(claimUID types.UID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.deallocated != nil && r.deallocated[claimUID]
}

func (r *oracleClaimReader) GetProjectedClaims() (*v1alpha1.ProjectedClaims, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.projected == nil {
		return nil, nil
	}
	clone := *r.projected
	clone.Claims = slices.Clone(r.projected.Claims)
	return &clone, nil
}

type oracleHarness struct {
	t                    *testing.T
	driver               *CPUDriver
	mockCDI              *mockCdiMgr
	fakeUpdater          *fakeContainerUpdater
	claimReader          *oracleClaimReader
	cgroups              fstest.MapFS
	claims               map[types.UID]*oracleClaim
	activeTransitClaims  map[types.UID]struct{}
	inTransit            bool
	topoCfg              oracleTopologyConfig
	baseTopology         deviceTopology
	partitions           []devattr.Partition
	defaultPartitionCPUs cpuset.CPUSet
	cacheIDToCPUs        map[int]cpuset.CPUSet
	cacheIDToNUMA        map[int]int
	cacheIDToPartition   map[int]string
	deviceNameToCacheID  map[string]int
}

func newOracleHarness(t *testing.T, cfg oracleTopologyConfig) *oracleHarness {
	t.Helper()

	totalCaches := cfg.numaNodes * cfg.cachesPerNUMA
	cacheSizes := make([]int, totalCaches)
	for i := range totalCaches {
		if len(cfg.cpusPerCache) == 1 {
			cacheSizes[i] = cfg.cpusPerCache[0]
		} else if i < len(cfg.cpusPerCache) {
			cacheSizes[i] = cfg.cpusPerCache[i]
		} else {
			cacheSizes[i] = cfg.cpusPerCache[len(cfg.cpusPerCache)-1]
		}
	}

	var infos []cpuinfo.CPUInfo
	cpuID := 0
	cacheIDToCPUs := make(map[int]cpuset.CPUSet)
	cacheIDToNUMA := make(map[int]int)
	cacheIDToPartition := make(map[int]string)
	deviceNameToCacheID := make(map[string]int)

	for cID := range totalCaches {
		numaID := cID / cfg.cachesPerNUMA
		cacheIDToNUMA[cID] = numaID
		cacheCPUs := cpuset.New()
		size := cacheSizes[cID]
		for range size {
			infos = append(infos, cpuinfo.CPUInfo{
				CpuID:         cpuID,
				CoreID:        cpuID,
				SocketID:      numaID,
				NUMANodeID:    numaID,
				UncoreCacheID: cID,
			})
			cacheCPUs = cacheCPUs.Union(cpuset.New(cpuID))
			cpuID++
		}
		cacheIDToCPUs[cID] = cacheCPUs
		devName := cacheDevice(cID)
		deviceNameToCacheID[devName] = cID
	}

	topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(testr.New(t))
	require.NoError(t, err)

	allCPUs := topo.CPUDetails.CPUs()
	cgroups := fstest.MapFS{}
	mockCDI := newMockCdiMgr()
	updater := &fakeContainerUpdater{}
	reader := &oracleClaimReader{
		deallocated: make(map[types.UID]bool),
		projected:   &v1alpha1.ProjectedClaims{Generation: 1, APIVersion: v1alpha1.APIVersion},
	}
	reg := prometheus.NewRegistry()

	d := &CPUDriver{
		nodeName:         "test-node",
		driverName:       "dra.cpu",
		cdiMgr:           mockCDI,
		containerUpdater: updater,
		claimReader:      reader,
		metrics:          cpumetrics.New(reg),
		cgroupfs:         cgroups,
		poisonedNodes:    make(map[int]*poisonedNode),
		pendingRounds:    make(map[defragScope]*defragRound),
		defragRetries:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[defragScope]()),
		defragRetryDue:   make(chan defragScope),
		makeRoomTargets:  make(map[types.UID]*makeRoomTarget),
		reconcileTrigger: make(chan struct{}, 1),
		sysfs: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte(allCPUs.String() + "\n")},
		},
		defrag:                  defragOptions{enabled: true, allowTransientOverlap: true, batchTimeout: defaultDefragBatchTimeout},
		cpuDeviceGroupBy:        devattr.GROUP_BY_UNCORE_CACHE,
		cpuDeviceMode:           devattr.CPU_DEVICE_MODE_GROUPED,
		devicesPerResourceSlice: 64,
		podConfigStore:          store.NewPodConfig(),
		claimTracker:            store.NewClaimTracker(),
		cpuAllocationStore:      store.NewCPUAllocation(topo, cpuset.New()),
		topology: deviceTopology{
			cpuTopology:               topo,
			reservedCPUs:              cpuset.New(),
			onlineCPUs:                allCPUs,
			deviceNameToCPUs:          make(map[string]cpuset.CPUSet),
			deviceNameToUncoreCacheID: make(map[string]int),
			deviceNameToNUMANodeID:    make(map[string]int),
			deviceNameToPartition:     make(map[string]string),
			deviceNameToRole:          make(map[string]string),
		},
	}
	t.Cleanup(d.defragRetries.ShutDown)

	if len(cfg.partitions) > 0 {
		d.partitions = devattr.WithImplicitDefault(cfg.partitions, allCPUs)
	} else {
		d.partitions = []devattr.Partition{
			{Name: devattr.DefaultPartitionName, Role: devattr.PARTITION_ROLE_DEFAULT, CPUs: allCPUs},
		}
	}
	d.defaultPartitionCPUs = cpuset.New()
	for _, p := range d.partitions {
		if p.Role == devattr.PARTITION_ROLE_DEFAULT {
			d.defaultPartitionCPUs = d.defaultPartitionCPUs.Union(p.CPUs)
		}
	}

	var partitionDevices []resourceapi.Device
	for cID, cCPUs := range cacheIDToCPUs {
		devName := cacheDevice(cID)
		d.topology.deviceNameToCPUs[devName] = cCPUs
		d.topology.deviceNameToUncoreCacheID[devName] = cID
		d.topology.deviceNameToNUMANodeID[devName] = cacheIDToNUMA[cID]

		partName := devattr.DefaultPartitionName
		partRole := devattr.PARTITION_ROLE_DEFAULT
		for _, p := range d.partitions {
			if cCPUs.IsSubsetOf(p.CPUs) {
				partName = p.Name
				partRole = p.Role
				break
			}
		}
		d.topology.deviceNameToPartition[devName] = partName
		d.topology.deviceNameToRole[devName] = partRole
		cacheIDToPartition[cID] = partName

		devObj := resourceapi.Device{
			Name: devName,
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				devattr.CPUResourceQualifiedName: {
					Value: *resource.NewQuantity(int64(cCPUs.Size()), resource.DecimalSI),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min:  resource.NewQuantity(2, resource.DecimalSI),
							Step: resource.NewQuantity(2, resource.DecimalSI),
						},
					},
				},
			},
		}
		partitionDevices = append(partitionDevices, devObj)
	}
	d.topology.devicesByPartition = [][]resourceapi.Device{partitionDevices}

	return &oracleHarness{
		t:                    t,
		driver:               d,
		mockCDI:              mockCDI,
		fakeUpdater:          updater,
		claimReader:          reader,
		cgroups:              cgroups,
		claims:               make(map[types.UID]*oracleClaim),
		activeTransitClaims:  make(map[types.UID]struct{}),
		topoCfg:              cfg,
		baseTopology:         d.topology,
		partitions:           d.partitions,
		defaultPartitionCPUs: d.defaultPartitionCPUs,
		cacheIDToCPUs:        cacheIDToCPUs,
		cacheIDToNUMA:        cacheIDToNUMA,
		cacheIDToPartition:   cacheIDToPartition,
		deviceNameToCacheID:  deviceNameToCacheID,
	}
}

func (h *oracleHarness) createClaim(uid string, size int, chargedCacheID int, relocatable bool, alignment v1alpha1.Alignment, flexible bool) *oracleClaim {
	c := &oracleClaim{
		uid:           types.UID(uid),
		namespace:     "default",
		name:          "pod-" + uid,
		size:          size,
		relocatable:   relocatable,
		alignment:     alignment,
		flexible:      flexible,
		podUID:        types.UID("pod-uid-" + uid),
		containerName: "ctr-" + uid,
		containerUID:  types.UID("ctr-uid-" + uid),
		chargedCache:  cacheDevice(chargedCacheID),
	}
	h.claims[c.uid] = c
	return c
}

func (h *oracleHarness) createJointClaim(uid string, podUID, containerUID types.UID, containerName string, size int, chargedCacheID int, relocatable bool, alignment v1alpha1.Alignment, flexible bool) *oracleClaim {
	c := &oracleClaim{
		uid:           types.UID(uid),
		namespace:     "default",
		name:          "pod-" + string(podUID),
		size:          size,
		relocatable:   relocatable,
		alignment:     alignment,
		flexible:      flexible,
		podUID:        podUID,
		containerName: containerName,
		containerUID:  containerUID,
		chargedCache:  cacheDevice(chargedCacheID),
	}
	h.claims[c.uid] = c
	return c
}

func getContainerLiveCPUs(cgroups fstest.MapFS, containerUID types.UID) cpuset.CPUSet {
	dir, err := cgroupfs.Dir("/kubepods/" + string(containerUID))
	if err != nil {
		return cpuset.New()
	}
	f, ok := cgroups[dir+"/cpuset.cpus.effective"]
	if !ok || f == nil {
		return cpuset.New()
	}
	parsed, err := cpuset.Parse(strings.TrimSpace(string(f.Data)))
	if err != nil {
		return cpuset.New()
	}
	return parsed
}

func (h *oracleHarness) syncContainerLiveCPUs(ctrUID types.UID) {
	ctrCPUs := cpuset.New()
	for _, c := range h.claims {
		if !c.isPrepared {
			continue
		}
		if c.containerUID == ctrUID || slices.Contains(c.sharedContainerUIDs, ctrUID) {
			ctrCPUs = ctrCPUs.Union(c.currentCPUs)
		}
	}
	setContainerLiveCPUs(h.cgroups, ctrUID, ctrCPUs)
}

func (h *oracleHarness) buildResourceClaim(c *oracleClaim) *resourceapi.ResourceClaim {
	opaqueCfg := v1alpha1.OpaqueConfig{
		APIVersion: v1alpha1.APIVersion,
		CPUConfig: v1alpha1.CPUConfig{
			Relocatable: c.relocatable,
			Alignment:   c.alignment,
		},
	}
	opaqueParams, err := json.Marshal(opaqueCfg)
	require.NoError(h.t, err)

	var requests []resourceapi.DeviceRequest
	if c.flexible || c.alignment != "" {
		requests = []resourceapi.DeviceRequest{
			{
				Name: "req-0",
				FirstAvailable: []resourceapi.DeviceSubRequest{
					{Name: "subreq-0"},
					{Name: "subreq-1"},
				},
			},
		}
	} else {
		requests = []resourceapi.DeviceRequest{
			{
				Name: "req-0",
			},
		}
	}

	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			UID:       c.uid,
			Namespace: c.namespace,
			Name:      c.name,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: requests,
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Config: []resourceapi.DeviceAllocationConfiguration{
						{
							Source: resourceapi.AllocationConfigSourceClaim,
							DeviceConfiguration: resourceapi.DeviceConfiguration{
								Opaque: &resourceapi.OpaqueDeviceConfiguration{
									Driver: h.driver.driverName,
									Parameters: k8sruntime.RawExtension{
										Raw: opaqueParams,
									},
								},
							},
						},
					},
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver:  h.driver.driverName,
							Pool:    h.driver.nodeName,
							Device:  c.chargedCache,
							Request: "req-0",
							ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{
								devattr.CPUResourceQualifiedName: *resource.NewQuantity(int64(c.size), resource.DecimalSI),
							},
						},
					},
				},
			},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{UID: c.podUID},
			},
		},
	}
}

func (h *oracleHarness) Allocate(claim *oracleClaim) {
	h.t.Helper()
	claim.isAllocated = true
	claim.isDeallocated = false
	claim.isDeleted = false

	h.claimReader.mu.Lock()
	pClaim := v1alpha1.ProjectedClaim{
		UID:       string(claim.uid),
		Namespace: claim.namespace,
		Name:      claim.name,
		State:     v1alpha1.ClaimStateAllocated,
		Devices: []v1alpha1.ProjectedDevice{
			{
				Request: "req-0",
				Pool:    h.driver.nodeName,
				Device:  claim.chargedCache,
			},
		},
	}
	replaced := false
	for i, pc := range h.claimReader.projected.Claims {
		if pc.UID == string(claim.uid) {
			h.claimReader.projected.Claims[i] = pClaim
			replaced = true
			break
		}
	}
	if !replaced {
		h.claimReader.projected.Claims = append(h.claimReader.projected.Claims, pClaim)
	}

	rc := h.buildResourceClaim(claim)
	rcReplaced := false
	for i, existing := range h.claimReader.claims {
		if existing.UID == claim.uid {
			h.claimReader.claims[i] = rc
			rcReplaced = true
			break
		}
	}
	if !rcReplaced {
		h.claimReader.claims = append(h.claimReader.claims, rc)
	}
	h.claimReader.deallocated[claim.uid] = false
	h.claimReader.mu.Unlock()

	h.AssertInvariants()
}

func (h *oracleHarness) Prepare(claim *oracleClaim) {
	h.t.Helper()
	rc := h.buildResourceClaim(claim)
	results, err := h.driver.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{rc})
	require.NoError(h.t, err)
	res, ok := results[claim.uid]
	require.True(h.t, ok)
	require.NoError(h.t, res.Err)

	claim.isPrepared = true
	claim.isUnprepared = false

	placed, ok := h.driver.cpuAllocationStore.GetResourceClaimAllocation(claim.uid)
	require.True(h.t, ok)
	if claim.initialCPUs.IsEmpty() {
		claim.initialCPUs = placed
	}
	claim.currentCPUs = placed

	h.driver.applyMu.Lock()
	_, _ = h.driver.claimTracker.SetOwner(testr.New(h.t), claim.podUID, claim.containerName, claim.uid)
	existingState := h.driver.podConfigStore.GetContainerState(claim.podUID, claim.containerName)
	if existingState != nil {
		allClaims := append(existingState.ClaimUIDs(), claim.uid)
		h.driver.podConfigStore.SetContainerState(claim.podUID,
			store.NewContainerState(claim.containerName, claim.containerUID, allClaims...).WithCgroup(cgroupOf(claim.containerUID)))
	} else {
		h.driver.podConfigStore.SetContainerState(claim.podUID,
			store.NewContainerState(claim.containerName, claim.containerUID, claim.uid).WithCgroup(cgroupOf(claim.containerUID)))
	}
	h.driver.applyMu.Unlock()

	for _, ctrUID := range claim.allContainers() {
		h.syncContainerLiveCPUs(ctrUID)
	}

	h.AssertInvariants()
}

func (h *oracleHarness) BeginMove(claim *oracleClaim, target cpuset.CPUSet) {
	h.t.Helper()
	require.True(h.t, claim.relocatable, "cannot move an immobile claim")
	h.inTransit = true
	h.activeTransitClaims[claim.uid] = struct{}{}

	logger := testr.New(h.t)
	require.NoError(h.t, h.driver.cpuAllocationStore.BeginRebind(logger, claim.uid, target))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacementWithRound(logger, claim.uid, &store.RoundProvenance{
		RoundID:  "round-move-" + string(claim.uid),
		Origin:   claim.currentCPUs,
		Target:   target,
		Partners: nil,
	}))
	h.driver.applyMu.Unlock()

	h.AssertInvariants()
}

func (h *oracleHarness) CommitMove(claim *oracleClaim, target cpuset.CPUSet) {
	h.t.Helper()
	logger := testr.New(h.t)
	claim.currentCPUs = target
	for _, ctrUID := range claim.allContainers() {
		h.syncContainerLiveCPUs(ctrUID)
	}

	require.NoError(h.t, h.driver.cpuAllocationStore.CommitRebind(logger, claim.uid))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claim.uid))
	h.driver.applyMu.Unlock()

	delete(h.activeTransitClaims, claim.uid)
	h.inTransit = len(h.activeTransitClaims) > 0

	h.AssertInvariants()
}

func (h *oracleHarness) AbortMove(claim *oracleClaim) {
	h.t.Helper()
	logger := testr.New(h.t)
	require.NoError(h.t, h.driver.cpuAllocationStore.AbortRebind(logger, claim.uid))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claim.uid))
	h.driver.applyMu.Unlock()

	delete(h.activeTransitClaims, claim.uid)
	h.inTransit = len(h.activeTransitClaims) > 0

	h.AssertInvariants()
}

func (h *oracleHarness) BeginSwap(claimA, claimB *oracleClaim) {
	h.t.Helper()
	require.True(h.t, claimA.relocatable && claimB.relocatable, "swap partners must both be relocatable")
	h.inTransit = true
	h.activeTransitClaims[claimA.uid] = struct{}{}
	h.activeTransitClaims[claimB.uid] = struct{}{}

	logger := testr.New(h.t)
	targets := map[types.UID]cpuset.CPUSet{
		claimA.uid: claimB.currentCPUs,
		claimB.uid: claimA.currentCPUs,
	}
	require.NoError(h.t, h.driver.cpuAllocationStore.BeginSwap(logger, targets))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacementWithRound(logger, claimA.uid, &store.RoundProvenance{
		RoundID:  "swap-round-1",
		Origin:   claimA.currentCPUs,
		Target:   claimB.currentCPUs,
		Partners: []types.UID{claimB.uid},
	}))
	require.NoError(h.t, h.driver.writeClaimPlacementWithRound(logger, claimB.uid, &store.RoundProvenance{
		RoundID:  "swap-round-1",
		Origin:   claimB.currentCPUs,
		Target:   claimA.currentCPUs,
		Partners: []types.UID{claimA.uid},
	}))
	h.driver.applyMu.Unlock()

	h.AssertInvariants()
}

func (h *oracleHarness) CommitSwap(claimA, claimB *oracleClaim) {
	h.t.Helper()
	logger := testr.New(h.t)

	targetA := claimB.currentCPUs
	targetB := claimA.currentCPUs
	claimA.currentCPUs = targetA
	claimB.currentCPUs = targetB

	for _, c := range []*oracleClaim{claimA, claimB} {
		for _, ctrUID := range c.allContainers() {
			h.syncContainerLiveCPUs(ctrUID)
		}
	}

	require.NoError(h.t, h.driver.cpuAllocationStore.CommitSwap(logger, claimA.uid, claimB.uid))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claimA.uid))
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claimB.uid))
	h.driver.applyMu.Unlock()

	delete(h.activeTransitClaims, claimA.uid)
	delete(h.activeTransitClaims, claimB.uid)
	h.inTransit = len(h.activeTransitClaims) > 0

	h.AssertInvariants()
}

func (h *oracleHarness) AbortSwap(claimA, claimB *oracleClaim) {
	h.t.Helper()
	logger := testr.New(h.t)
	require.NoError(h.t, h.driver.cpuAllocationStore.AbortSwap(logger, claimA.uid, claimB.uid))

	h.driver.applyMu.Lock()
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claimA.uid))
	require.NoError(h.t, h.driver.writeClaimPlacement(logger, claimB.uid))
	h.driver.applyMu.Unlock()

	delete(h.activeTransitClaims, claimA.uid)
	delete(h.activeTransitClaims, claimB.uid)
	h.inTransit = len(h.activeTransitClaims) > 0

	h.AssertInvariants()
}

func (h *oracleHarness) Unprepare(claim *oracleClaim) {
	h.t.Helper()
	results, err := h.driver.UnprepareResourceClaims(context.Background(), []kubeletplugin.NamespacedObject{
		{UID: claim.uid},
	})
	require.NoError(h.t, err)
	require.NoError(h.t, results[claim.uid])

	claim.isPrepared = false
	claim.isUnprepared = true
	claim.currentCPUs = cpuset.New()

	for _, ctrUID := range claim.allContainers() {
		h.syncContainerLiveCPUs(ctrUID)
	}

	h.AssertInvariants()
}

func (h *oracleHarness) Deallocate(claim *oracleClaim) {
	h.t.Helper()
	claim.isDeallocated = true
	h.claimReader.mu.Lock()
	h.claimReader.deallocated[claim.uid] = true
	for i, pc := range h.claimReader.projected.Claims {
		if pc.UID == string(claim.uid) {
			h.claimReader.projected.Claims[i].State = v1alpha1.ClaimStateDeallocated
			break
		}
	}
	h.claimReader.mu.Unlock()

	h.AssertInvariants()
}

func (h *oracleHarness) Delete(claim *oracleClaim) {
	h.t.Helper()
	claim.isDeleted = true
	h.claimReader.mu.Lock()
	newProjected := make([]v1alpha1.ProjectedClaim, 0, len(h.claimReader.projected.Claims))
	for _, pc := range h.claimReader.projected.Claims {
		if pc.UID != string(claim.uid) {
			newProjected = append(newProjected, pc)
		}
	}
	h.claimReader.projected.Claims = newProjected
	newClaims := make([]*resourceapi.ResourceClaim, 0, len(h.claimReader.claims))
	for _, rc := range h.claimReader.claims {
		if rc.UID != claim.uid {
			newClaims = append(newClaims, rc)
		}
	}
	h.claimReader.claims = newClaims
	h.claimReader.mu.Unlock()

	h.AssertInvariants()
}

func (h *oracleHarness) CrashAndRestart() {
	h.t.Helper()
	h.driver.applyMu.Lock()
	oldD := h.driver
	h.driver.applyMu.Unlock()
	oldD.defragRetries.ShutDown()

	topo := h.baseTopology.cpuTopology
	allCPUs := h.baseTopology.onlineCPUs

	newD := &CPUDriver{
		nodeName:         oldD.nodeName,
		driverName:       oldD.driverName,
		cdiMgr:           h.mockCDI,
		containerUpdater: h.fakeUpdater,
		claimReader:      h.claimReader,
		metrics:          oldD.metrics,
		cgroupfs:         h.cgroups,
		poisonedNodes:    make(map[int]*poisonedNode),
		pendingRounds:    make(map[defragScope]*defragRound),
		defragRetries:    workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[defragScope]()),
		defragRetryDue:   make(chan defragScope),
		makeRoomTargets:  make(map[types.UID]*makeRoomTarget),
		reconcileTrigger: make(chan struct{}, 1),
		sysfs: fstest.MapFS{
			"devices/system/cpu/online": &fstest.MapFile{Data: []byte(allCPUs.String() + "\n")},
		},
		defrag:                  defragOptions{enabled: true, allowTransientOverlap: true, batchTimeout: defaultDefragBatchTimeout},
		cpuDeviceGroupBy:        devattr.GROUP_BY_UNCORE_CACHE,
		cpuDeviceMode:           devattr.CPU_DEVICE_MODE_GROUPED,
		devicesPerResourceSlice: 64,
		podConfigStore:          store.NewPodConfig(),
		claimTracker:            store.NewClaimTracker(),
		cpuAllocationStore:      store.NewCPUAllocation(topo, cpuset.New()),
		topology:                h.baseTopology,
		partitions:              h.partitions,
		defaultPartitionCPUs:    h.defaultPartitionCPUs,
	}
	h.t.Cleanup(newD.defragRetries.ShutDown)

	var pods []*api.PodSandbox
	var containers []*api.Container
	for _, claim := range h.claims {
		if !claim.isPrepared {
			continue
		}
		pod := &api.PodSandbox{
			Id:        string(claim.podUID),
			Uid:       string(claim.podUID),
			Name:      claim.name,
			Namespace: claim.namespace,
		}
		pods = append(pods, pod)
		envVar := fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, claim.uid)
		ctr := &api.Container{
			Id:           string(claim.containerUID),
			PodSandboxId: string(claim.podUID),
			Name:         claim.containerName,
			Env:          []string{envVar},
			Linux: &api.LinuxContainer{
				CgroupsPath: cgroupOf(claim.containerUID),
				Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: claim.currentCPUs.String()}},
			},
		}
		containers = append(containers, ctr)
	}

	_, err := newD.Synchronize(context.Background(), pods, containers)
	require.NoError(h.t, err)

	h.driver = newD
	h.activeTransitClaims = make(map[types.UID]struct{})
	h.inTransit = false

	for _, claim := range h.claims {
		if claim.isPrepared {
			current, ok := h.driver.cpuAllocationStore.GetResourceClaimAllocation(claim.uid)
			if ok {
				claim.currentCPUs = current
			}
			for _, ctrUID := range claim.allContainers() {
				h.syncContainerLiveCPUs(ctrUID)
			}
		}
	}

	h.AssertInvariants()
}

func (h *oracleHarness) AssertInvariants() {
	h.t.Helper()
	h.driver.applyMu.Lock()
	defer h.driver.applyMu.Unlock()

	devices := h.driver.mirroredDevices()
	mirror := h.driver.capacityMirror()
	holdings := h.driver.cpuAllocationStore.ClaimHoldings()

	for devName, devCPUs := range devices {
		physicalFree := devCPUs.Size()
		for _, holding := range holdings {
			physicalFree -= devCPUs.Intersection(holding.Held).Size()
		}

		inFlightAllocated := 0
		inFlightDeallocated := 0
		liveConsumed := 0
		for _, claim := range h.claims {
			if claim.isAllocated && !claim.isDeallocated && !claim.isDeleted {
				if claim.chargedCache == devName {
					liveConsumed += claim.size
					if !claim.isPrepared {
						inFlightAllocated += claim.size
					}
				}
			}
			if claim.isPrepared && !claim.isUnprepared && (claim.isDeallocated || claim.isDeleted) {
				if claim.chargedCache == devName && claim.currentCPUs.Intersection(devCPUs).Size() > 0 {
					inFlightDeallocated += claim.currentCPUs.Intersection(devCPUs).Size()
				}
			}
		}

		terms := mirror[devName]
		computedValue := devCPUs.Size() + terms.correction()
		expectedFree := computedValue - liveConsumed
		effectiveFree := physicalFree + inFlightDeallocated - inFlightAllocated

		require.Equal(h.t, effectiveFree, expectedFree,
			"mirror identity violated on cache %q: effectiveFree=%d, expectedFree=%d (physicalFree=%d, inFlight=%d, inFlightDeallocated=%d, size=%d, computed=%d, consumed=%d, departed=%d, squatters=%d)",
			devName, effectiveFree, expectedFree, physicalFree, inFlightAllocated, inFlightDeallocated, devCPUs.Size(), computedValue, liveConsumed, terms.departed, terms.squatters)

		cID, ok := h.deviceNameToCacheID[devName]
		require.True(h.t, ok)
		require.Equal(h.t, h.cacheIDToNUMA[cID], h.driver.topology.deviceNameToNUMANodeID[devName])
		require.Equal(h.t, h.cacheIDToPartition[cID], h.driver.topology.deviceNameToPartition[devName])
	}

	if !h.inTransit {
		seenCPUs := cpuset.New()
		for _, claim := range h.claims {
			if claim.isPrepared {
				require.False(h.t, claim.currentCPUs.IsEmpty(), "prepared claim %s has empty current cpuset", claim.uid)
				overlap := seenCPUs.Intersection(claim.currentCPUs)
				require.True(h.t, overlap.IsEmpty(), "overlap on exclusive CPUs %s between claim %s and other claims", overlap.String(), claim.uid)
				seenCPUs = seenCPUs.Union(claim.currentCPUs)

				cID := h.deviceNameToCacheID[claim.chargedCache]
				expectedNUMA := h.cacheIDToNUMA[cID]
				expectedPartition := h.cacheIDToPartition[cID]
				for _, cpu := range claim.currentCPUs.List() {
					info, ok := h.driver.topology.cpuTopology.CPUDetails[cpu]
					require.True(h.t, ok)
					require.Equal(h.t, expectedNUMA, info.NUMANodeID, "claim %s CPUs outside NUMA node %d", claim.uid, expectedNUMA)
					partName := h.driver.topology.deviceNameToPartition[claim.chargedCache]
					require.Equal(h.t, expectedPartition, partName, "claim %s CPUs outside partition %s", claim.uid, expectedPartition)
				}
			}
		}
		preparedInStore := h.driver.cpuAllocationStore.GetPreparedCPUs()
		require.True(h.t, preparedInStore.Equals(seenCPUs),
			"store prepared CPUs %s != union of prepared claims %s", preparedInStore.String(), seenCPUs.String())
	}

	for _, claim := range h.claims {
		if !claim.relocatable && claim.isPrepared {
			require.False(h.t, claim.initialCPUs.IsEmpty(), "immobile claim %s has empty initial CPUs", claim.uid)
			current, ok := h.driver.cpuAllocationStore.GetResourceClaimAllocation(claim.uid)
			require.True(h.t, ok, "immobile claim %s missing from store", claim.uid)
			require.True(h.t, current.Equals(claim.initialCPUs),
				"immobile claim %s cpuset shifted: initial=%s, current=%s", claim.uid, claim.initialCPUs.String(), current.String())
		}
	}

	h.assertThreeWayInvariant()
}

func (h *oracleHarness) assertThreeWayInvariant() {
	h.t.Helper()
	if h.inTransit {
		return
	}

	containerClaims := make(map[types.UID]cpuset.CPUSet)
	for _, claim := range h.claims {
		if claim.isPrepared {
			record, ok := h.driver.cpuAllocationStore.GetClaimRecord(claim.uid)
			require.True(h.t, ok, "prepared claim %s must have record in store", claim.uid)
			claimCPUs, ok := h.driver.cpuAllocationStore.GetResourceClaimAllocation(claim.uid)
			require.True(h.t, ok, "prepared claim %s must have allocation in store", claim.uid)
			require.False(h.t, claimCPUs.IsEmpty(), "prepared claim %s must have non-empty CPUs in store", claim.uid)
			require.True(h.t, claimCPUs.Equals(claim.currentCPUs), "store CPUs %s != claim current CPUs %s", claimCPUs.String(), claim.currentCPUs.String())
			require.True(h.t, claimCPUs.Equals(store.UnionOf(record.Requests)), "store allocation %s != record requests %s", claimCPUs.String(), store.UnionOf(record.Requests).String())

			devName := getCDIDeviceName(claim.uid)
			cdiRecord, err := h.mockCDI.GetDeviceAllocations(devName)
			require.NoError(h.t, err, "CDI device %s must exist", devName)
			require.True(h.t, store.UnionOf(cdiRecord.Requests).Equals(claimCPUs), "CDI allocated CPUs %s != store CPUs %s", store.UnionOf(cdiRecord.Requests).String(), claimCPUs.String())

			for _, ctrUID := range claim.allContainers() {
				containerClaims[ctrUID] = containerClaims[ctrUID].Union(claimCPUs)
			}
		}
	}

	if !h.inTransit {
		for ctrUID, expectedCPUs := range containerClaims {
			liveCPUs := getContainerLiveCPUs(h.cgroups, ctrUID)
			require.True(h.t, liveCPUs.Equals(expectedCPUs),
				"kernel cgroup cpuset %s for container %s does not match expected claim union %s",
				liveCPUs.String(), ctrUID, expectedCPUs.String())
		}
	}
}

func TestInvariantOracle_LifecyclePermutations(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     2,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	c1 := h.createClaim("c1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c1)
	h.Prepare(c1)
	targetCPUs := cpuset.New(2, 3)
	h.BeginMove(c1, targetCPUs)
	h.CommitMove(c1, targetCPUs)
	h.Unprepare(c1)
	h.Deallocate(c1)
	h.Delete(c1)

	c2 := h.createClaim("c2", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c2)
	h.Prepare(c2)
	h.Deallocate(c2)
	h.Unprepare(c2)
	h.Delete(c2)

	c3 := h.createClaim("c3", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c3)
	h.Prepare(c3)
	h.Delete(c3)
	h.Unprepare(c3)

	c4 := h.createClaim("c4", 4, 1, true, v1alpha1.AlignmentBestEffort, false)
	h.Allocate(c4)
	h.Deallocate(c4)
	h.Delete(c4)

	c5 := h.createClaim("c5", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c6 := h.createClaim("c6", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c5)
	h.Allocate(c6)
	h.Prepare(c5)
	h.Prepare(c6)
	h.BeginSwap(c5, c6)
	h.CommitSwap(c5, c6)
	h.Unprepare(c5)
	h.Unprepare(c6)
	h.Deallocate(c5)
	h.Deallocate(c6)
	h.Delete(c5)
	h.Delete(c6)

	c7 := h.createClaim("c7", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c8 := h.createClaim("c8", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c7)
	h.Allocate(c8)
	h.Prepare(c7)
	h.Prepare(c8)
	h.BeginSwap(c7, c8)
	h.Unprepare(c7)
	h.AbortSwap(c8, c8)
	h.Unprepare(c8)
	h.Deallocate(c7)
	h.Deallocate(c8)
	h.Delete(c7)
	h.Delete(c8)

	c9 := h.createClaim("c9", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c9)
	h.Prepare(c9)
	h.BeginMove(c9, cpuset.New(2, 3))
	h.Unprepare(c9)
	h.Deallocate(c9)
	h.Delete(c9)

	cImm := h.createClaim("cImm", 2, 0, false, v1alpha1.AlignmentBestEffort, false)
	cRel := h.createClaim("cRel", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cImm)
	h.Allocate(cRel)
	h.Prepare(cImm)
	h.Prepare(cRel)
	h.BeginMove(cRel, cpuset.New(6, 7))
	h.CommitMove(cRel, cpuset.New(6, 7))
	h.Unprepare(cRel)
	h.Unprepare(cImm)
	h.Deallocate(cRel)
	h.Deallocate(cImm)
	h.Delete(cRel)
	h.Delete(cImm)
}

func TestInvariantOracle_CrashPointsAtPersistenceBoundaries(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     1,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	cA := h.createClaim("cA", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cA)
	h.Prepare(cA)
	originA := cA.currentCPUs
	targetA := cpuset.New(2, 3)
	require.NoError(t, h.driver.cpuAllocationStore.BeginRebind(testr.New(t), cA.uid, targetA))
	h.CrashAndRestart()
	require.Equal(t, originA, cA.currentCPUs)
	h.Unprepare(cA)
	h.Deallocate(cA)
	h.Delete(cA)

	cB1 := h.createClaim("cB1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	cB2 := h.createClaim("cB2", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cB1)
	h.Allocate(cB2)
	h.Prepare(cB1)
	h.Prepare(cB2)
	originB1 := cB1.currentCPUs
	originB2 := cB2.currentCPUs
	h.driver.applyMu.Lock()
	require.NoError(t, h.driver.writeClaimPlacementWithRound(testr.New(t), cB1.uid, &store.RoundProvenance{
		RoundID:  "partial-round-1",
		Origin:   originB1,
		Target:   originB2,
		Partners: []types.UID{cB2.uid},
	}))
	h.driver.applyMu.Unlock()
	h.CrashAndRestart()
	require.Equal(t, originB1, cB1.currentCPUs)
	require.Equal(t, originB2, cB2.currentCPUs)
	h.Unprepare(cB1)
	h.Unprepare(cB2)
	h.Deallocate(cB1)
	h.Deallocate(cB2)
	h.Delete(cB1)
	h.Delete(cB2)

	cC1 := h.createClaim("cC1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	cC2 := h.createClaim("cC2", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cC1)
	h.Allocate(cC2)
	h.Prepare(cC1)
	h.Prepare(cC2)
	targetC1 := cC2.currentCPUs
	targetC2 := cC1.currentCPUs
	h.BeginSwap(cC1, cC2)
	h.CrashAndRestart()
	require.Equal(t, targetC1, cC1.currentCPUs)
	require.Equal(t, targetC2, cC2.currentCPUs)
	h.Unprepare(cC1)
	h.Unprepare(cC2)
	h.Deallocate(cC1)
	h.Deallocate(cC2)
	h.Delete(cC1)
	h.Delete(cC2)

	cD := h.createClaim("cD", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cD)
	h.Prepare(cD)
	targetD := cpuset.New(2, 3)
	h.BeginMove(cD, targetD)
	setContainerLiveCPUs(h.cgroups, cD.containerUID, targetD)
	cD.currentCPUs = targetD
	h.CrashAndRestart()
	require.Equal(t, targetD, cD.currentCPUs)
	h.Unprepare(cD)
	h.Deallocate(cD)
	h.Delete(cD)
}

func TestInvariantOracle_RuntimeFailureMatrix(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     1,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	c1 := h.createClaim("c1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c2 := h.createClaim("c2", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c1)
	h.Allocate(c2)
	h.Prepare(c1)
	h.Prepare(c2)
	h.BeginSwap(c1, c2)
	h.fakeUpdater.reply = refusing(string(c1.containerUID))
	round := &defragRound{
		id:       "fail-round-1",
		scope:    defaultScope(0),
		store:    h.driver.cpuAllocationStore,
		outcomes: map[int]exchangeOutcome{1: exchangeUndone},
	}
	step := []defrag.Move{
		{ClaimUID: c1.uid, From: c1.currentCPUs, To: c2.currentCPUs, Exchange: 1},
		{ClaimUID: c2.uid, From: c2.currentCPUs, To: c1.currentCPUs, Exchange: 1},
	}
	h.driver.applyMu.Lock()
	settled := h.driver.settleExchangeStep(testr.New(t), round, step)
	h.driver.applyMu.Unlock()
	require.True(t, settled)
	delete(h.activeTransitClaims, c1.uid)
	delete(h.activeTransitClaims, c2.uid)
	h.inTransit = false
	h.AssertInvariants()
	h.Unprepare(c1)
	h.Unprepare(c2)
	h.Deallocate(c1)
	h.Deallocate(c2)
	h.Delete(c1)
	h.Delete(c2)

	c3 := h.createClaim("c3", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c4 := h.createClaim("c4", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c3)
	h.Allocate(c4)
	h.Prepare(c3)
	h.Prepare(c4)
	h.BeginSwap(c3, c4)
	h.fakeUpdater.reply = refusing(string(c4.containerUID))
	round2 := &defragRound{
		id:       "fail-round-2",
		scope:    defaultScope(0),
		store:    h.driver.cpuAllocationStore,
		outcomes: map[int]exchangeOutcome{1: exchangeUndone},
	}
	step2 := []defrag.Move{
		{ClaimUID: c3.uid, From: c3.currentCPUs, To: c4.currentCPUs, Exchange: 1},
		{ClaimUID: c4.uid, From: c4.currentCPUs, To: c3.currentCPUs, Exchange: 1},
	}
	h.driver.applyMu.Lock()
	settled2 := h.driver.settleExchangeStep(testr.New(t), round2, step2)
	h.driver.applyMu.Unlock()
	require.True(t, settled2)
	delete(h.activeTransitClaims, c3.uid)
	delete(h.activeTransitClaims, c4.uid)
	h.inTransit = false
	h.AssertInvariants()
	h.Unprepare(c3)
	h.Unprepare(c4)
	h.Deallocate(c3)
	h.Deallocate(c4)
	h.Delete(c3)
	h.Delete(c4)

	c5 := h.createClaim("c5", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c5)
	h.Prepare(c5)
	h.BeginMove(c5, cpuset.New(2, 3))
	h.driver.applyMu.Lock()
	h.driver.abortMoves(testr.New(t), []defrag.Move{{ClaimUID: c5.uid, From: c5.currentCPUs, To: cpuset.New(2, 3)}})
	h.driver.applyMu.Unlock()
	delete(h.activeTransitClaims, c5.uid)
	h.inTransit = false
	h.AssertInvariants()
	h.Unprepare(c5)
	h.Deallocate(c5)
	h.Delete(c5)

	cP1 := h.createClaim("cP1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	cP2 := h.createClaim("cP2", 2, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cP1)
	h.Allocate(cP2)
	h.Prepare(cP1)
	h.Prepare(cP2)
	pod1 := &api.PodSandbox{Id: "p1", Uid: string(cP1.podUID), Name: cP1.name, Namespace: cP1.namespace}
	pod2 := &api.PodSandbox{Id: "p2", Uid: string(cP2.podUID), Name: cP2.name, Namespace: cP2.namespace}
	ctr1 := &api.Container{
		Id:           string(cP1.containerUID),
		PodSandboxId: "p1",
		Name:         cP1.containerName,
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, cP1.uid)},
		Linux: &api.LinuxContainer{
			CgroupsPath: cgroupOf(cP1.containerUID),
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: cP1.currentCPUs.String()}},
		},
	}
	ctr2 := &api.Container{
		Id:           string(cP2.containerUID),
		PodSandboxId: "p2",
		Name:         cP2.containerName,
		Env:          []string{fmt.Sprintf("%s_%s=dynamic", cdiEnvVarPrefix, cP2.uid)},
		Linux: &api.LinuxContainer{
			CgroupsPath: cgroupOf(cP2.containerUID),
			Resources:   &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: cP2.currentCPUs.String()}},
		},
	}
	_, err := h.driver.Synchronize(context.Background(), []*api.PodSandbox{pod2, pod1}, []*api.Container{ctr2, ctr1})
	require.NoError(t, err)
	h.AssertInvariants()
	_, err = h.driver.Synchronize(context.Background(), []*api.PodSandbox{pod1, pod2}, []*api.Container{ctr1, ctr2})
	require.NoError(t, err)
	h.AssertInvariants()
	h.Unprepare(cP1)
	h.Unprepare(cP2)
	h.Deallocate(cP1)
	h.Deallocate(cP2)
	h.Delete(cP1)
	h.Delete(cP2)
}

func TestInvariantOracle_UnequalCacheSizesAgainstUncanonicalisedOracle(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     1,
		cachesPerNUMA: 3,
		cpusPerCache:  []int{4, 8, 12},
	})

	topo := h.driver.topology.cpuTopology
	dTopo, err := defrag.NewTopology(topo, 0, h.driver.topology.onlineCPUs)
	require.NoError(t, err)

	claimA := defrag.Placement{ClaimUID: "claim-A", CPUs: cpuset.New(0, 1, 4, 5)}
	claimB := defrag.Placement{ClaimUID: "claim-B", CPUs: cpuset.New(2, 3)}
	claimC := defrag.Placement{ClaimUID: "claim-C", CPUs: cpuset.New(6, 7, 8, 9)}
	placements := []defrag.Placement{claimA, claimB, claimC}
	free := cpuset.New(10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)

	sel := func(available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
		if available.Size() < numCPUs {
			return cpuset.New(), fmt.Errorf("insufficient cpus")
		}
		return cpuset.New(available.List()[:numCPUs]...), nil
	}

	goalMakeWhole := defrag.GoalMakeClaimWhole{ClaimUID: "claim-A"}
	planCanonical1, err1 := defrag.ExactSearch(dTopo, placements, free, cpuset.New(), goalMakeWhole, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	planOracle1, err2 := defrag.ExactSearchUncanonicalised(dTopo, placements, free, cpuset.New(), goalMakeWhole, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, planCanonical1.Status, planOracle1.Status)
	require.Equal(t, len(planCanonical1.Moves), len(planOracle1.Moves))

	goalFree := defrag.GoalFreeCache{CacheID: 0}
	planCanonical2, err1 := defrag.ExactSearch(dTopo, placements, free, cpuset.New(), goalFree, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	planOracle2, err2 := defrag.ExactSearchUncanonicalised(dTopo, placements, free, cpuset.New(), goalFree, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, planCanonical2.Status, planOracle2.Status)
	require.Equal(t, len(planCanonical2.Moves), len(planOracle2.Moves))

	goalK := defrag.GoalKCacheHome{K: 1}
	planCanonical3, err1 := defrag.ExactSearch(dTopo, placements, free, cpuset.New(), goalK, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	planOracle3, err2 := defrag.ExactSearchUncanonicalised(dTopo, placements, free, cpuset.New(), goalK, sel, defrag.ExactOptions{
		AllowSwaps: true,
		MaxDepth:   3,
	})
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.Equal(t, planCanonical3.Status, planOracle3.Status)
	require.Equal(t, len(planCanonical3.Moves), len(planOracle3.Moves))

	cU1 := h.createClaim("cU1", 4, 0, true, v1alpha1.AlignmentBestEffort, true)
	cU2 := h.createClaim("cU2", 6, 1, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(cU1)
	h.Allocate(cU2)
	h.Prepare(cU1)
	h.Prepare(cU2)
	h.Unprepare(cU1)
	h.Unprepare(cU2)
	h.Deallocate(cU1)
	h.Deallocate(cU2)
	h.Delete(cU1)
	h.Delete(cU2)
}

func TestInvariantOracle_ReplacementPodPrepareBeforeEvictedUnprepare(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     1,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	claimA := h.createClaim("claim-A", 4, 0, true, v1alpha1.AlignmentBestEffort, false)
	h.Allocate(claimA)
	h.Prepare(claimA)
	require.Equal(t, cpuset.New(0, 1, 2, 3), claimA.currentCPUs)

	claimB := h.createClaim("claim-B", 4, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(claimB)
	h.Prepare(claimB)

	require.Equal(t, cpuset.New(4, 5, 6, 7), claimB.currentCPUs)
	h.AssertInvariants()

	h.Unprepare(claimA)
	h.Deallocate(claimA)
	h.Delete(claimA)
	h.AssertInvariants()

	h.driver.applyMu.Lock()
	mirror := h.driver.capacityMirror()
	require.Equal(t, 4, mirror[cacheDevice(0)].departed)
	require.Equal(t, 4, mirror[cacheDevice(1)].squatters)
	h.driver.applyMu.Unlock()

	targetB := cpuset.New(0, 1, 2, 3)
	h.BeginMove(claimB, targetB)
	h.CommitMove(claimB, targetB)
	require.Equal(t, targetB, claimB.currentCPUs)

	h.driver.applyMu.Lock()
	mirrorAfter := h.driver.capacityMirror()
	require.Equal(t, 0, mirrorAfter[cacheDevice(0)].departed)
	require.Equal(t, 0, mirrorAfter[cacheDevice(1)].squatters)
	h.driver.applyMu.Unlock()

	h.Unprepare(claimB)
	h.Deallocate(claimB)
	h.Delete(claimB)

	h.driver.applyMu.Lock()
	finalMirror := h.driver.capacityMirror()
	for _, dev := range h.driver.mirroredDevices() {
		for devName := range h.driver.topology.deviceNameToCPUs {
			if dev.Equals(h.driver.topology.deviceNameToCPUs[devName]) {
				require.Equal(t, 0, finalMirror[devName].squatters, "leaked squatter on cache %s", devName)
			}
		}
	}
	h.driver.applyMu.Unlock()
}

func TestInvariantOracle_JointClaimLifecycle(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     2,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	podUID := types.UID("joint-pod")
	ctrUID := types.UID("joint-ctr")
	ctrName := "joint-container"

	c1 := h.createJointClaim("claim-joint-1", podUID, ctrUID, ctrName, 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c2 := h.createJointClaim("claim-joint-2", podUID, ctrUID, ctrName, 2, 0, true, v1alpha1.AlignmentBestEffort, true)

	h.Allocate(c1)
	h.Allocate(c2)

	h.Prepare(c1)
	require.Equal(t, c1.currentCPUs, getContainerLiveCPUs(h.cgroups, ctrUID))

	h.Prepare(c2)
	expectedUnion := c1.currentCPUs.Union(c2.currentCPUs)
	require.Equal(t, 4, expectedUnion.Size())
	require.Equal(t, expectedUnion, getContainerLiveCPUs(h.cgroups, ctrUID))

	target := cpuset.New(4, 5)
	h.BeginMove(c1, target)
	h.CommitMove(c1, target)
	expectedAfterMove := c1.currentCPUs.Union(c2.currentCPUs)
	require.Equal(t, expectedAfterMove, getContainerLiveCPUs(h.cgroups, ctrUID))

	h.Unprepare(c1)
	require.Equal(t, c2.currentCPUs, getContainerLiveCPUs(h.cgroups, ctrUID))

	h.Unprepare(c2)
	require.True(t, getContainerLiveCPUs(h.cgroups, ctrUID).IsEmpty())

	h.Deallocate(c1)
	h.Deallocate(c2)
	h.Delete(c1)
	h.Delete(c2)
}

func TestInvariantOracle_SharedClaimLifecycle(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     2,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	podUID := types.UID("shared-claim-pod")
	ctr1UID := types.UID("shared-ctr-1")
	ctr2UID := types.UID("shared-ctr-2")

	c1 := h.createJointClaim("claim-shared-1", podUID, ctr1UID, "ctr-1", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	c1.sharedContainerUIDs = append(c1.sharedContainerUIDs, ctr2UID)

	h.Allocate(c1)
	h.Prepare(c1)

	h.driver.applyMu.Lock()
	h.driver.podConfigStore.SetContainerState(podUID,
		store.NewContainerState("ctr-2", ctr2UID, c1.uid).WithCgroup(cgroupOf(ctr2UID)))
	h.driver.applyMu.Unlock()
	h.syncContainerLiveCPUs(ctr2UID)

	h.AssertInvariants()
	require.Equal(t, c1.currentCPUs, getContainerLiveCPUs(h.cgroups, ctr1UID))
	require.Equal(t, c1.currentCPUs, getContainerLiveCPUs(h.cgroups, ctr2UID))

	target := cpuset.New(4, 5)
	h.BeginMove(c1, target)
	h.CommitMove(c1, target)

	require.Equal(t, target, getContainerLiveCPUs(h.cgroups, ctr1UID))
	require.Equal(t, target, getContainerLiveCPUs(h.cgroups, ctr2UID))

	h.Unprepare(c1)
	h.AssertInvariants()
	require.True(t, getContainerLiveCPUs(h.cgroups, ctr1UID).IsEmpty())
	require.True(t, getContainerLiveCPUs(h.cgroups, ctr2UID).IsEmpty())

	h.Deallocate(c1)
	h.Delete(c1)
}

func TestInvariantOracle_RequeueLifecycle(t *testing.T) {
	h := newOracleHarness(t, oracleTopologyConfig{
		numaNodes:     2,
		cachesPerNUMA: 2,
		cpusPerCache:  []int{4},
	})

	c1 := h.createClaim("claim-requeue", 2, 0, true, v1alpha1.AlignmentBestEffort, true)
	h.Allocate(c1)
	h.Prepare(c1)

	h.driver.applyMu.Lock()
	holdings := h.driver.cpuAllocationStore.ClaimHoldings()
	require.Equal(t, 2, holdings[c1.uid].Recorded[cacheDevice(0)])
	h.driver.applyMu.Unlock()

	h.Unprepare(c1)
	h.Deallocate(c1)
	h.AssertInvariants()

	c1.chargedCache = cacheDevice(1)
	h.Allocate(c1)
	h.Prepare(c1)

	h.driver.applyMu.Lock()
	holdingsAfter := h.driver.cpuAllocationStore.ClaimHoldings()
	require.Equal(t, 2, holdingsAfter[c1.uid].Recorded[cacheDevice(1)])
	require.Equal(t, 0, holdingsAfter[c1.uid].Recorded[cacheDevice(0)])
	h.driver.applyMu.Unlock()

	require.Equal(t, 2, c1.currentCPUs.Size())
	require.True(t, c1.currentCPUs.IsSubsetOf(h.cacheIDToCPUs[1]))

	h.Unprepare(c1)
	h.Deallocate(c1)
	h.Delete(c1)
	h.AssertInvariants()
}
