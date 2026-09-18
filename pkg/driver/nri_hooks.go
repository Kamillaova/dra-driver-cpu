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
	"slices"
	"strings"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
)

// Synchronize is called by the NRI to synchronize the state of the driver during bootstrap.
func (cp *CPUDriver) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) (rupdates []*api.ContainerUpdate, rerr error) {
	startTime := time.Now()
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))
	// this happens once at startup and it's critical enough that we always want to see it.
	logger.Info("begin: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))
	defer logger.Info("end: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))

	defer func() { cp.metrics.RecordNRISynchronize(rerr, time.Since(startTime)) }()
	// Synchronize rebuilds all three stores and swaps them in, so nothing that
	// reads or writes a placement may run while it does.
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	cpuAllocationStore := store.NewCPUAllocation(cp.topology.cpuTopology, cp.topology.reservedCPUs)
	cpuAllocationStore.SetClaimlessCPUs(cp.defaultPartitionCPUs)
	podConfigStore := store.NewPodConfig()
	claimTracker := store.NewClaimTracker()
	var containerUpdates []*api.ContainerUpdate
	var claimsToClearRound []types.UID
	var observed []observedContainer
	cdiCacheRefreshAttempted := false

	for _, pod := range pods {
		pLogger := logger.WithValues("pod", ctxlog.KObj(pod), "podUID", pod.Uid)
		pLogger.V(2).Info("synchronize pod")
		for _, container := range containers {
			if container.PodSandboxId != pod.Id {
				continue
			}
			cLogger := pLogger.WithValues("container", container.Name)

			entries, err := parseDRAEnv(cLogger, container.Env)
			if err != nil {
				cLogger.Error(err, "ignoring container with malformed DRA env during synchronize")
				continue
			}
			containerUID := types.UID(container.GetId())
			var claimUIDs []types.UID
			reportedCDIDevices := runtimeCDIDevices(container)
			for _, entry := range entries {
				uid := entry.claimUID
				caLogger := cLogger.WithValues("claimUID", uid)
				if !claimInjectedByRuntime(reportedCDIDevices, uid) {
					caLogger.Info("ignoring claim the runtime injected no CDI device for during synchronize")
					continue
				}

				if !cdiCacheRefreshAttempted {
					err = cp.cdiMgr.Refresh()
					cdiCacheRefreshAttempted = true
					if err != nil {
						logger.Error(err, "failed to refresh CDI cache, continuing with available CDI devices")
					}
					cp.reconcileActiveRounds(logger)
				}

				deviceName := getCDIDeviceName(uid)
				// CCX-FORK: upstream instead requires the container's env to
				// equal the CDI spec's, and drops the claim when it does not.
				//
				// The CDI spec is the driver's own record of where the claim
				// belongs, and it is rewritten whenever placement changes. The
				// container's env is only where it belonged when the container
				// started, so it cannot decide anything here.
				recorded, err := cp.cdiMgr.GetDeviceAllocations(deviceName)
				if err != nil {
					caLogger.Error(err, "ignoring claim not prepared by this driver during synchronize")
					continue
				}
				desired := store.UnionOf(recorded.Requests)
				if !entry.dynamic && !desired.Equals(entry.cpus) {
					// Expected whenever the claim was moved after its container
					// started. The ContainerUpdate below carries the container to
					// the desired set, so log and converge rather than dropping
					// the claim and leaking its CPUs into the shared pool.
					caLogger.V(2).Info("container was created for a cpuset the claim has since left, converging",
						"createdWithCPUs", entry.cpus.String(), "desiredCPUs", desired.String())
				}
				// An overlapping claim rebuilt earlier in this call must not fail
				// the whole synchronize; skip it instead of leaving every other pod
				// and container on the node without a driver.
				if err := cpuAllocationStore.ReserveResourceClaimAllocation(caLogger, uid, recorded, false); err != nil {
					caLogger.Error(err, "skipping claim with an allocation inconsistent with an earlier one during synchronize")
					cp.metrics.RecordSynchronizeSkippedClaim()
					continue
				}
				cp.checkClaimPartition(caLogger, desired)
				// CCX-FORK: the claim's own reservation, recorded at Prepare and
				// read back from the spec the driver wrote. A pod spec can name
				// any claim UID in a DRA_CPUSET_* variable, so the container is
				// not a source for who may hold the claim; CreateContainer's
				// CRI-O fallback checks this, and it must not be forgeable.
				//
				// A spec written before the driver recorded it leaves the
				// reservation unknown, which that fallback refuses. The
				// projection cannot stand in: it carries no reservation.
				if len(recorded.ReservedFor) > 0 {
					claimTracker.SetReservedFor(uid, recorded.ReservedFor)
				}
				claimUIDs = append(claimUIDs, uid)
			}

			// CCX-FORK: upstream binds every claim the container names.
			if owned := exclusiveClaimUIDs(cpuAllocationStore, claimUIDs); len(owned) > 0 {
				if _, err := claimTracker.SetOwner(cLogger, types.UID(pod.Uid), container.Name, owned...); err != nil {
					// An inconsistency in the runtime's own reported state, not a
					// reason to fail every other pod and container being
					// synchronized: treat this container as unclaimed instead.
					cLogger.Error(err, "treating container as unclaimed: its claim ownership conflicts with an earlier one during synchronize")
					cp.metrics.RecordSynchronizeSkippedClaim()
					claimUIDs = nil
				}
			}
			var state *store.ContainerState
			if len(claimUIDs) == 0 {
				state = store.NewContainerState(container.GetName(), containerUID).WithCgroup(container.GetLinux().GetCgroupsPath())
			} else {
				allGuaranteedCPUs, err := cpuAllocationStore.GetResourceClaimAllocationUnion(claimUIDs...)
				if err != nil {
					return nil, err
				}
				cLogger.V(2).Info("found guaranteed CPUs", "cpus", allGuaranteedCPUs.String())
				cgroupsPath := ""
				if linux := container.GetLinux(); linux != nil {
					cgroupsPath = linux.GetCgroupsPath()
				}
				state = store.NewContainerState(container.GetName(), containerUID, claimUIDs...).WithCgroup(cgroupsPath)

				originUnion := cpuset.New()
				targetUnion := cpuset.New()
				hadRound := false
				for _, uid := range claimUIDs {
					rec, err := cp.cdiMgr.GetDeviceAllocations(getCDIDeviceName(uid))
					if err == nil && rec.Round != nil && rec.Round.RoundID != "" {
						hadRound = true
						// A round carries only the claim's exclusive CPUs; a pool share
						// stays where it is, and the container runs on both.
						pool := nonExclusiveCPUs(rec)
						originUnion = originUnion.Union(rec.Round.Origin).Union(pool)
						targetUnion = targetUnion.Union(rec.Round.Target).Union(pool)
					} else if claimCPUs, ok := cpuAllocationStore.GetResourceClaimAllocation(uid); ok {
						originUnion = originUnion.Union(claimCPUs)
						targetUnion = targetUnion.Union(claimCPUs)
					}
				}
				if !hadRound {
					originUnion = allGuaranteedCPUs
					targetUnion = allGuaranteedCPUs
				}

				var committedCPUs cpuset.CPUSet
				if linux := container.GetLinux(); linux != nil {
					if res := linux.GetResources(); res != nil && res.GetCpu() != nil {
						committedCPUs, _ = cpuset.Parse(res.GetCpu().GetCpus())
					}
				}

				var kernelCPUs cpuset.CPUSet
				var kernelErr error
				switch {
				case cgroupsPath == "":
					kernelErr = errors.New("the runtime reported no cgroups path for this container")
				case cp.cgroupfs == nil:
					kernelErr = errors.New("this driver reads no cgroup tree: it mounts one only for defragmentation")
				default:
					kernelCPUs, kernelErr = cgroupfs.CPUSet(cp.cgroupfs, cgroupsPath)
				}

				// CCX-FORK: the three-way check exists to settle a move the
				// runtime never confirmed, and only a claim carrying a round
				// annotation can be in that position. Every other container is
				// upstream's case, where the record is simply the answer -- and
				// asking a third source about it can only turn a container that
				// needs pinning into one that is never pinned at all.
				if !hadRound {
					observed = append(observed, observedContainer{
						logger:    cLogger,
						claimUIDs: claimUIDs,
						desired:   allGuaranteedCPUs,
						elsewhere: committedCPUs.Union(kernelCPUs),
					})
					// Wherever either source can be read and disagrees. A driver
					// that died before pinning leaves a container the runtime
					// reports with no cpuset at all, which is this branch too: a
					// guaranteed container then runs on the whole node while its
					// claim charges a handful of CPUs, and nothing else will pin
					// it, because CreateContainer has already been and gone.
					if !committedCPUs.Equals(allGuaranteedCPUs) || (kernelErr == nil && !kernelCPUs.Equals(allGuaranteedCPUs)) {
						cLogger.Info("owned container is not on the CPUs its claim holds, converging",
							"committed", committedCPUs.String(), "kernel", kernelCPUs.String(), "desired", allGuaranteedCPUs.String())
						containerUpdates = append(containerUpdates, cpusetUpdate(container.GetId(), allGuaranteedCPUs))
					}
				} else {
					classification := cp.classifyContainer(allGuaranteedCPUs, committedCPUs, kernelCPUs, originUnion, targetUnion, kernelErr)
					switch classification {
					case containerSettled:
						cLogger.V(2).Info("owned container is settled", "cpus", allGuaranteedCPUs.String())
						claimsToClearRound = append(claimsToClearRound, claimUIDs...)
					case containerForwardApplicable:
						cLogger.Info("owned container is forward-applicable, converging", "cpus", allGuaranteedCPUs.String())
						containerUpdates = append(containerUpdates, cpusetUpdate(container.GetId(), allGuaranteedCPUs))
						claimsToClearRound = append(claimsToClearRound, claimUIDs...)
					case containerRollbackApplicable:
						cLogger.Info("owned container is rollback-applicable, reverting", "cpus", allGuaranteedCPUs.String())
						containerUpdates = append(containerUpdates, cpusetUpdate(container.GetId(), allGuaranteedCPUs))
						claimsToClearRound = append(claimsToClearRound, claimUIDs...)
					case containerUnknown:
						cLogger.Error(kernelErr, "owned container is in unknown state from three-way check, poisoning NUMA node",
							"desired", allGuaranteedCPUs.String(), "committed", committedCPUs.String(), "kernel", kernelCPUs.String())
						cp.poisonNUMANodeForCPUs(cLogger, allGuaranteedCPUs.Union(originUnion).Union(kernelCPUs))
					}
				}
			}
			podConfigStore.SetContainerState(types.UID(pod.GetUid()), state)
			cLogger.V(6).Info("set container state", "claims", len(claimUIDs))
		}
	}

	// CCX-FORK: the loop above rebuilt the store from what is running, which
	// cannot account for a claim whose container never started.
	cp.restoreUnstartedClaims(logger, cpuAllocationStore, claimTracker)

	cp.podConfigStore = podConfigStore
	cp.cpuAllocationStore = cpuAllocationStore
	cp.claimTracker = claimTracker
	cp.refreshAllocationMetrics()
	cp.reportForeignCPUs(ctx, cpuAllocationStore, observed)

	for _, uid := range claimsToClearRound {
		_ = cp.writeClaimPlacement(logger, uid)
	}

	// Reconcile container CPU masks to handle cases where the NRI plugin might have crashed
	// or restarted and missed updating the cgroup settings.
	// See: https://github.com/containerd/nri/issues/282
	sharedContainerUpdates, err := cp.getSharedContainerUpdates(logger, types.UID(""))
	if err != nil {
		return nil, err
	}
	containerUpdates = append(containerUpdates, sharedContainerUpdates...)
	logger.V(6).Info("synchronization complete", "updatesCount", len(containerUpdates))
	// CCX-FORK: the store was just rebuilt from what is actually running, which
	// is the other way a cache stops being empty.
	cp.republishStaleSlices(ctx)
	// CCX-FORK: a driver that has just learnt the node's real placements is
	// looking at a node nothing has defragmented since it went down, which is
	// exactly when the spread is worst.
	if cp.defrag.enabled {
		cp.requestReconcile()
	}
	return containerUpdates, nil
}

// restoreUnstartedClaims reserves the claims this node holds that no running
// container can account for.
//
// Synchronize rebuilds the store by walking the containers the runtime reports,
// and a container holding a claim cannot start until that claim is prepared. A
// driver that was down when the kubelet asked therefore leaves a deadlock behind
// it: the claim stays allocated and unprepared, every CreateContainer for it is
// refused, and nothing on either side can break the cycle -- the observed
// recovery was deleting the pod.
//
// The two halves that close it are already here. The projected claims say which
// claims this node is holding, and the CDI spec written at Prepare says where
// each one was put; a claim with both and no container is exactly the stuck
// case. Claims the projector does not name are left alone, so a spec left behind
// by an Unprepare this driver missed cannot resurrect itself.
func (cp *CPUDriver) restoreUnstartedClaims(logger logr.Logger, allocations *store.CPUAllocation, tracker *store.ClaimTracker) {
	if cp.claimReader == nil {
		return
	}

	claims, err := cp.claimReader.AllocatedClaims()
	if err != nil {
		logger.Error(err, "cannot read the projected claims, so claims without a container stay unprepared")
		return
	}

	for _, claim := range claims {
		if _, held := allocations.GetResourceClaimAllocation(claim.UID); held {
			continue
		}
		record, err := cp.cdiMgr.GetDeviceAllocations(getCDIDeviceName(claim.UID))
		if err != nil {
			// Never prepared, rather than prepared and forgotten. The kubelet
			// asks again for this one, and there is nothing to restore.
			continue
		}
		if err := allocations.ReserveResourceClaimAllocation(logger, claim.UID, record, false); err != nil {
			logger.Error(err, "cannot restore a claim no container holds", "claimUID", claim.UID)
			continue
		}
		// The claim's reservation goes back with it, from the spec rather than
		// from the projection, which carries none: the container this claim was
		// prepared for has yet to be created, and CreateContainer refuses a
		// claim whose reservation it cannot read on a runtime that reports no
		// CDI devices of its own.
		if len(record.ReservedFor) > 0 {
			tracker.SetReservedFor(claim.UID, record.ReservedFor)
		}
		logger.Info("restored a prepared claim no running container holds",
			"claimUID", claim.UID, "cpus", store.UnionOf(record.Requests).String())
	}
}

// revertRoundOrigin puts a claim's exclusive requests back on the CPUs the round
// moved them from, and reports whether it could.
//
// A round moves the claim's exclusive CPUs as a whole and spreads the target
// over those requests in name order, each keeping its size; the reverse is the
// same spread of the origin. Only the exclusive requests take part: a claim may
// also hold a share of a pool, and a pool never moves. Writing the origin into
// the first request instead would hand the claim's own CPUs to whichever request
// sorts first -- for the shipped VM example, whose requests are "helpers" and
// "vcpus", that is the pool share.
//
// A record whose exclusive requests no longer add up to the round's target is
// left alone: something has already rewritten it, and guessing which request the
// origin belongs to is how a claim ends up holding CPUs twice.
func revertRoundOrigin(record *store.ClaimRecord) bool {
	if record.Round == nil {
		return false
	}
	origin, target := record.Round.Origin, record.Round.Target
	if origin.IsEmpty() || origin.Size() != target.Size() {
		return false
	}
	var moved []int
	union := cpuset.New()
	for i, request := range record.Requests {
		if request.Role != store.RoleExclusive {
			continue
		}
		moved = append(moved, i)
		union = union.Union(request.CPUs)
	}
	if !union.Equals(target) {
		return false
	}
	cpuIDs := origin.List()
	for _, i := range moved {
		size := record.Requests[i].CPUs.Size()
		record.Requests[i].CPUs = cpuset.New(cpuIDs[:size]...)
		cpuIDs = cpuIDs[size:]
	}
	return true
}

func nonExclusiveCPUs(record store.ClaimRecord) cpuset.CPUSet {
	cpus := cpuset.New()
	for _, request := range record.Requests {
		if request.Role != store.RoleExclusive {
			cpus = cpus.Union(request.CPUs)
		}
	}
	return cpus
}

func cpusetUpdate(containerID string, cpus cpuset.CPUSet) *api.ContainerUpdate {
	update := &api.ContainerUpdate{ContainerId: containerID}
	update.SetLinuxCPUSetCPUs(cpus.String())
	return update
}

// observedContainer is a claim-holding container Synchronize converged onto the
// CPUs its claims hold, together with where the runtime and the kernel said it
// was. Whether those CPUs belong to another claim cannot be told while the store
// is still being rebuilt, so the question is asked once it is whole.
type observedContainer struct {
	logger    logr.Logger
	claimUIDs []types.UID
	desired   cpuset.CPUSet
	// elsewhere is everywhere the container was seen, whether the runtime
	// reported it or the kernel did. Empty when neither could say.
	elsewhere cpuset.CPUSet
}

// reportForeignCPUs counts the converged containers that were running on CPUs
// another claim holds, and says which.
//
// This is the shape a crash-looping driver leaves behind: nothing pinned the
// container after its restart, so it came back on whatever the runtime gives a
// container with no cpuset -- usually the whole node -- while its claim still
// charged a handful of CPUs. The update converging it is already queued; this
// is what makes the episode visible afterwards rather than silent.
func (cp *CPUDriver) reportForeignCPUs(ctx context.Context, allocations *store.CPUAllocation, observed []observedContainer) {
	if len(observed) == 0 {
		return
	}
	claimed := cpuset.New()
	for _, cpus := range allocations.ExclusiveClaimAllocations() {
		claimed = claimed.Union(cpus)
	}
	var byUID map[types.UID]*resourceapi.ResourceClaim
	for _, container := range observed {
		foreign := container.elsewhere.Intersection(claimed).Difference(container.desired)
		if foreign.IsEmpty() {
			continue
		}
		cp.metrics.RecordSynchronizeForeignCPUs()
		container.logger.Error(nil, "owned container was running on CPUs another claim holds, converging onto its own",
			"foreignCPUs", foreign.String(), "desiredCPUs", container.desired.String())
		if byUID == nil {
			byUID = cp.allocatedClaimsByUID(container.logger)
		}
		for _, uid := range container.claimUIDs {
			claim, ok := byUID[uid]
			if !ok {
				continue
			}
			cp.recordClaimEvent(ctx, claim, "ForeignCPUs", fmt.Sprintf(
				"container was running on %s, which another claim holds, and has been converged onto %s",
				foreign.String(), container.desired.String()))
		}
	}
}

// allocatedClaimsByUID is the projection indexed for lookup, or nil where there
// is no projection to read. Built once per Synchronize and only when something
// is actually being reported, since the common case reports nothing.
func (cp *CPUDriver) allocatedClaimsByUID(logger logr.Logger) map[types.UID]*resourceapi.ResourceClaim {
	if cp.claimReader == nil {
		return map[types.UID]*resourceapi.ResourceClaim{}
	}
	claims, err := cp.claimReader.AllocatedClaims()
	if err != nil {
		logger.Error(err, "cannot name the claims a foreign-cpuset container holds; reporting the metric alone")
		return map[types.UID]*resourceapi.ResourceClaim{}
	}
	byUID := make(map[types.UID]*resourceapi.ResourceClaim, len(claims))
	for _, claim := range claims {
		byUID[claim.UID] = claim
	}
	return byUID
}

// checkClaimPartition reports a restored claim whose CPUs no single partition
// holds, which is what a partition list edited under a running node looks like
// from here. The claim is kept: its container is running on those CPUs, and
// taking them away would stop a workload to enforce a description that changed
// after it started. A pass moves it home once one may.
//
// It checks that the claim sits inside some one partition, not inside the
// partition of the device its allocation charged, so a claim that drifted wholly
// into another partition passes. The narrower reading would report nothing on
// exactly the nodes it matters on: this runs while the store is being rebuilt
// from the specs on disk, and a spec written by an older driver names no charged
// device to compare against.
func (cp *CPUDriver) checkClaimPartition(logger logr.Logger, cpus cpuset.CPUSet) {
	if cpus.IsEmpty() {
		return
	}
	for _, partition := range cp.partitions {
		if cpus.IsSubsetOf(partition.CPUs) {
			return
		}
	}
	logger.Info("restored claim is held by no single partition, leaving it where it is", "cpus", cpus.String())
	cp.metrics.RecordMisplacedClaim()
}

// A claim given no CPUs of its own takes nothing away from anything else, so it
// binds to no single container and several containers and pods may reference it.
func exclusiveClaimUIDs(allocations *store.CPUAllocation, claimUIDs []types.UID) []types.UID {
	var owned []types.UID
	for _, claimUID := range claimUIDs {
		if allocations.HoldsExclusiveCPUs(claimUID) {
			owned = append(owned, claimUID)
		}
	}
	return owned
}

// runtimeCDIDevices returns the CDI device names the runtime reports for the
// container, or nil when it reports none.
//
// A nil result means "unknown", not "none": not every runtime fills the field
// in. Callers must treat nil as inconclusive rather than as a rejection.
func runtimeCDIDevices(ctr *api.Container) map[string]struct{} {
	devices := ctr.GetCDIDevices()
	if len(devices) == 0 {
		return nil
	}
	names := make(map[string]struct{}, len(devices))
	for _, dev := range devices {
		names[dev.GetName()] = struct{}{}
	}
	return names
}

// claimInjectedByRuntime reports whether the runtime confirms this driver's CDI
// device for claimUID was injected into the container.
//
// The DRA_CPUSET entry a container carries comes from its own pod spec, so a pod
// can name another pod's claim and, by winning the race to CreateContainer, take
// that claim's CPUs. The runtime's own record of the CDI devices kubelet asked it
// to inject cannot be forged that way, which makes it the stronger signal.
//
// reported must come from runtimeCDIDevices. A nil map means the runtime does not
// report CDI devices, and this returns true so the remaining checks decide.
func claimInjectedByRuntime(reported map[string]struct{}, claimUID types.UID) bool {
	if reported == nil {
		return true
	}
	_, ok := reported[cdiparser.QualifiedName(cdiVendor, cdiClass, getCDIDeviceName(claimUID))]
	return ok
}

// draEnvEntry is one DRA_CPUSET_* variable a container carries.
type draEnvEntry struct {
	claimUID types.UID
	// cpus is the placement the value named, and is unset when dynamic is true:
	// a claim whose placement may change has none to name.
	cpus    cpuset.CPUSet
	dynamic bool
}

// parseDRAEnv returns the claims a container's environment names.
//
// CCX-FORK: upstream returns only claim-to-cpuset pairs, since to it the value is
// the placement. Here the name is what matters and the value may say "dynamic".
func parseDRAEnv(logger logr.Logger, envs []string) ([]draEnvEntry, error) {
	var entries []draEnvEntry
	for _, env := range envs {
		if !strings.HasPrefix(env, cdiEnvVarPrefix) {
			continue
		}
		logger.V(4).Info("parsing DRA env entry", "env", env)
		key, value, found := strings.Cut(env, "=")
		if !found {
			return nil, fmt.Errorf("malformed DRA env entry %q", env)
		}
		uidStr, ok := strings.CutPrefix(key, cdiEnvVarPrefix+"_")
		if !ok {
			continue
		}

		entry := draEnvEntry{claimUID: types.UID(uidStr)}
		if value == cdiEnvDynamicValue {
			entry.dynamic = true
		} else {
			cpus, err := cpuset.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cpuset value %q from env %q: %w", value, env, err)
			}
			entry.cpus = cpus
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// parseDRAEnvToClaimAllocations returns the placements recorded by the
// environment edits of a driver-owned CDI spec, which unlike a container's
// environment is rewritten whenever a placement changes. Entries that name no
// placement are left out.
func parseDRAEnvToClaimAllocations(logger logr.Logger, envs []string) (map[types.UID]cpuset.CPUSet, error) {
	entries, err := parseDRAEnv(logger, envs)
	if err != nil {
		return nil, err
	}
	allocations := make(map[types.UID]cpuset.CPUSet, len(entries))
	for _, entry := range entries {
		if entry.dynamic {
			continue
		}
		allocations[entry.claimUID] = entry.cpus
	}
	return allocations, nil
}

// sharedContainerCPUs is the mask a container without a claim is confined to:
// whatever the claims have left over inside the partitions such a container may
// run in.
//
// CCX-FORK: upstream's dynamic pool spans the whole node, with no partition to
// keep a claimless container out of the cores claims are meant to have.
func (cp *CPUDriver) sharedContainerCPUs() cpuset.CPUSet {
	shared := cp.cpuAllocationStore.GetSharedCPUs()
	// A driver whose cores have not been resolved into partitions has no
	// description to enforce and keeps upstream's node-wide pool. On a node
	// whose cores nobody described the resolution is one default partition over
	// every allocatable CPU, so that is upstream's pool too; where an exclusive
	// partition exists, its free CPUs are not the pool's to hand out.
	if len(cp.partitions) == 0 {
		return shared
	}
	return shared.Intersection(cp.defaultPartitionCPUs)
}

func (cp *CPUDriver) getSharedContainerUpdates(logger logr.Logger, excludeID types.UID) ([]*api.ContainerUpdate, error) {
	updates := []*api.ContainerUpdate{}
	sharedCPUs := cp.sharedContainerCPUs()
	preparedCPUs := cp.cpuAllocationStore.GetPreparedCPUs()
	sharedCPUContainers := cp.podConfigStore.GetContainersWithSharedCPUs()
	// An empty CPUSet is serialized by NRI as Cpus="", which means "do not
	// change the current CPUSet" rather than "clear the CPUSet". Never emit
	// that update while a prepared DRA allocation has exhausted the pool and
	// shared containers still exist. An empty pool with no prepared allocation
	// is valid when the node has no driver-managed CPUs.
	if sharedCPUs.IsEmpty() && !preparedCPUs.IsEmpty() && len(sharedCPUContainers) > 0 {
		return nil, fmt.Errorf("cannot update shared containers: no shared CPUs available")
	}
	logger.V(2).Info("updating CPU allocation for containers without guaranteed CPUs", "sharedCPUs", sharedCPUs.String())
	for _, containerUID := range sharedCPUContainers {
		if containerUID == excludeID {
			// Skip the container being created as it is already covered in the container adjustment.
			continue
		}

		containerUpdate := &api.ContainerUpdate{
			ContainerId: string(containerUID),
		}
		containerUpdate.SetLinuxCPUSetCPUs(sharedCPUs.String())
		updates = append(updates, containerUpdate)
	}
	return updates, nil
}

// CreateContainer handles container creation requests from the NRI.
func (cp *CPUDriver) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (radjust *api.ContainerAdjustment, rupdates []*api.ContainerUpdate, rerr error) {
	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: CreateContainer")
	defer logger.V(2).Info("end: CreateContainer")

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	adjust := &api.ContainerAdjustment{}
	var updates []*api.ContainerUpdate

	claimCount := -1
	entries, err := parseDRAEnv(logger, ctr.Env)
	defer func() { cp.metrics.RecordNRICreateContainer(rerr, claimCount, time.Since(startTime)) }()
	if err != nil {
		logger.Error(err, "error parsing DRA env for container")
		return nil, nil, err
	}
	claimCount = len(entries)

	containerId := types.UID(ctr.GetId())
	podUID := types.UID(pod.GetUid())

	if claimCount == 0 {
		// This is a shared container.
		sharedCPUs := cp.sharedContainerCPUs()
		if sharedCPUs.IsEmpty() && !cp.cpuAllocationStore.GetPreparedCPUs().IsEmpty() {
			// NRI cannot represent an empty CPUSet as a ContainerAdjustment. Fail
			// closed instead of allowing the runtime to keep its default affinity.
			return nil, nil, fmt.Errorf("cannot create shared container: no shared CPUs available")
		}
		state := store.NewContainerState(ctr.GetName(), containerId).WithCgroup(ctr.GetLinux().GetCgroupsPath())
		cp.podConfigStore.SetContainerState(podUID, state)

		logger.V(2).Info("no guaranteed CPUs found, using shared CPUs", "sharedCPUs", sharedCPUs.String())
		adjust.SetLinuxCPUSetCPUs(sharedCPUs.String())
	} else {
		// CCX-FORK: upstream pins the container to the cpuset parsed out of its
		// env, after checking that value against the store for equality.
		//
		// NRI invokes CreateContainer for all containers. The DRA env only names
		// which claims the container holds; where each one is placed comes from
		// the store, so a claim moved between Prepare and CreateContainer is
		// applied at its current placement rather than at the stale one the
		// container's immutable environment carries.
		claimUIDs := []types.UID{}
		reportedCDIDevices := runtimeCDIDevices(ctr)
		for _, entry := range entries {
			if !claimInjectedByRuntime(reportedCDIDevices, entry.claimUID) {
				return nil, nil, fmt.Errorf("container claims %q but the runtime injected no CDI device for it", entry.claimUID)
			}
			if reportedCDIDevices == nil {
				// The runtime reports no CDI devices at all (CRI-O today); fall
				// back to the claim's own API-server reservation, which a pod
				// spec cannot forge the way it can a DRA_CPUSET_* env value.
				if reserved, recorded := cp.claimTracker.ReservedFor(entry.claimUID, podUID); !reserved || !recorded {
					return nil, nil, fmt.Errorf("container claims %q but the pod is not in its reservation", entry.claimUID)
				}
			}
			claimUIDs = append(claimUIDs, entry.claimUID)
		}
		// CCX-FORK: upstream binds every claim the container names.
		var newOwners []types.UID
		if owned := exclusiveClaimUIDs(cp.cpuAllocationStore, claimUIDs); len(owned) > 0 {
			newOwners, err = cp.claimTracker.SetOwner(logger, podUID, ctr.Name, owned...)
			if err != nil {
				return nil, nil, err
			}
		}
		guaranteedCPUs, err := cp.cpuAllocationStore.GetResourceClaimAllocationUnion(claimUIDs...)
		if err != nil {
			cp.claimTracker.Cleanup(newOwners...)
			return nil, nil, err
		}
		logger.V(2).Info("guaranteed CPUs found", "cpus", guaranteedCPUs.String())
		state := store.NewContainerState(ctr.GetName(), containerId, claimUIDs...).WithCgroup(ctr.GetLinux().GetCgroupsPath())
		adjust.SetLinuxCPUSetCPUs(guaranteedCPUs.String())
		// A new owner means this is the first CreateContainer after Prepare, so
		// existing shared containers must be moved off the newly claimed CPUs.
		// On restart the owner already exists and no shared-container updates are
		// needed.
		if len(newOwners) > 0 {
			updates, err = cp.getSharedContainerUpdates(logger, containerId)
			if err != nil {
				cp.claimTracker.Cleanup(newOwners...)
				return nil, nil, err
			}
		}
		cp.podConfigStore.SetContainerState(podUID, state)
	}

	return adjust, updates, nil
}

// StopContainer removes runtime container state without changing DRA-owned allocations.
//
// CPU-allocation lifetime across the DRA and NRI hooks:
//   - PrepareResourceClaims (DRA) reserves CPUs and writes the CDI spec carrying that cpuset.
//   - CreateContainer (NRI) validates the CDI cpuset and applies it to the container.
//   - StopContainer (NRI, here) removes only the matching runtime container state. The prepared
//     allocation and owner remain unchanged so a restarted container reuses the same CPUs.
//   - UnprepareResourceClaims (DRA) is the authoritative release point for the allocation and owner.
//   - Synchronize (NRI, on restart) rebuilds the stores from the running containers' CDI env.
func (cp *CPUDriver) StopContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) ([]*api.ContainerUpdate, error) {
	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: StopContainer")
	defer logger.V(2).Info("end: StopContainer")

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	updates := []*api.ContainerUpdate{}
	claimUIDs, removed := cp.podConfigStore.RemoveContainerState(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()))
	if !removed {
		logger.V(2).Info("ignoring stale or unknown StopContainer event")
		return updates, nil
	}

	// leverage the fact the hook can't fail in our current design
	cp.metrics.RecordNRIStopContainer(nil, len(claimUIDs), time.Since(startTime))
	return updates, nil
}

// RemoveContainer handles container removal requests from the NRI.
func (cp *CPUDriver) RemoveContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: RemoveContainer")
	defer logger.V(2).Info("end: RemoveContainer")

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	claimUIDs, removed := cp.podConfigStore.RemoveContainerState(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()))
	if !removed {
		logger.V(2).Info("ignoring stale or unknown RemoveContainer event")
		return nil
	}
	if len(claimUIDs) > 0 {
		// this serves only for debugging purposes. We should never get here
		updates, err := cp.getSharedContainerUpdates(logger, types.UID(ctr.GetId()))
		if err != nil {
			logger.Error(err, "unable to calculate shared container updates after RemoveContainer")
		} else {
			logger.Info("RemoveContainer spurious updates needed (unexpected, please file a bug)", "updates", updates)
		}
	}

	// leverage the fact the hook can't fail in our current design
	// **NOTE** because of the flow (see comment before StopContainer) this metric becomes a signal
	// we leaked state and StopContainer didn't clean up properly.
	cp.metrics.RecordNRIRemoveContainer(nil, len(claimUIDs), time.Since(startTime))
	return nil
}

type containerClassification int

const (
	containerSettled containerClassification = iota
	containerForwardApplicable
	containerRollbackApplicable
	containerUnknown
)

func (cp *CPUDriver) classifyContainer(desired, committed, kernel, origin, target cpuset.CPUSet, kernelErr error) containerClassification {
	if kernelErr != nil {
		return containerUnknown
	}

	if kernel.IsEmpty() {
		if committed.IsEmpty() {
			if desired.Equals(target) {
				return containerForwardApplicable
			}
			return containerRollbackApplicable
		}
		if committed.Equals(desired) {
			return containerSettled
		}
		if desired.Equals(target) && committed.Equals(origin) {
			return containerForwardApplicable
		}
		if desired.Equals(origin) && committed.Equals(target) {
			return containerRollbackApplicable
		}
		return containerUnknown
	}

	if kernel.Equals(desired) && (committed.IsEmpty() || committed.Equals(desired)) {
		return containerSettled
	}

	if desired.Equals(target) {
		if (kernel.Equals(origin) || kernel.Equals(target)) && (committed.IsEmpty() || committed.Equals(origin) || committed.Equals(target)) {
			return containerForwardApplicable
		}
		return containerUnknown
	}

	if desired.Equals(origin) {
		if (kernel.Equals(target) || committed.Equals(target)) && (kernel.Equals(origin) || kernel.Equals(target)) && (committed.IsEmpty() || committed.Equals(origin) || committed.Equals(target)) {
			return containerRollbackApplicable
		}
		if kernel.Equals(origin) && (committed.IsEmpty() || committed.Equals(origin)) {
			return containerSettled
		}
		return containerUnknown
	}

	return containerUnknown
}

func (cp *CPUDriver) reconcileActiveRounds(logger logr.Logger) {
	allocations := cp.cdiMgr.PreparedClaimAllocations(logger)
	if len(allocations) == 0 {
		return
	}

	rounds := make(map[string]map[types.UID]store.ClaimRecord)
	for uid, record := range allocations {
		if record.Round != nil && record.Round.RoundID != "" {
			if rounds[record.Round.RoundID] == nil {
				rounds[record.Round.RoundID] = make(map[types.UID]store.ClaimRecord)
			}
			rounds[record.Round.RoundID][uid] = record
		}
	}

	if len(rounds) == 0 {
		return
	}

	for roundID, participants := range rounds {
		isComplete := true
		for uid, rec := range participants {
			for _, partnerUID := range rec.Round.Partners {
				partnerRec, exists := participants[partnerUID]
				if !exists {
					isComplete = false
					break
				}
				if !slices.Contains(partnerRec.Round.Partners, uid) {
					isComplete = false
					break
				}
			}
			if !isComplete {
				break
			}
		}

		if isComplete {
			logger.Info("active defrag round is complete, converging forward", "roundID", roundID, "participants", len(participants))
			continue
		}

		logger.Info("active defrag round is incomplete, reverting to origin", "roundID", roundID, "participants", len(participants))
		for uid, rec := range participants {
			if !revertRoundOrigin(&rec) {
				logger.Error(nil, "leaving an incomplete round's CDI spec as written: its requests do not hold the CPUs the round moved",
					"roundID", roundID, "claimUID", uid, "target", rec.Round.Target.String(), "origin", rec.Round.Origin.String())
				continue
			}
			envVar := fmt.Sprintf("%s_%s=%s", cdiEnvVarPrefix, uid, cp.cdiEnvValue(rec))
			if err := cp.cdiMgr.AddDevice(logger, getCDIDeviceName(uid), envVar, rec); err != nil {
				logger.Error(err, "failed to revert incomplete round CDI spec", "roundID", roundID, "claimUID", uid)
			}
		}
		_ = cp.cdiMgr.Refresh()
	}
}
