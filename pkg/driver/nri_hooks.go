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
	"fmt"
	"strings"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
)

// Synchronize is called by the NRI to synchronize the state of the driver during bootstrap.
func (cp *CPUDriver) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))

	// this happens once at startup and it's critical enough that we always want to see it.
	logger.Info("begin: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))
	defer logger.Info("end: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))

	// Synchronize rebuilds all three stores and swaps them in, so nothing that
	// reads or writes a placement may run while it does.
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	cpuAllocationStore := store.NewCPUAllocation(cp.topology.cpuTopology, cp.topology.reservedCPUs)
	podConfigStore := store.NewPodConfig()
	claimTracker := store.NewClaimTracker()
	var containerUpdates []*api.ContainerUpdate
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
				desired := store.UnionOf(recorded)
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
					cp.metricsRecorder().RecordSynchronizeSkippedClaim()
					continue
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
					cp.metricsRecorder().RecordSynchronizeSkippedClaim()
					claimUIDs = nil
				}
			}
			// This container is exactly as trustworthy a source as a fresh
			// Prepare: CreateContainer's CRI-O fallback reads this to
			// authenticate a container recreated after this driver restarts, on
			// a runtime that never reports CDI devices. Every claim it holds
			// needs one, ownership or not, since that fallback checks them all.
			for _, uid := range claimUIDs {
				claimTracker.SetReservedFor(uid, []types.UID{types.UID(pod.Uid)})
			}

			var state *store.ContainerState
			if len(claimUIDs) == 0 {
				state = store.NewContainerState(container.GetName(), containerUID)
			} else {
				allGuaranteedCPUs, err := cpuAllocationStore.GetResourceClaimAllocationUnion(claimUIDs...)
				if err != nil {
					return nil, err
				}
				cLogger.V(2).Info("found guaranteed CPUs", "cpus", allGuaranteedCPUs.String())
				state = store.NewContainerState(container.GetName(), containerUID, claimUIDs...)

				// Reconcile guaranteed container CPU mask.
				guaranteedUpdate := &api.ContainerUpdate{
					ContainerId: container.GetId(),
				}
				guaranteedUpdate.SetLinuxCPUSetCPUs(allGuaranteedCPUs.String())
				containerUpdates = append(containerUpdates, guaranteedUpdate)
			}
			podConfigStore.SetContainerState(types.UID(pod.GetUid()), state)
		}
	}

	cp.podConfigStore = podConfigStore
	cp.cpuAllocationStore = cpuAllocationStore
	cp.claimTracker = claimTracker
	cp.refreshAllocationMetrics()

	// Reconcile container CPU masks to handle cases where the NRI plugin might have crashed
	// or restarted and missed updating the cgroup settings.
	// See: https://github.com/containerd/nri/issues/282
	sharedContainerUpdates, err := cp.getSharedContainerUpdates(logger, types.UID(""))
	if err != nil {
		return nil, err
	}
	containerUpdates = append(containerUpdates, sharedContainerUpdates...)
	return containerUpdates, nil
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

func (cp *CPUDriver) getSharedContainerUpdates(logger logr.Logger, excludeID types.UID) ([]*api.ContainerUpdate, error) {
	updates := []*api.ContainerUpdate{}
	sharedCPUs := cp.cpuAllocationStore.GetSharedCPUs()
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
func (cp *CPUDriver) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (*api.ContainerAdjustment, []*api.ContainerUpdate, error) {
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: CreateContainer")
	defer logger.V(2).Info("end: CreateContainer")

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	adjust := &api.ContainerAdjustment{}
	var updates []*api.ContainerUpdate

	entries, err := parseDRAEnv(logger, ctr.Env)
	if err != nil {
		logger.Error(err, "error parsing DRA env for container")
		return nil, nil, err
	}

	containerId := types.UID(ctr.GetId())
	podUID := types.UID(pod.GetUid())

	if len(entries) == 0 {
		// This is a shared container.
		sharedCPUs := cp.cpuAllocationStore.GetSharedCPUs()
		if sharedCPUs.IsEmpty() && !cp.cpuAllocationStore.GetPreparedCPUs().IsEmpty() {
			// NRI cannot represent an empty CPUSet as a ContainerAdjustment. Fail
			// closed instead of allowing the runtime to keep its default affinity.
			return nil, nil, fmt.Errorf("cannot create shared container: no shared CPUs available")
		}
		state := store.NewContainerState(ctr.GetName(), containerId)
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
		state := store.NewContainerState(ctr.GetName(), containerId, claimUIDs...)
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
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: StopContainer")
	defer logger.V(2).Info("end: StopContainer")

	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	updates := []*api.ContainerUpdate{}
	_, removed := cp.podConfigStore.RemoveContainerState(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()))
	if !removed {
		logger.V(2).Info("ignoring stale or unknown StopContainer event")
		return updates, nil
	}
	return updates, nil
}

// RemoveContainer handles container removal requests from the NRI.
func (cp *CPUDriver) RemoveContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
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
	return nil
}
