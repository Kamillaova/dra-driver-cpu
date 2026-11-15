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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// reservedHold is a claim prepared before the driver started whose exclusive
// CPUs a reserved partition holds since the node's partitions changed.
type reservedHold struct {
	namespace string
	name      string
	requests  []heldRequest
}

// heldRequest is the CPUs of one exclusive request a reserved partition holds.
type heldRequest struct {
	partition string
	request   string
	cpus      cpuset.CPUSet
}

// reservedHoldOf returns what the reserved partitions hold of a claim's
// exclusive requests. A shared request's recorded CPUs are where its devices
// were, not CPUs it holds, so they are left out.
func reservedHoldOf(reserved []device.Partition, record store.ClaimRecord) (reservedHold, bool) {
	hold := reservedHold{namespace: record.Namespace, name: record.Name}
	for _, request := range record.Requests {
		if request.Role != store.RoleExclusive {
			continue
		}
		for _, partition := range reserved {
			if cpus := request.CPUs.Intersection(partition.CPUs); !cpus.IsEmpty() {
				hold.requests = append(hold.requests, heldRequest{partition: partition.Name, request: request.Request, cpus: cpus})
			}
		}
	}
	slices.SortFunc(hold.requests, func(a, b heldRequest) int {
		return cmp.Or(strings.Compare(a.request, b.request), strings.Compare(a.partition, b.partition))
	})
	return hold, len(hold.requests) > 0
}

func (h reservedHold) claim(uid types.UID) string {
	if h.name == "" {
		return fmt.Sprintf("prepared claim with UID %q", uid)
	}
	return fmt.Sprintf("prepared claim %s/%s", h.namespace, h.name)
}

func (h reservedHold) describe(uid types.UID) string {
	held := make([]string, 0, len(h.requests))
	for _, request := range h.requests {
		clause := fmt.Sprintf("reserved partition %q now holds CPUs %s of %s", request.partition, request.cpus.String(), h.claim(uid))
		if request.request != "" {
			clause += fmt.Sprintf(" (request %q)", request.request)
		}
		held = append(held, clause)
	}
	return strings.Join(held, ", and ")
}

func (h reservedHold) refusal(uid types.UID) string {
	return h.describe(uid) + "; its containers are stopped: delete the claim's pod or keep those CPUs out of the partition"
}

// recordReservedHolds finds the restored claims whose exclusive CPUs a
// reserved partition holds. Callers hold stateMu, or the driver serves no
// hook yet.
func (cp *CPUDriver) recordReservedHolds(logger logr.Logger, claims map[types.UID]store.ClaimRecord) {
	for _, uid := range slices.Sorted(maps.Keys(claims)) {
		hold, held := reservedHoldOf(cp.reservedPartitions, claims[uid])
		if !held {
			continue
		}
		if cp.reservedHolds == nil {
			cp.reservedHolds = make(map[types.UID]reservedHold)
		}
		cp.reservedHolds[uid] = hold
		logger.Info("stopping the containers of a claim a reserved partition now holds", "claimUID", uid, "reason", hold.refusal(uid))
	}
	cp.reservedHoldsClaims.Store(len(cp.reservedHolds) > 0)
}

// heldClaimOf returns, in claim UID order, the first claim a container names
// whose CPUs a reserved partition holds.
func (cp *CPUDriver) heldClaimOf(claims map[types.UID]draEnvClaim) (types.UID, reservedHold, bool) {
	if len(cp.reservedHolds) == 0 {
		return "", reservedHold{}, false
	}
	for _, uid := range slices.Sorted(maps.Keys(claims)) {
		if hold, held := cp.reservedHolds[uid]; held {
			return uid, hold, true
		}
	}
	return "", reservedHold{}, false
}

// releaseReservedHold forgets a held claim once it is unprepared, and has the
// condition writer write CPUPartitionDegraded again. It makes no API call.
// Callers hold stateMu.
func (cp *CPUDriver) releaseReservedHold(claimUID types.UID) {
	if _, held := cp.reservedHolds[claimUID]; !held {
		return
	}
	delete(cp.reservedHolds, claimUID)
	cp.reservedHoldsClaims.Store(len(cp.reservedHolds) > 0)
	cp.nodeConditions.requeue()
}

// reservedHoldsMessage names every claim a reserved partition holds, in
// claim UID order, or is empty where none is held.
func (cp *CPUDriver) reservedHoldsMessage() string {
	if len(cp.reservedHolds) == 0 {
		return ""
	}
	held := make([]string, 0, len(cp.reservedHolds))
	for _, uid := range slices.Sorted(maps.Keys(cp.reservedHolds)) {
		held = append(held, cp.reservedHolds[uid].describe(uid))
	}
	if len(held) == 1 {
		return held[0] + "; its containers are stopped: delete the claim's pod or keep those CPUs out of the partition"
	}
	return strings.Join(held, "; ") + "; their containers are stopped: delete the claims' pods or keep those CPUs out of the partitions"
}

// killUpdate stops a container: the runtime writes the cgroup v2 file
// cgroup.kill of the container's cgroup, which kills every process of its
// tree, threaded children included.
func killUpdate(containerID string) *api.ContainerUpdate {
	update := &api.ContainerUpdate{ContainerId: containerID}
	// NRI defines a container eviction that no runtime receives: its
	// pkg/adaptation drops it in every containerd and CRI-O.
	update.AddLinuxUnified("cgroup.kill", "1")
	return update
}

// hasTask reports whether a listed container has a task to kill. A created
// container has none, and its start is refused instead.
func hasTask(ctr *api.Container) bool {
	switch ctr.GetState() {
	case api.ContainerState_CONTAINER_RUNNING, api.ContainerState_CONTAINER_PAUSED:
		return true
	}
	return false
}
