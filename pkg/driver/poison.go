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
	"slices"
	"time"

	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	"k8s.io/utils/cpuset"
)

// poisonedNode is a NUMA node the driver has stopped vouching for. An exchange
// there ended in a state nobody can name -- the runtime would neither finish it
// nor undo it -- so two of its claims may be sharing CPUs and the driver's
// record of which claim is where may be wrong.
//
// Everything the fence does follows from that one fact. New claims are refused
// there, because the CPUs on offer are computed from a record that may be wrong.
// Nothing further is planned there, for the same reason. The devices are tainted
// so the scheduler stops sending claims to a node that will refuse them. And the
// fence lifts on evidence rather than on time: the kernel is asked where the
// participants' containers actually are, and the node reopens when that agrees
// with the ledger.
type poisonedNode struct {
	since time.Time
	// scopes are the planning regions whose unsettled rounds fenced this node.
	// The node reopens when every one of them has been settled from a read-back.
	scopes map[defragScope]struct{}
}

// poisonNode fences the NUMA node of a scope whose exchange could not be
// settled. Called with applyMu held.
func (cp *CPUDriver) poisonNode(logger logr.Logger, scope defragScope) {
	fence, already := cp.poisonedNodes[scope.numaNodeID]
	if !already {
		fence = &poisonedNode{since: time.Now(), scopes: map[defragScope]struct{}{}}
		cp.poisonedNodes[scope.numaNodeID] = fence
		cp.metricsRecorder().RecordDefragNodePoisoned()
		cp.metricsRecorder().SetDefragNodePoisoned(scope.numaNodeID, true)
		logger.Info("fencing a NUMA node: an exchange there could not be settled either way, so two claims may be sharing CPUs and this driver cannot say which",
			"numaNode", scope.numaNodeID)
	}
	fence.scopes[scope] = struct{}{}
}

// nodeIsPoisoned reports whether a NUMA node is fenced. Called with applyMu
// held.
func (cp *CPUDriver) nodeIsPoisoned(numaNodeID int) bool {
	_, fenced := cp.poisonedNodes[numaNodeID]
	return fenced
}

// poisonedNUMANodes is the fenced nodes as a plain set, which is what the
// published slices carry and what a later publication compares against. Called
// with applyMu held.
func (cp *CPUDriver) poisonedNUMANodes() map[int]bool {
	fenced := make(map[int]bool, len(cp.poisonedNodes))
	for numaNodeID := range cp.poisonedNodes {
		fenced[numaNodeID] = true
	}
	return fenced
}

// poisonedNUMANodesOf is the fenced NUMA nodes a set of CPUs lies in, which is
// how a device is told from the fence: a device that reaches into a fenced node
// cannot be allocated from, whichever grouping produced it. Called with applyMu
// held.
func (cp *CPUDriver) poisonedNUMANodesOf(cpus cpuset.CPUSet) []int {
	if len(cp.poisonedNodes) == 0 {
		return nil
	}
	var fenced []int
	details := cp.topology.cpuTopology.CPUDetails
	for numaNodeID := range cp.poisonedNodes {
		if !details.CPUsInNUMANodes(numaNodeID).Intersection(cpus).IsEmpty() {
			fenced = append(fenced, numaNodeID)
		}
	}
	slices.Sort(fenced)
	return fenced
}

// liftPoison asks the kernel where the participants of a fenced node's unsettled
// exchanges are actually running, and reopens the node when what it says agrees
// with the ledger.
//
// The ledger is rebuilt from the answer rather than from either guess: a call
// that failed may have applied work before its reply was lost, so neither the
// forward state nor the rolled-back one may be assumed. An exchange whose
// containers all sit on their targets is committed; one whose containers all sit
// where they came from is undone; anything else leaves the fence up.
func (cp *CPUDriver) liftPoison(logger logr.Logger, numaNodeID int) {
	cp.applyMu.Lock()
	defer cp.applyMu.Unlock()

	fence, fenced := cp.poisonedNodes[numaNodeID]
	if !fenced {
		return
	}
	logger = logger.WithValues("numaNode", numaNodeID)
	for scope := range fence.scopes {
		round := cp.pendingRounds[scope]
		if round == nil || round.store != cp.cpuAllocationStore {
			// Synchronize rebuilt the stores from the specs on disk and converged
			// the containers onto them, which is the same answer a read-back would
			// have given.
			delete(fence.scopes, scope)
			continue
		}
		if !cp.settleFromReadBack(logger.WithValues(scope.logValues()...), round) {
			cp.pendingRounds[scope] = round.retainUnsettled()
			continue
		}
		delete(cp.pendingRounds, scope)
		cp.forgetDefragRetry(scope)
		delete(fence.scopes, scope)
	}
	if len(fence.scopes) > 0 {
		return
	}
	delete(cp.poisonedNodes, numaNodeID)
	cp.metricsRecorder().RecordDefragNodeReopened(time.Since(fence.since))
	cp.metricsRecorder().SetDefragNodePoisoned(numaNodeID, false)
	logger.Info("reopening a NUMA node: the CPUs its containers are running on agree with this driver's records")
}

// settleFromReadBack settles every exchange of a round from what the kernel says,
// and reports whether all of them could be. Called with applyMu held.
func (cp *CPUDriver) settleFromReadBack(logger logr.Logger, round *defragRound) bool {
	settled := true
	for _, step := range defragSteps(round.moves) {
		if step[0].Exchange == 0 {
			continue
		}
		sLogger := logger.WithValues("claimUIDs", stepClaims(step))
		switch cp.readBackOf(sLogger, step) {
		case readBackForward:
			round.outcomes[step[0].Exchange] = exchangeApplied
		case readBackOrigin:
			round.outcomes[step[0].Exchange] = exchangeUndone
		default:
			cp.metricsRecorder().RecordDefragReadbackMismatch()
			round.outcomes[step[0].Exchange] = exchangeUnsettled
		}
		if !cp.settleExchangeStep(sLogger, round, step) {
			settled = false
		}
	}
	return settled
}

// readBack is what the kernel says about an exchange's containers.
type readBack int

const (
	// readBackUnknown means they disagree with each other, or with both of the
	// two states the exchange could be in, or could not be read at all.
	readBackUnknown readBack = iota
	// readBackForward means every one of them took its new cpuset.
	readBackForward
	// readBackOrigin means none of them did.
	readBackOrigin
)

// readBackOf reads every container of an exchange back from the kernel and
// reports which of the two states they are all in.
//
// A claim whose container has gone, or was replaced, says nothing either way: a
// container created after the reservation is pinned from the store, which
// already holds the target, so it is on the forward state by construction.
//
// Called with applyMu held.
func (cp *CPUDriver) readBackOf(logger logr.Logger, step []defrag.Move) readBack {
	if cp.cgroupfs == nil {
		logger.Info("cannot read a container's CPUs back: no cgroup tree is mounted")
		return readBackUnknown
	}
	answer, seen := readBackForward, false
	for _, move := range step {
		owner, ok := cp.claimTracker.Owner(move.ClaimUID)
		if !ok {
			continue
		}
		state := cp.podConfigStore.GetContainerState(owner.PodUID, owner.ContainerName)
		if state == nil {
			continue
		}
		live, err := cgroupfs.CPUSet(cp.cgroupfs, state.CgroupPath())
		if err != nil {
			logger.Error(err, "cannot read a container's CPUs back", "claimUID", move.ClaimUID)
			return readBackUnknown
		}
		forward, err := cp.cpuAllocationStore.GetResourceClaimAllocationUnion(state.ClaimUIDs()...)
		if err != nil {
			logger.Error(err, "cannot say where a container belongs", "claimUID", move.ClaimUID)
			return readBackUnknown
		}
		origin, err := cp.cpuAllocationStore.GetResourceClaimOriginUnion(state.ClaimUIDs()...)
		if err != nil {
			logger.Error(err, "cannot say where a container came from", "claimUID", move.ClaimUID)
			return readBackUnknown
		}

		var says readBack
		switch {
		case live.Equals(forward):
			says = readBackForward
		case live.Equals(origin):
			says = readBackOrigin
		default:
			logger.Info("a container is on CPUs this driver cannot account for", "claimUID", move.ClaimUID,
				"cpus", live.String(), "target", forward.String(), "origin", origin.String())
			return readBackUnknown
		}
		if seen && says != answer {
			logger.Info("the containers of one exchange are in different states", "claimUID", move.ClaimUID)
			return readBackUnknown
		}
		answer, seen = says, true
	}
	return answer
}
