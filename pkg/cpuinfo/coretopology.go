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

package cpuinfo

import (
	"k8s.io/utils/cpuset"
)

// CoreLocation identifies one physical core.
//
// CoreID on its own does not identify a core: the kernel restarts core_id
// numbering on every socket, and on architectures with a cluster level between
// socket and core it can repeat within a socket too. Whole-core reasoning must
// therefore key on all three fields, as GetCPUTopology already does when it
// counts NumCores.
type CoreLocation struct {
	SocketID  int
	ClusterID int
	CoreID    int
}

func coreLocationOf(info CPUInfo) CoreLocation {
	return CoreLocation{
		SocketID:  info.SocketID,
		ClusterID: info.ClusterID,
		CoreID:    info.CoreID,
	}
}

// CoreOf returns the physical core cpuID sits on. ok is false when cpuID is not
// part of this CPUDetails.
func (d CPUDetails) CoreOf(cpuID int) (loc CoreLocation, ok bool) {
	info, ok := d[cpuID]
	if !ok {
		return CoreLocation{}, false
	}
	return coreLocationOf(info), true
}

// CPUsInCoreLocations returns the logical CPU IDs on the given physical cores.
//
// This is the socket- and cluster-aware counterpart of CPUsInCores, which
// matches on CoreID alone and so conflates same-numbered cores on different
// sockets.
func (d CPUDetails) CPUsInCoreLocations(locs ...CoreLocation) cpuset.CPUSet {
	wanted := make(map[CoreLocation]struct{}, len(locs))
	for _, loc := range locs {
		wanted[loc] = struct{}{}
	}
	var cpuIDs []int
	for cpuID, info := range d {
		if _, ok := wanted[coreLocationOf(info)]; ok {
			cpuIDs = append(cpuIDs, cpuID)
		}
	}
	return cpuset.New(cpuIDs...)
}

// SiblingsOf returns every logical CPU sharing a physical core with cpuID,
// including cpuID itself. The result is empty when cpuID is not part of this
// CPUDetails.
//
// This does not use CPUInfo.SiblingCPUID, which records a single sibling and is
// left unset on cores with more than two threads, so it is also correct on
// 4- and 8-way SMT.
func (d CPUDetails) SiblingsOf(cpuID int) cpuset.CPUSet {
	loc, ok := d.CoreOf(cpuID)
	if !ok {
		return cpuset.New()
	}
	return d.CPUsInCoreLocations(loc)
}

// CompleteCores returns the subset of cpus whose physical core is wholly
// contained in cpus, dropping any CPU that has a sibling outside cpus. CPUs
// absent from this CPUDetails are dropped, since their core is unknown.
//
// Core membership is judged against the receiver, so the receiver must be the
// full topology: calling this on an already-filtered CPUDetails would report
// partial cores as complete.
func (d CPUDetails) CompleteCores(cpus cpuset.CPUSet) cpuset.CPUSet {
	threadsPerCore := make(map[CoreLocation]int, len(d))
	for _, info := range d {
		threadsPerCore[coreLocationOf(info)]++
	}

	present := make(map[CoreLocation][]int, len(threadsPerCore))
	for _, cpuID := range cpus.List() {
		info, ok := d[cpuID]
		if !ok {
			continue
		}
		loc := coreLocationOf(info)
		present[loc] = append(present[loc], cpuID)
	}

	var complete []int
	for loc, members := range present {
		if len(members) == threadsPerCore[loc] {
			complete = append(complete, members...)
		}
	}
	return cpuset.New(complete...)
}
