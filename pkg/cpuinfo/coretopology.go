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

// CoreLocation identifies one physical core, by the lowest logical CPU that
// sits on it.
//
// The identity comes from the sibling list the kernel publishes for each CPU,
// which names every thread of that CPU's core and is exact on every
// architecture. The (physical_package_id, cluster_id, core_id) triple is not:
// core_id is unique only within a (package, cluster), and a platform that
// publishes no cluster_id -- every CPU then reports -1 -- may repeat core_id
// within a package, which is the shape of an arm64 machine whose big and little
// cores number from zero each. Two distinct cores sharing a key is the one
// mistake whole-core reasoning may not make: it would report half of each as a
// complete core.
//
// A topology carrying no sibling lists falls back to the triple, resolved to the
// lowest CPU sharing it, which is as good as that topology can support. Anything
// the sysfs parser builds carries them, since it fails a CPU whose core_cpus_list
// and thread_siblings_list are both unreadable.
type CoreLocation struct {
	// FirstThread is the lowest logical CPU of the core. It is an identity
	// rather than a member: the CPU it names may itself be outside the set
	// being reasoned about.
	FirstThread int
}

// coreLocationOf is the core identity of one CPU, for a topology whose CPUs
// carry sibling lists. ok is false when this CPU carries none and the caller has
// to fall back to the triple.
func coreLocationOf(info CPUInfo) (CoreLocation, bool) {
	if info.SiblingCPUSet.IsEmpty() {
		return CoreLocation{}, false
	}
	return CoreLocation{FirstThread: info.SiblingCPUSet.List()[0]}, true
}

// coreIdent is the (package, cluster, core) triple, used only to group the CPUs
// of a topology that publishes no sibling lists.
type coreIdent struct {
	socketID  int
	clusterID int
	coreID    int
}

func coreIdentOf(info CPUInfo) coreIdent {
	return coreIdent{socketID: info.SocketID, clusterID: info.ClusterID, coreID: info.CoreID}
}

// coreLocationByTriple is the identity of the core cpuID's triple names, which
// is the lowest CPU of this topology reporting that same triple.
func (d CPUDetails) coreLocationByTriple(info CPUInfo) CoreLocation {
	want := coreIdentOf(info)
	first := -1
	for cpuID, other := range d {
		if coreIdentOf(other) != want {
			continue
		}
		if first == -1 || cpuID < first {
			first = cpuID
		}
	}
	return CoreLocation{FirstThread: first}
}

// CoreOf returns the physical core cpuID sits on. ok is false when cpuID is not
// part of this CPUDetails.
func (d CPUDetails) CoreOf(cpuID int) (loc CoreLocation, ok bool) {
	info, ok := d[cpuID]
	if !ok {
		return CoreLocation{}, false
	}
	if loc, ok := coreLocationOf(info); ok {
		return loc, true
	}
	return d.coreLocationByTriple(info), true
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
	for cpuID := range d {
		loc, ok := d.CoreOf(cpuID)
		if !ok {
			continue
		}
		if _, ok := wanted[loc]; ok {
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
	for cpuID := range d {
		loc, ok := d.CoreOf(cpuID)
		if !ok {
			continue
		}
		threadsPerCore[loc]++
	}

	present := make(map[CoreLocation][]int, len(threadsPerCore))
	for _, cpuID := range cpus.List() {
		loc, ok := d.CoreOf(cpuID)
		if !ok {
			continue
		}
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

// UniformThreadsPerCore returns how many threads each physical core in cpus has,
// when every one of them has the same count. It returns 0 when the counts differ
// or cpus is empty, so callers that can only act on a uniform core size — such as
// expressing it as a single allocation step — can detect that up front.
//
// Counts are taken from the receiver, which must be the full topology: a core
// only partly present in cpus still has all of its threads.
func (d CPUDetails) UniformThreadsPerCore(cpus cpuset.CPUSet) int {
	threads := 0
	for _, loc := range d.coreLocationsOf(cpus) {
		n := d.CPUsInCoreLocations(loc).Size()
		if threads == 0 {
			threads = n
			continue
		}
		if n != threads {
			return 0
		}
	}
	return threads
}

// coreLocationsOf returns the distinct physical cores the CPUs in cpus occupy,
// skipping CPUs absent from this CPUDetails.
func (d CPUDetails) coreLocationsOf(cpus cpuset.CPUSet) []CoreLocation {
	seen := make(map[CoreLocation]struct{})
	var locs []CoreLocation
	for _, cpuID := range cpus.List() {
		loc, ok := d.CoreOf(cpuID)
		if !ok {
			continue
		}
		if _, dup := seen[loc]; dup {
			continue
		}
		seen[loc] = struct{}{}
		locs = append(locs, loc)
	}
	return locs
}
