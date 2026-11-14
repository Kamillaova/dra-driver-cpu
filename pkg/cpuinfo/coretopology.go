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

package cpuinfo

import (
	"k8s.io/utils/cpuset"
)

// CoreLocation identifies a physical core by the lowest CPU of its sibling list.
// That CPU may be outside the set of CPUs a caller reasons about.
type CoreLocation struct {
	FirstThread int
}

func coreLocationOf(info CPUInfo) (CoreLocation, bool) {
	if info.SiblingCPUSet.IsEmpty() {
		return CoreLocation{}, false
	}
	return CoreLocation{FirstThread: info.SiblingCPUSet.List()[0]}, true
}

type coreIdent struct {
	socketID  int
	clusterID int
	coreID    int
}

func coreIdentOf(info CPUInfo) coreIdent {
	return coreIdent{socketID: info.SocketID, clusterID: info.ClusterID, coreID: info.CoreID}
}

// coreLocationByTriple names the core of a CPU that carries no sibling list by
// the lowest CPU reporting the same (socket, cluster, core) triple.
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

// CoreOf returns the physical core cpuID sits on; ok is false when cpuID is not in d.
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

// CPUsInCoreLocations returns the CPUs of d on the given physical cores.
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

// SiblingsOf returns the CPUs of d on cpuID's physical core, cpuID included.
func (d CPUDetails) SiblingsOf(cpuID int) cpuset.CPUSet {
	loc, ok := d.CoreOf(cpuID)
	if !ok {
		return cpuset.New()
	}
	return d.CPUsInCoreLocations(loc)
}

// CompleteCores returns the CPUs of cpus whose core has every one of its CPUs in
// d inside cpus. d must hold the whole topology: a core cut short in d counts as
// complete.
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

// UniformThreadsPerCore returns the number of CPUs of d on each core that cpus
// touches when every such core has the same number, and 0 when they differ or
// cpus is empty. A core only partly in cpus counts all of its CPUs in d.
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
