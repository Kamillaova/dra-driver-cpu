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

// Package coreselect takes CPUs a whole physical core at a time.
//
// It is not part of pkg/cpumanager, which is a copy of the kubelet's allocator,
// whose core helpers match on core_id alone and so take same-numbered cores of
// different sockets for one core.
package coreselect

import (
	"fmt"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"k8s.io/utils/cpuset"
)

// NoFreeSingleThreadCoreError reports an odd number of CPUs asked of CPUs whose
// free whole cores all have two threads.
type NoFreeSingleThreadCoreError struct {
	CPUs               int
	FreeTwoThreadCores int
}

func (e *NoFreeSingleThreadCoreError) Error() string {
	return fmt.Sprintf("%d CPUs need a free core of 1 thread, and none is free (%d free cores of 2 threads)",
		e.CPUs, e.FreeTwoThreadCores)
}

// NotEnoughWholeCoresError reports more CPUs asked than free whole cores hold.
type NotEnoughWholeCoresError struct {
	Requested int
	Available int
}

func (e *NotEnoughWholeCoresError) Error() string {
	return fmt.Sprintf("not enough whole cores available to satisfy request: requested=%d, available in whole cores=%d",
		e.Requested, e.Available)
}

// TakeWholeCores returns numCPUs CPUs of available as whole physical cores, so
// no core is split between the result and anything else. A core whose CPUs are
// not all in available is not taken. Cores of two threads are taken before cores
// of one, and the cores come from as few uncore caches as the amount allows.
func TakeWholeCores(topo *cpuinfo.CPUTopology, available cpuset.CPUSet, numCPUs int) (cpuset.CPUSet, error) {
	if numCPUs == 0 {
		return cpuset.New(), nil
	}
	if numCPUs < 0 {
		return cpuset.New(), fmt.Errorf("cannot take a negative number of CPUs (%d)", numCPUs)
	}

	caches, err := freeCoresByCache(topo.CPUDetails, available)
	if err != nil {
		return cpuset.New(), err
	}
	twoThread, oneThread := 0, 0
	for _, c := range caches {
		twoThread += len(c.twoThread)
		oneThread += len(c.oneThread)
	}
	if numCPUs > 2*twoThread+oneThread {
		return cpuset.New(), &NotEnoughWholeCoresError{Requested: numCPUs, Available: 2*twoThread + oneThread}
	}
	if numCPUs%2 == 1 && oneThread == 0 {
		return cpuset.New(), &NoFreeSingleThreadCoreError{CPUs: numCPUs, FreeTwoThreadCores: twoThread}
	}

	// Two-thread cores first, counted over all the caches: takes with no release
	// between them then fit in any order or in none, where one-thread cores first
	// can strand a later odd amount.
	needTwo := min(twoThread, numCPUs/2)
	needOne := numCPUs - 2*needTwo
	result := cpuset.New()
	for needTwo > 0 || needOne > 0 {
		c := pickCache(caches, needTwo, needOne)
		if c == nil {
			return cpuset.New(), fmt.Errorf("failed to take %d CPUs as whole cores: %d cores of 2 threads and %d of 1 still needed",
				numCPUs, needTwo, needOne)
		}
		takeTwo := min(needTwo, len(c.twoThread))
		takeOne := min(needOne, len(c.oneThread))
		for _, core := range c.twoThread[:takeTwo] {
			result = result.Union(core)
		}
		for _, core := range c.oneThread[:takeOne] {
			result = result.Union(core)
		}
		c.twoThread = c.twoThread[takeTwo:]
		c.oneThread = c.oneThread[takeOne:]
		needTwo -= takeTwo
		needOne -= takeOne
	}
	return result, nil
}

type cacheCores struct {
	id        int
	twoThread []cpuset.CPUSet
	oneThread []cpuset.CPUSet
}

func freeCoresByCache(details cpuinfo.CPUDetails, available cpuset.CPUSet) ([]*cacheCores, error) {
	usable := details.CompleteCores(available)
	byID := make(map[int]*cacheCores)
	var caches []*cacheCores
	seen := make(map[cpuinfo.CoreLocation]bool)
	for _, cpuID := range usable.List() {
		loc, ok := details.CoreOf(cpuID)
		if !ok || seen[loc] {
			continue
		}
		seen[loc] = true
		cacheID := details[cpuID].UncoreCacheID
		c, ok := byID[cacheID]
		if !ok {
			c = &cacheCores{id: cacheID}
			byID[cacheID] = c
			caches = append(caches, c)
		}
		core := details.CPUsInCoreLocations(loc)
		switch core.Size() {
		case 2:
			c.twoThread = append(c.twoThread, core)
		case 1:
			c.oneThread = append(c.oneThread, core)
		default:
			return nil, fmt.Errorf("core of CPU %d has %d threads, where whole cores of 1 or 2 threads are supported", cpuID, core.Size())
		}
	}
	return caches, nil
}

// pickCache returns the cache to take the next cores from: of the caches that
// hold every core still needed, the one with the fewest free CPUs, which fills a
// used cache before a clean one; when none holds them all, the one that gives
// the most. Ties go to the lower cache id. A cache holding no core still needed
// is never picked.
func pickCache(caches []*cacheCores, needTwo, needOne int) *cacheCores {
	var holder, largest *cacheCores
	holderFree, largestGives := 0, 0
	for _, c := range caches {
		gives := 2*min(needTwo, len(c.twoThread)) + min(needOne, len(c.oneThread))
		if gives == 0 {
			continue
		}
		free := 2*len(c.twoThread) + len(c.oneThread)
		if len(c.twoThread) >= needTwo && len(c.oneThread) >= needOne {
			if holder == nil || free < holderFree || (free == holderFree && c.id < holder.id) {
				holder, holderFree = c, free
			}
		}
		if largest == nil || gives > largestGives || (gives == largestGives && c.id < largest.id) {
			largest, largestGives = c, gives
		}
	}
	if holder != nil {
		return holder
	}
	return largest
}
