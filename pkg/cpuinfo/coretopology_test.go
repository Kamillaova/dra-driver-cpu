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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/cpuset"
)

// repeatingCoreIDs mirrors real multi-socket enumeration: core_id restarts at 0
// on every socket, and the SMT siblings are the high-numbered CPUs.
//
//	socket 0: core 0 -> {0,4}   core 1 -> {1,5}
//	socket 1: core 0 -> {2,6}   core 1 -> {3,7}
var repeatingCoreIDs = CPUDetails{
	0: {CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	1: {CpuID: 1, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	2: {CpuID: 2, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
	3: {CpuID: 3, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
	4: {CpuID: 4, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	5: {CpuID: 5, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	6: {CpuID: 6, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
	7: {CpuID: 7, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
}

// repeatingClusterCoreIDs repeats core_id across clusters within one socket, as
// seen on some ARM parts.
//
//	socket 0 cluster 0: core 0 -> {0,2}
//	socket 0 cluster 1: core 0 -> {1,3}
var repeatingClusterCoreIDs = CPUDetails{
	0: {CpuID: 0, CoreID: 0, ClusterID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	1: {CpuID: 1, CoreID: 0, ClusterID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	2: {CpuID: 2, CoreID: 0, ClusterID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	3: {CpuID: 3, CoreID: 0, ClusterID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
}

// hybridCores has cores of differing thread counts, as on Intel P/E-core parts:
// core 0 is SMT, cores 1 and 2 are single-threaded.
var hybridCores = CPUDetails{
	0: {CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	1: {CpuID: 1, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	2: {CpuID: 2, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
	3: {CpuID: 3, CoreID: 2, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
}

func TestCoreOf(t *testing.T) {
	loc, ok := repeatingCoreIDs.CoreOf(6)
	assert.True(t, ok)
	assert.Equal(t, CoreLocation{SocketID: 1, ClusterID: 0, CoreID: 0}, loc)

	_, ok = repeatingCoreIDs.CoreOf(99)
	assert.False(t, ok, "absent CPU must not report a core")
}

func TestSiblingsOf(t *testing.T) {
	tests := []struct {
		name    string
		details CPUDetails
		cpuID   int
		want    cpuset.CPUSet
	}{
		{
			// The regression this file exists for: CPUsInCores(0) would return
			// {0,2,4,6}, conflating core 0 of both sockets.
			name:    "does not conflate same core id on another socket",
			details: repeatingCoreIDs,
			cpuID:   0,
			want:    cpuset.New(0, 4),
		},
		{
			name:    "other socket's core 0 is its own core",
			details: repeatingCoreIDs,
			cpuID:   2,
			want:    cpuset.New(2, 6),
		},
		{
			name:    "does not conflate same core id in another cluster",
			details: repeatingClusterCoreIDs,
			cpuID:   0,
			want:    cpuset.New(0, 2),
		},
		{
			name:    "single-threaded core is its own only sibling",
			details: hybridCores,
			cpuID:   3,
			want:    cpuset.New(3),
		},
		{
			name:    "absent cpu has no siblings",
			details: repeatingCoreIDs,
			cpuID:   99,
			want:    cpuset.New(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.details.SiblingsOf(tt.cpuID))
		})
	}
}

func TestCPUsInCoreLocations(t *testing.T) {
	got := repeatingCoreIDs.CPUsInCoreLocations(
		CoreLocation{SocketID: 0, CoreID: 1},
		CoreLocation{SocketID: 1, CoreID: 0},
	)
	assert.Equal(t, cpuset.New(1, 5, 2, 6), got)

	assert.Equal(t, cpuset.New(), repeatingCoreIDs.CPUsInCoreLocations(),
		"no locations must select no CPUs")
	assert.Equal(t, cpuset.New(), repeatingCoreIDs.CPUsInCoreLocations(
		CoreLocation{SocketID: 9, CoreID: 9}), "unknown location selects nothing")
}

func TestCompleteCores(t *testing.T) {
	tests := []struct {
		name    string
		details CPUDetails
		cpus    cpuset.CPUSet
		want    cpuset.CPUSet
	}{
		{
			name:    "whole topology is complete",
			details: repeatingCoreIDs,
			cpus:    repeatingCoreIDs.CPUs(),
			want:    repeatingCoreIDs.CPUs(),
		},
		{
			name:    "half a core is dropped",
			details: repeatingCoreIDs,
			cpus:    cpuset.New(0, 1, 5),
			want:    cpuset.New(1, 5),
		},
		{
			name:    "a reserved sibling makes its partner unusable",
			details: repeatingCoreIDs,
			cpus:    repeatingCoreIDs.CPUs().Difference(cpuset.New(4)),
			want:    cpuset.New(1, 5, 2, 6, 3, 7),
		},
		{
			name:    "mixed thread counts each judged on their own core",
			details: hybridCores,
			cpus:    cpuset.New(0, 2, 3),
			want:    cpuset.New(2, 3),
		},
		{
			name:    "cpus outside the topology are dropped",
			details: hybridCores,
			cpus:    cpuset.New(2, 99),
			want:    cpuset.New(2),
		},
		{
			name:    "empty input",
			details: repeatingCoreIDs,
			cpus:    cpuset.New(),
			want:    cpuset.New(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.details.CompleteCores(tt.cpus))
		})
	}
}

func TestCompleteCoresIsIdempotent(t *testing.T) {
	once := repeatingCoreIDs.CompleteCores(cpuset.New(0, 1, 5))
	assert.Equal(t, once, repeatingCoreIDs.CompleteCores(once))
}
