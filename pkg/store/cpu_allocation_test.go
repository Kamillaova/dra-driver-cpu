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

package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

func newTestCPUAllocation(logger logr.Logger, allCPUs, reserved cpuset.CPUSet) *CPUAllocation {
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)
	return NewCPUAllocation(topo, reserved)
}

func requirePreparedAllocation(t testing.TB, logger logr.Logger, store *CPUAllocation, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	require.NoError(t, store.ReserveResourceClaimAllocation(logger, claimUID, cpus, false))
}

func TestCPUAllocationPreparedLifecycle(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	claimUID := types.UID("claim-1")
	claimCPUs := cpuset.New(0, 1)
	store := newTestCPUAllocation(logger, allCPUs, cpuset.New())

	require.NoError(t, store.ReserveResourceClaimAllocation(logger, claimUID, claimCPUs, false))
	require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2, 3)))
	require.Error(t, store.ReserveResourceClaimAllocation(logger, "claim-2", claimCPUs, false))

	union, err := store.GetResourceClaimAllocationUnion(claimUID)
	require.NoError(t, err)
	require.True(t, union.Equals(claimCPUs))
	require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2, 3)))

	store.RemoveResourceClaimAllocation(logger, claimUID)
	require.True(t, store.GetSharedCPUs().Equals(allCPUs))
}

func TestNewCPUAllocation(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	testCases := []struct {
		name               string
		allCPUs            cpuset.CPUSet
		reservedCPUs       cpuset.CPUSet
		expectedAvailable  cpuset.CPUSet
		expectedSharedCPUs cpuset.CPUSet
	}{
		{
			name:               "no reserved cpus",
			allCPUs:            allCPUs,
			reservedCPUs:       cpuset.New(),
			expectedAvailable:  allCPUs,
			expectedSharedCPUs: allCPUs,
		},
		{
			name:               "with reserved cpus",
			allCPUs:            allCPUs,
			reservedCPUs:       cpuset.New(0, 1),
			expectedAvailable:  allCPUs.Difference(cpuset.New(0, 1)),
			expectedSharedCPUs: allCPUs.Difference(cpuset.New(0, 1)),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestCPUAllocation(logger, tc.allCPUs, tc.reservedCPUs)
			require.NotNil(t, store)
			require.True(t, store.availableCPUs.Equals(tc.expectedAvailable))
			require.True(t, store.GetSharedCPUs().Equals(tc.expectedSharedCPUs))
		})
	}
}

func TestCPUAllocationResourceClaimAllocation(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	store := newTestCPUAllocation(logger, allCPUs, cpuset.New())
	claimUID := types.UID("claim-uid-1")
	cpus := cpuset.New(0, 1)

	// Add allocation
	requirePreparedAllocation(t, logger, store, claimUID, cpus)
	gotCPUs, ok := store.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.True(t, cpus.Equals(gotCPUs))

	// Remove allocation
	store.RemoveResourceClaimAllocation(logger, claimUID)
	_, ok = store.GetResourceClaimAllocation(claimUID)
	require.False(t, ok)

	// Remove non-existent allocation
	store.RemoveResourceClaimAllocation(logger, types.UID("non-existent"))
}

func TestCPUAllocationGetSharedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0)
	store := newTestCPUAllocation(logger, allCPUs, reserved)
	available := allCPUs.Difference(reserved)

	// No allocations
	require.True(t, store.GetSharedCPUs().Equals(available))

	// With allocations
	claimUID1 := types.UID("claim-uid-1")
	cpus1 := cpuset.New(1, 2)
	requirePreparedAllocation(t, logger, store, claimUID1, cpus1)
	expectedShared := available.Difference(cpus1)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	claimUID2 := types.UID("claim-uid-2")
	cpus2 := cpuset.New(3, 4)
	requirePreparedAllocation(t, logger, store, claimUID2, cpus2)
	expectedShared = expectedShared.Difference(cpus2)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))
}

func TestReserveResourceClaimAllocationRepeatedCalls(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	testCases := []struct {
		name        string
		firstCPUs   cpuset.CPUSet
		secondCPUs  cpuset.CPUSet
		expectError bool
	}{
		{
			name:        "same cpus repeated",
			firstCPUs:   cpuset.New(0, 1),
			secondCPUs:  cpuset.New(0, 1),
			expectError: false,
		},
		{
			name:        "different cpus repeated",
			firstCPUs:   cpuset.New(0, 1),
			secondCPUs:  cpuset.New(2, 3),
			expectError: true,
		},
		{
			name:        "overlapping cpus repeated",
			firstCPUs:   cpuset.New(0, 1, 2),
			secondCPUs:  cpuset.New(1, 2, 3),
			expectError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestCPUAllocation(logger, allCPUs, cpuset.New())
			claimUID := types.UID("claim-uid-1")

			require.NoError(t, store.ReserveResourceClaimAllocation(logger, claimUID, tc.firstCPUs, false))
			err := store.ReserveResourceClaimAllocation(logger, claimUID, tc.secondCPUs, false)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			gotCPUs, ok := store.GetResourceClaimAllocation(claimUID)
			require.True(t, ok)
			require.True(t, tc.firstCPUs.Equals(gotCPUs), "claim cpus mismatch: got %s, want %s", gotCPUs, tc.firstCPUs)
			require.True(t, allCPUs.Difference(tc.firstCPUs).Equals(store.GetSharedCPUs()))
		})
	}
}

func TestReserveResourceClaimAllocationSharedPoolGuard(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)

	tests := []struct {
		name                string
		cpus                cpuset.CPUSet
		hasSharedContainers bool
		wantErr             bool
	}{
		{
			name:                "rejects exhausting pool with shared containers",
			cpus:                allCPUs,
			hasSharedContainers: true,
			wantErr:             true,
		},
		{
			name:                "allows exhausting pool without shared containers",
			cpus:                allCPUs,
			hasSharedContainers: false,
		},
		{
			name:                "allows reservation that leaves shared CPUs",
			cpus:                cpuset.New(0, 1),
			hasSharedContainers: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestCPUAllocation(logger, allCPUs, cpuset.New())
			err := store.ReserveResourceClaimAllocation(logger, "claim", test.cpus, test.hasSharedContainers)
			if test.wantErr {
				require.ErrorContains(t, err, "would exhaust the shared CPU pool")
				_, ok := store.GetResourceClaimAllocation("claim")
				require.False(t, ok)
				require.True(t, store.GetSharedCPUs().Equals(allCPUs))
			} else {
				require.NoError(t, err)
				require.True(t, store.GetSharedCPUs().Equals(allCPUs.Difference(test.cpus)))
			}
		})
	}
}

func TestCPUAllocationStoreCacheConsistency(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	store := newTestCPUAllocation(logger, allCPUs, cpuset.New())

	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(2, 3))
	requirePreparedAllocation(t, logger, store, "claim-3", cpuset.New(4, 5))

	expectedShared := cpuset.New(6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.RemoveResourceClaimAllocation(logger, "claim-2")
	expectedShared = cpuset.New(2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.RemoveResourceClaimAllocation(logger, "claim-1")
	expectedShared = cpuset.New(0, 1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.RemoveResourceClaimAllocation(logger, "claim-3")
	require.True(t, store.GetSharedCPUs().Equals(allCPUs))
}

func TestCPUAllocationGetReservedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0, 1)
	store := newTestCPUAllocation(logger, allCPUs, reserved)
	require.True(t, store.GetReservedCPUs().Equals(reserved))
}

func TestCPUAllocationGetPreparedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	store := newTestCPUAllocation(logger, allCPUs, cpuset.New())

	require.True(t, store.GetPreparedCPUs().IsEmpty())

	claimUID := types.UID("claim-1")
	cpus := cpuset.New(2, 3)
	requirePreparedAllocation(t, logger, store, claimUID, cpus)
	require.True(t, store.GetPreparedCPUs().Equals(cpus))

	store.RemoveResourceClaimAllocation(logger, claimUID)
	require.True(t, store.GetPreparedCPUs().IsEmpty())
}

func TestCPUAllocationSnapshot(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0, 1)
	store := newTestCPUAllocation(logger, allCPUs, reserved)

	require.Equal(t, AllocationSnapshot{
		AllocatedCPUs:        0,
		AvailableCPUs:        6,
		ReservedCPUs:         2,
		ActiveResourceClaims: 0,
	}, store.Snapshot())

	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(2, 3))
	require.Equal(t, AllocationSnapshot{
		AllocatedCPUs:        2,
		AvailableCPUs:        4,
		ReservedCPUs:         2,
		ActiveResourceClaims: 1,
	}, store.Snapshot())

	store.RemoveResourceClaimAllocation(logger, "claim-1")
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(4, 5, 6))
	require.Equal(t, AllocationSnapshot{
		AllocatedCPUs:        3,
		AvailableCPUs:        3,
		ReservedCPUs:         2,
		ActiveResourceClaims: 1,
	}, store.Snapshot())

	requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(2, 3))
	require.Equal(t, AllocationSnapshot{
		AllocatedCPUs:        5,
		AvailableCPUs:        1,
		ReservedCPUs:         2,
		ActiveResourceClaims: 2,
	}, store.Snapshot())

	store.RemoveResourceClaimAllocation(logger, "claim-1")
	require.Equal(t, AllocationSnapshot{
		AllocatedCPUs:        2,
		AvailableCPUs:        4,
		ReservedCPUs:         2,
		ActiveResourceClaims: 1,
	}, store.Snapshot())
}

func getSharedCPUsNaive(availableCPUs cpuset.CPUSet, allocations map[types.UID]cpuset.CPUSet) cpuset.CPUSet {
	allocated := cpuset.New()
	for _, cpus := range allocations {
		allocated = allocated.Union(cpus)
	}
	return availableCPUs.Difference(allocated)
}

func BenchmarkGetSharedCPUs(b *testing.B) {
	testCases := []struct {
		name           string
		numCPUs        int
		numAllocations int
	}{
		{"10_allocations", 128, 10},
		{"100_allocations", 128, 100},
		{"500_allocations", 1024, 500},
	}

	for _, tc := range testCases {
		var infos []cpuinfo.CPUInfo
		for i := 0; i < tc.numCPUs; i++ {
			infos = append(infos, cpuinfo.CPUInfo{CpuID: i, CoreID: i, SocketID: 0, NUMANodeID: 0})
		}
		mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
		topo, _ := mockProvider.GetCPUTopology(logr.Discard())

		allocations := make(map[types.UID]cpuset.CPUSet)
		for i := 0; i < tc.numAllocations && i*2+1 < tc.numCPUs; i++ {
			allocations[types.UID(fmt.Sprintf("claim-%d", i))] = cpuset.New(i*2, i*2+1)
		}

		cpuIDs := make([]int, 0, tc.numCPUs)
		for i := 0; i < tc.numCPUs; i++ {
			cpuIDs = append(cpuIDs, i)
		}
		availableCPUs := cpuset.New(cpuIDs...)

		b.Run(tc.name+"/naive", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = getSharedCPUsNaive(availableCPUs, allocations)
			}
		})

		b.Run(tc.name+"/optimized", func(b *testing.B) {
			store := NewCPUAllocation(topo, cpuset.New())
			for claimUID, cpus := range allocations {
				requirePreparedAllocation(b, logr.Discard(), store, claimUID, cpus)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = store.GetSharedCPUs()
			}
		})
	}
}

func TestGetResourceClaimAllocationUnion(t *testing.T) {
	logger := testr.New(t)
	store := newTestCPUAllocation(logger, cpuset.New(0, 1, 2, 3, 4, 5), cpuset.New())
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(4))

	testCases := []struct {
		name          string
		claimUIDs     []types.UID
		expected      cpuset.CPUSet
		expectedError string
	}{
		{
			name:      "no claims",
			claimUIDs: nil,
			expected:  cpuset.New(),
		},
		{
			name:      "single claim",
			claimUIDs: []types.UID{"claim-1"},
			expected:  cpuset.New(0, 1),
		},
		{
			name:      "a container holding several claims gets all of their CPUs",
			claimUIDs: []types.UID{"claim-1", "claim-2"},
			expected:  cpuset.New(0, 1, 4),
		},
		{
			name:          "unprepared claim",
			claimUIDs:     []types.UID{"claim-absent"},
			expectedError: `claim "claim-absent" is not prepared by this driver`,
		},
		{
			// Partial results would pin a container to fewer CPUs than it holds.
			name:          "one unprepared claim rejects the whole set",
			claimUIDs:     []types.UID{"claim-1", "claim-absent"},
			expectedError: `claim "claim-absent" is not prepared by this driver`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.GetResourceClaimAllocationUnion(tc.claimUIDs...)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				require.True(t, got.IsEmpty())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestRebindLifecycle(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	claimUID := types.UID("claim-1")

	testCases := []struct {
		name string
		// target of the rebind begun on claimUID, which starts on 0-1.
		target cpuset.CPUSet
		// commit, otherwise abort.
		commit           bool
		expectedCPUs     cpuset.CPUSet
		expectedShared   cpuset.CPUSet
		expectedInFlight cpuset.CPUSet
	}{
		{
			name:             "commit keeps the target and releases the origin",
			target:           cpuset.New(2, 3),
			commit:           true,
			expectedCPUs:     cpuset.New(2, 3),
			expectedShared:   cpuset.New(0, 1, 4, 5, 6, 7),
			expectedInFlight: cpuset.New(4, 5, 6, 7),
		},
		{
			name:             "abort keeps the origin and releases the target",
			target:           cpuset.New(2, 3),
			commit:           false,
			expectedCPUs:     cpuset.New(0, 1),
			expectedShared:   cpuset.New(2, 3, 4, 5, 6, 7),
			expectedInFlight: cpuset.New(4, 5, 6, 7),
		},
		{
			// The halves overlap, so only the CPUs actually left behind may be
			// released -- CPU 1 belongs to the claim before and after.
			name:             "commit of an overlapping move releases only the CPUs left behind",
			target:           cpuset.New(1, 2),
			commit:           true,
			expectedCPUs:     cpuset.New(1, 2),
			expectedShared:   cpuset.New(0, 3, 4, 5, 6, 7),
			expectedInFlight: cpuset.New(3, 4, 5, 6, 7),
		},
		{
			name:             "abort of an overlapping move releases only the CPUs not yet held",
			target:           cpuset.New(1, 2),
			commit:           false,
			expectedCPUs:     cpuset.New(0, 1),
			expectedShared:   cpuset.New(2, 3, 4, 5, 6, 7),
			expectedInFlight: cpuset.New(3, 4, 5, 6, 7),
		},
		{
			name:             "a move to the same CPUs commits without releasing anything",
			target:           cpuset.New(0, 1),
			commit:           true,
			expectedCPUs:     cpuset.New(0, 1),
			expectedShared:   cpuset.New(2, 3, 4, 5, 6, 7),
			expectedInFlight: cpuset.New(2, 3, 4, 5, 6, 7),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestCPUAllocation(logger, allCPUs, cpuset.New())
			requirePreparedAllocation(t, logger, store, claimUID, cpuset.New(0, 1))

			require.NoError(t, store.BeginRebind(logger, claimUID, tc.target))

			// While in flight the claim holds both halves, so neither is offered
			// to a shared container or to another claim.
			require.Equal(t, tc.expectedInFlight, store.GetSharedCPUs())
			origin, ok := store.GetRebindOrigin(claimUID)
			require.True(t, ok)
			require.Equal(t, cpuset.New(0, 1), origin)
			current, ok := store.GetResourceClaimAllocation(claimUID)
			require.True(t, ok)
			require.Equal(t, tc.target, current, "the claim reads as being on its target while in flight")

			if tc.commit {
				require.NoError(t, store.CommitRebind(logger, claimUID))
			} else {
				require.NoError(t, store.AbortRebind(logger, claimUID))
			}

			got, ok := store.GetResourceClaimAllocation(claimUID)
			require.True(t, ok)
			require.Equal(t, tc.expectedCPUs, got)
			require.Equal(t, tc.expectedShared, store.GetSharedCPUs())
			require.Equal(t, tc.expectedCPUs, store.GetPreparedCPUs())

			_, ok = store.GetRebindOrigin(claimUID)
			require.False(t, ok, "the rebind must no longer be in flight")
		})
	}
}

func TestBeginRebindRejections(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)

	testCases := []struct {
		name          string
		claimUID      types.UID
		target        cpuset.CPUSet
		beginFirst    cpuset.CPUSet
		expectedError string
	}{
		{
			name:          "unprepared claim",
			claimUID:      "claim-absent",
			target:        cpuset.New(4, 5),
			expectedError: `claim "claim-absent" is not prepared by this driver`,
		},
		{
			name:          "growing the claim",
			claimUID:      "claim-1",
			target:        cpuset.New(4, 5, 6),
			expectedError: `rebind of claim "claim-1" would change its CPU count from 2 to 3`,
		},
		{
			name:          "shrinking the claim",
			claimUID:      "claim-1",
			target:        cpuset.New(4),
			expectedError: `rebind of claim "claim-1" would change its CPU count from 2 to 1`,
		},
		{
			name:          "onto another claim's CPUs",
			claimUID:      "claim-1",
			target:        cpuset.New(2, 3),
			expectedError: `rebind target "2-3" for claim "claim-1" is not free`,
		},
		{
			name:          "partly onto another claim's CPUs",
			claimUID:      "claim-1",
			target:        cpuset.New(3, 4),
			expectedError: `rebind target "3-4" for claim "claim-1" is not free`,
		},
		{
			name:          "onto reserved CPUs",
			claimUID:      "claim-1",
			target:        cpuset.New(6, 7),
			expectedError: `rebind target "6-7" for claim "claim-1" is not free`,
		},
		{
			// The origin would be lost, leaving the abort path with nothing to
			// fall back to.
			name:          "while a rebind is already in flight",
			claimUID:      "claim-1",
			beginFirst:    cpuset.New(4, 5),
			target:        cpuset.New(0, 1),
			expectedError: `claim "claim-1" is already rebinding from "0-1" to "4-5"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestCPUAllocation(logger, allCPUs, cpuset.New(6, 7))
			requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
			requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(2, 3))
			sharedBefore := store.GetSharedCPUs()

			if !tc.beginFirst.IsEmpty() {
				require.NoError(t, store.BeginRebind(logger, tc.claimUID, tc.beginFirst))
				sharedBefore = store.GetSharedCPUs()
			}

			err := store.BeginRebind(logger, tc.claimUID, tc.target)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedError)
			require.Equal(t, sharedBefore, store.GetSharedCPUs(), "a rejected rebind must not change accounting")
		})
	}
}

func TestCommitAndAbortRebindWithoutBegin(t *testing.T) {
	logger := testr.New(t)
	store := newTestCPUAllocation(logger, cpuset.New(0, 1, 2, 3), cpuset.New())
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))

	require.EqualError(t, store.CommitRebind(logger, "claim-1"), `claim "claim-1" has no rebind in flight`)
	require.EqualError(t, store.AbortRebind(logger, "claim-1"), `claim "claim-1" has no rebind in flight`)
	require.EqualError(t, store.CommitRebind(logger, "claim-absent"), `claim "claim-absent" has no rebind in flight`)
	require.EqualError(t, store.AbortRebind(logger, "claim-absent"), `claim "claim-absent" has no rebind in flight`)

	// A second commit must not release the origin twice.
	require.NoError(t, store.BeginRebind(logger, "claim-1", cpuset.New(2, 3)))
	require.NoError(t, store.CommitRebind(logger, "claim-1"))
	require.EqualError(t, store.CommitRebind(logger, "claim-1"), `claim "claim-1" has no rebind in flight`)
	require.Equal(t, cpuset.New(0, 1), store.GetSharedCPUs())
}

func TestReserveIsBlockedByBothHalvesOfARebind(t *testing.T) {
	logger := testr.New(t)
	store := newTestCPUAllocation(logger, cpuset.New(0, 1, 2, 3, 4, 5), cpuset.New())
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	require.NoError(t, store.BeginRebind(logger, "claim-1", cpuset.New(2, 3)))

	require.Error(t, store.ReserveResourceClaimAllocation(logger, "claim-2", cpuset.New(0), false),
		"the CPUs the container still runs on are not available")
	require.Error(t, store.ReserveResourceClaimAllocation(logger, "claim-2", cpuset.New(3), false),
		"the CPUs the claim is moving onto are not available")
	require.NoError(t, store.ReserveResourceClaimAllocation(logger, "claim-2", cpuset.New(4, 5), false))
}

func TestRemoveDuringRebindReleasesBothHalves(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	store := newTestCPUAllocation(logger, allCPUs, cpuset.New())
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	require.NoError(t, store.BeginRebind(logger, "claim-1", cpuset.New(2, 3)))

	store.RemoveResourceClaimAllocation(logger, "claim-1")

	require.Equal(t, allCPUs, store.GetSharedCPUs())
	require.True(t, store.GetPreparedCPUs().IsEmpty())
	_, ok := store.GetRebindOrigin("claim-1")
	require.False(t, ok)
	require.Equal(t, 0, store.Snapshot().ActiveResourceClaims)
}

func TestConcurrentRebindsAndReserves(t *testing.T) {
	logger := testr.New(t)
	store := newTestCPUAllocation(logger, cpuset.New(0, 1, 2, 3, 4, 5, 6, 7), cpuset.New())
	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))

	// Everyone contends for the same two CPUs: claim-1 wants to move onto them,
	// and other claims want to be prepared on them. Whoever wins, the CPUs must
	// end up with exactly one owner.
	contested := cpuset.New(2, 3)
	const racers = 8
	var wg sync.WaitGroup
	var winners atomic.Int64

	for i := range racers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if store.BeginRebind(logger, "claim-1", contested) == nil {
				winners.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			if store.ReserveResourceClaimAllocation(logger, types.UID(fmt.Sprintf("claim-r%d", i)), contested, false) == nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), winners.Load(), "exactly one claim may take the contested CPUs")
	// Held either way: 0-1 by claim-1, and 2-3 by whichever racer won.
	require.Equal(t, cpuset.New(4, 5, 6, 7), store.GetSharedCPUs())
}

func TestResourceClaimAllocations(t *testing.T) {
	logger := testr.New(t)
	store := newTestCPUAllocation(logger, cpuset.New(0, 1, 2, 3, 4, 5), cpuset.New())
	require.Empty(t, store.ResourceClaimAllocations())

	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(2))
	require.Equal(t, map[types.UID]cpuset.CPUSet{
		"claim-1": cpuset.New(0, 1),
		"claim-2": cpuset.New(2),
	}, store.ResourceClaimAllocations())

	// The caller's copy must not be the store's own map.
	got := store.ResourceClaimAllocations()
	delete(got, "claim-1")
	require.Len(t, store.ResourceClaimAllocations(), 2)

	// A claim being moved reads as being on its target, matching the
	// single-claim getter.
	require.NoError(t, store.BeginRebind(logger, "claim-1", cpuset.New(4, 5)))
	require.Equal(t, cpuset.New(4, 5), store.ResourceClaimAllocations()["claim-1"])

	store.RemoveResourceClaimAllocation(logger, "claim-2")
	require.Len(t, store.ResourceClaimAllocations(), 1)
}
