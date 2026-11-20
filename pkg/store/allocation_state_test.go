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
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

func newTestAllocationState(logger logr.Logger, allCPUs, reserved cpuset.CPUSet) *AllocationState {
	var infos []cpuinfo.CPUInfo
	for _, cpuID := range allCPUs.UnsortedList() {
		infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
	}
	mockProvider := &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}
	topo, _ := mockProvider.GetCPUTopology(logger)
	return NewAllocationState(topo, reserved)
}

func oneRequest(cpus cpuset.CPUSet) ClaimRecord {
	return ClaimRecord{Requests: []RequestAllocation{{Request: "cpus", CPUs: cpus}}}
}

func wholeClaims(cpus map[types.UID]cpuset.CPUSet) map[ClaimRequestRef]cpuset.CPUSet {
	expected := make(map[ClaimRequestRef]cpuset.CPUSet, len(cpus))
	for claimUID, claimCPUs := range cpus {
		expected[ClaimRequestRef{ClaimUID: claimUID}] = claimCPUs
	}
	return expected
}

func requirePreparedAllocation(t testing.TB, logger logr.Logger, store *AllocationState, claimUID types.UID, cpus cpuset.CPUSet) {
	t.Helper()
	require.NoError(t, store.Reserve(logger, claimUID, oneRequest(cpus), false))
}

func TestAllocationStatePreparedLifecycle(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)
	claimUID := types.UID("claim-1")
	claimCPUs := cpuset.New(0, 1)
	store := newTestAllocationState(logger, allCPUs, cpuset.New())

	require.NoError(t, store.Reserve(logger, claimUID, oneRequest(claimCPUs), false))
	require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2, 3)))
	require.Error(t, store.Reserve(logger, "claim-2", oneRequest(claimCPUs), false))

	updatedClaims, err := store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{claimUID: claimCPUs}))
	require.NoError(t, err)
	require.Equal(t, []types.UID{claimUID}, updatedClaims)
	require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2, 3)))

	store.Release(logger, claimUID)
	require.False(t, store.IsOwner(claimUID, "pod", "app"))
	require.True(t, store.GetSharedCPUs().Equals(allCPUs))
}

func TestAllocationStateBind(t *testing.T) {
	owner := OwnerIdent{PodUID: "pod", ContainerName: "app"}
	allocations := map[types.UID]cpuset.CPUSet{"a": cpuset.New(0), "b": cpuset.New(1)}
	tests := []struct {
		name              string
		initial           *OwnerIdent // Initial owner of claim b.
		expected          map[types.UID]cpuset.CPUSet
		wantUpdatedClaims []types.UID
		wantErrIs         error
	}{
		{
			name:      "empty claim list",
			wantErrIs: ErrEmptyClaimList,
		},
		{
			name:              "first binding",
			expected:          allocations,
			wantUpdatedClaims: []types.UID{"a", "b"},
		},
		{
			name:     "same owner",
			initial:  &owner,
			expected: map[types.UID]cpuset.CPUSet{"b": cpuset.New(1)},
		},
		{
			name:              "mixed new and existing owners",
			initial:           &owner,
			expected:          allocations,
			wantUpdatedClaims: []types.UID{"a"},
		},
		{
			name:      "pod conflict",
			initial:   &OwnerIdent{PodUID: "other", ContainerName: "app"},
			expected:  allocations,
			wantErrIs: ErrClaimAlreadyBound,
		},
		{
			name:      "container conflict",
			initial:   &OwnerIdent{PodUID: "pod", ContainerName: "other"},
			expected:  allocations,
			wantErrIs: ErrClaimAlreadyBound,
		},
		{
			name:      "missing claim",
			expected:  map[types.UID]cpuset.CPUSet{"a": cpuset.New(0), "missing": cpuset.New(2)},
			wantErrIs: ErrUnknownClaim,
		},
		{
			name:      "CPU mismatch",
			expected:  map[types.UID]cpuset.CPUSet{"a": cpuset.New(0), "b": cpuset.New(2)},
			wantErrIs: ErrClaimCPUMismatch,
		},
		{
			name:      "CPU mismatch for existing owner",
			initial:   &owner,
			expected:  map[types.UID]cpuset.CPUSet{"a": cpuset.New(0), "b": cpuset.New(2)},
			wantErrIs: ErrClaimCPUMismatch,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := testr.New(t)
			store := newTestAllocationState(logger, cpuset.New(0, 1, 2), cpuset.New())
			for uid, cpus := range allocations {
				requirePreparedAllocation(t, logger, store, uid, cpus)
			}
			if tc.initial != nil {
				_, err := store.Bind(logger, tc.initial.PodUID, tc.initial.ContainerName, nil, wholeClaims(map[types.UID]cpuset.CPUSet{"b": cpuset.New(1)}))
				require.NoError(t, err)
			}
			before := store.Clone()
			updatedClaims, err := store.Bind(logger, owner.PodUID, owner.ContainerName, nil, wholeClaims(tc.expected))
			if tc.wantErrIs != nil {
				require.ErrorIs(t, err, tc.wantErrIs)
				require.Equal(t, before.resourceClaimAllocations, store.resourceClaimAllocations, "failed validation must not change any claim")
			} else {
				require.NoError(t, err)
				for uid := range tc.expected {
					require.True(t, store.IsOwner(uid, owner.PodUID, owner.ContainerName))
				}
			}
			require.ElementsMatch(t, tc.wantUpdatedClaims, updatedClaims)
			require.Equal(t, before.Snapshot(), store.Snapshot())
			// Unbind clears only updated claims, preserving prior owners and CPU reservations.
			store.Unbind(logger, append(updatedClaims, "missing")...)
			require.Equal(t, before.resourceClaimAllocations, store.resourceClaimAllocations)
			require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2)))
		})
	}
}

func TestAllocationStateBindRequests(t *testing.T) {
	vcpus := ClaimRequestRef{ClaimUID: "vm", Request: "vcpus"}
	helpers := ClaimRequestRef{ClaimUID: "vm", Request: "helpers"}
	tests := []struct {
		name              string
		named             []ClaimRequestRef
		expected          map[ClaimRequestRef]cpuset.CPUSet
		wantUpdatedClaims []types.UID
		wantErr           string
	}{
		{
			name:              "requests named without CPUs to compare bind their claim once",
			named:             []ClaimRequestRef{vcpus, helpers},
			wantUpdatedClaims: []types.UID{"vm"},
		},
		{
			name:    "a named request the claim was prepared without",
			named:   []ClaimRequestRef{vcpus, {ClaimUID: "vm", Request: "gpus"}},
			wantErr: `claim "vm" was prepared without request "gpus"`,
		},
		{
			name:     "a named request of a claim not prepared",
			named:    []ClaimRequestRef{{ClaimUID: "other", Request: "vcpus"}},
			wantErr:  `claim "other" is not prepared by this driver`,
			expected: map[ClaimRequestRef]cpuset.CPUSet{vcpus: cpuset.New(0, 1)},
		},
		{
			name:              "one request binds its claim",
			expected:          map[ClaimRequestRef]cpuset.CPUSet{vcpus: cpuset.New(0, 1)},
			wantUpdatedClaims: []types.UID{"vm"},
		},
		{
			name:              "every request binds the claim once",
			expected:          map[ClaimRequestRef]cpuset.CPUSet{vcpus: cpuset.New(0, 1), helpers: cpuset.New(2)},
			wantUpdatedClaims: []types.UID{"vm"},
		},
		{
			name:              "the whole claim is the union of its requests",
			expected:          map[ClaimRequestRef]cpuset.CPUSet{{ClaimUID: "vm"}: cpuset.New(0, 1, 2)},
			wantUpdatedClaims: []types.UID{"vm"},
		},
		{
			name:     "a request compared with another request's CPUs",
			expected: map[ClaimRequestRef]cpuset.CPUSet{vcpus: cpuset.New(0, 1), helpers: cpuset.New(0)},
			wantErr:  `validation failed for claim "vm" request "helpers": cpuset mismatch (expected "2", got "0")`,
		},
		{
			name:     "the whole claim compared with one request's CPUs",
			expected: map[ClaimRequestRef]cpuset.CPUSet{{ClaimUID: "vm"}: cpuset.New(0, 1)},
			wantErr:  `validation failed for claim "vm": cpuset mismatch (expected "0-2", got "0-1")`,
		},
		{
			name:     "a request the claim was prepared without",
			expected: map[ClaimRequestRef]cpuset.CPUSet{{ClaimUID: "vm", Request: "gpus"}: cpuset.New(3)},
			wantErr:  `claim "vm" was prepared without request "gpus"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := testr.New(t)
			store := newTestAllocationState(logger, cpuset.New(0, 1, 2, 3), cpuset.New())
			require.NoError(t, store.Reserve(logger, "vm", ClaimRecord{Requests: []RequestAllocation{
				{Request: "vcpus", CPUs: cpuset.New(0, 1)},
				{Request: "helpers", CPUs: cpuset.New(2)},
			}}, false))
			before := store.Clone()
			updatedClaims, err := store.Bind(logger, "pod", "app", tc.named, tc.expected)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Equal(t, before.resourceClaimAllocations, store.resourceClaimAllocations, "failed validation must not change any claim")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantUpdatedClaims, updatedClaims)
			require.True(t, store.IsOwner("vm", "pod", "app"))
		})
	}
}

func TestAllocationStateRequestsSurviveBindAndUnbind(t *testing.T) {
	logger := testr.New(t)
	store := newTestAllocationState(logger, cpuset.New(0, 1, 2, 3), cpuset.New())
	requests := []RequestAllocation{
		{Request: "vcpus", CPUs: cpuset.New(0, 1), Group: "vcpu"},
		{Request: "helpers", CPUs: cpuset.New(2), Group: "vcpu"},
		{Request: "housekeeping", CPUs: cpuset.New(3), Role: RoleShared, Devices: []string{"cpudevnuma000"}, Cap: 250, Amount: 100, Group: "vmm-cpu"},
	}
	layout := &Layout{Place: "vmm-cpu"}
	require.NoError(t, store.Reserve(logger, "vm", ClaimRecord{Namespace: "ns", Name: "vm-claim", Requests: requests, Layout: layout}, false))
	layout.Place = "changed"
	byRequest := map[ClaimRequestRef]cpuset.CPUSet{
		{ClaimUID: "vm", Request: "vcpus"}:   cpuset.New(0, 1),
		{ClaimUID: "vm", Request: "helpers"}: cpuset.New(2),
	}
	recorded := func(s *AllocationState) ClaimRecord {
		record, ok := s.GetClaimRecord("vm")
		require.True(t, ok)
		return record
	}
	want := ClaimRecord{Namespace: "ns", Name: "vm-claim", Requests: []RequestAllocation{requests[1], requests[2], requests[0]}, Layout: &Layout{Place: "vmm-cpu"}}
	require.Equal(t, want, recorded(store), "the record reads back sorted by request, its layout copied")
	recorded(store).Layout.Place = "changed"
	require.Equal(t, want, recorded(store), "a record read back is a copy")

	_, err := store.Bind(logger, "pod", "app", nil, byRequest)
	require.NoError(t, err)
	require.Equal(t, want, recorded(store), "Acquire keeps the record")
	require.Equal(t, want, recorded(store.Clone()), "Clone keeps the record")

	store.Unbind(logger, "vm")
	require.Equal(t, want, recorded(store), "Release keeps the record")
	_, err = store.Bind(logger, "pod", "other", nil, byRequest)
	require.NoError(t, err, "each request reads back its own CPUs after an unbind")
	require.Equal(t, "0-2", store.GetPreparedCPUs().String())
}

func TestAllocationStateBindErrorDetails(t *testing.T) {
	logger := testr.New(t)
	store := newTestAllocationState(logger, cpuset.New(0, 1, 2), cpuset.New())
	requirePreparedAllocation(t, logger, store, "a", cpuset.New(0))

	_, err := store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"missing": cpuset.New(2)}))
	var unknownClaimErr UnknownClaimError
	require.ErrorAs(t, err, &unknownClaimErr)
	require.Equal(t, types.UID("missing"), unknownClaimErr.ClaimUID)

	_, err = store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"a": cpuset.New(2)}))
	var mismatchErr ClaimCPUMismatchError
	require.ErrorAs(t, err, &mismatchErr)
	require.Equal(t, types.UID("a"), mismatchErr.ClaimUID)
	require.True(t, mismatchErr.Expected.Equals(cpuset.New(0)))
	require.True(t, mismatchErr.Got.Equals(cpuset.New(2)))

	_, err = store.Bind(logger, "pod", "app", []ClaimRequestRef{{ClaimUID: "a", Request: "missing"}}, nil)
	var unknownRequestErr UnknownRequestError
	require.ErrorAs(t, err, &unknownRequestErr)
	require.Equal(t, UnknownRequestError{ClaimUID: "a", Request: "missing"}, unknownRequestErr)
	require.ErrorIs(t, err, ErrUnknownRequest)
}

func TestAllocationStateCloneRetainsOwnership(t *testing.T) {
	logger := testr.New(t)
	store := newTestAllocationState(logger, cpuset.New(0, 1, 2), cpuset.New())
	requirePreparedAllocation(t, logger, store, "a", cpuset.New(0))
	requirePreparedAllocation(t, logger, store, "b", cpuset.New(1))
	_, err := store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)}))
	require.NoError(t, err)
	require.True(t, store.IsOwner("a", "pod", "app"))
	require.False(t, store.IsOwner("b", "pod", "app"))

	clone := store.Clone()
	// mirror checks done on the original to make sure they agree
	require.True(t, clone.IsOwner("a", "pod", "app"))
	require.False(t, clone.IsOwner("b", "pod", "app"))
	require.Equal(t, store.Snapshot(), clone.Snapshot())
	// unbind
	clone.Unbind(logger, "a")
	// rebind, now succeeds
	_, err = clone.Bind(logger, "other", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0)}))
	require.NoError(t, err)
	clone.Release(logger, "b")
	// rebound clone
	require.True(t, clone.IsOwner("a", "other", "app"))
	require.True(t, clone.GetSharedCPUs().Equals(cpuset.New(1, 2)))

	// verify clone mutations didn't affect the original
	require.True(t, store.IsOwner("a", "pod", "app"))
	require.True(t, store.GetSharedCPUs().Equals(cpuset.New(2)))
}

func TestNewAllocationState(t *testing.T) {
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
			store := newTestAllocationState(logger, tc.allCPUs, tc.reservedCPUs)
			require.NotNil(t, store)
			require.True(t, store.availableCPUs.Equals(tc.expectedAvailable))
			require.True(t, store.GetSharedCPUs().Equals(tc.expectedSharedCPUs))
		})
	}
}

func TestAllocationStateResourceClaimAllocation(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	store := newTestAllocationState(logger, allCPUs, cpuset.New())
	claimUID := types.UID("claim-uid-1")
	cpus := cpuset.New(0, 1)

	// Add allocation
	requirePreparedAllocation(t, logger, store, claimUID, cpus)
	gotCPUs, ok := store.GetResourceClaimAllocation(claimUID)
	require.True(t, ok)
	require.True(t, cpus.Equals(gotCPUs))

	// Remove allocation
	store.Release(logger, claimUID)
	_, ok = store.GetResourceClaimAllocation(claimUID)
	require.False(t, ok)

	// Remove non-existent allocation
	store.Release(logger, types.UID("non-existent"))
}

func TestAllocationStateGetSharedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0)
	store := newTestAllocationState(logger, allCPUs, reserved)
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

func TestAllocationStateClaimlessCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	for _, tc := range []struct {
		name          string
		claimless     *cpuset.CPUSet
		prepared      map[types.UID]cpuset.CPUSet
		wantClaimless cpuset.CPUSet
		wantShared    cpuset.CPUSet
	}{{
		name:          "not confined: every CPU no claim holds",
		prepared:      map[types.UID]cpuset.CPUSet{"vm": cpuset.New(6, 7)},
		wantClaimless: cpuset.New(0, 1, 2, 3, 4, 5),
		wantShared:    cpuset.New(0, 1, 2, 3, 4, 5),
	}, {
		name:          "confined: the set less nothing",
		claimless:     new(cpuset.New(0, 1, 2, 3)),
		prepared:      map[types.UID]cpuset.CPUSet{"vm": cpuset.New(6, 7)},
		wantClaimless: cpuset.New(0, 1, 2, 3),
		wantShared:    cpuset.New(0, 1, 2, 3, 4, 5),
	}, {
		name:          "confined: a prepared claim's CPUs inside the set are left out",
		claimless:     new(cpuset.New(0, 1, 2, 3)),
		prepared:      map[types.UID]cpuset.CPUSet{"moved": cpuset.New(2, 3), "vm": cpuset.New(6)},
		wantClaimless: cpuset.New(0, 1),
		wantShared:    cpuset.New(0, 1, 4, 5, 7),
	}, {
		name:          "confined to no CPU",
		claimless:     new(cpuset.New()),
		wantClaimless: cpuset.New(),
		wantShared:    allCPUs,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestAllocationState(logger, allCPUs, cpuset.New())
			if tc.claimless != nil {
				store.SetClaimlessCPUs(*tc.claimless)
			}
			for uid, cpus := range tc.prepared {
				requirePreparedAllocation(t, logger, store, uid, cpus)
			}
			for name, s := range map[string]*AllocationState{"store": store, "clone": store.Clone()} {
				require.Truef(t, tc.wantClaimless.Equals(s.GetClaimlessCPUs()), "%s: claimless %s, want %s", name, s.GetClaimlessCPUs(), tc.wantClaimless)
				require.Truef(t, tc.wantShared.Equals(s.GetSharedCPUs()), "%s: shared %s, want %s", name, s.GetSharedCPUs(), tc.wantShared)
				confined, ok := s.ConfinedClaimlessCPUs()
				require.Equal(t, tc.claimless != nil, ok, name)
				if tc.claimless != nil {
					require.Truef(t, tc.claimless.Equals(confined), "%s: confined to %s, want %s", name, confined, *tc.claimless)
				}
			}
		})
	}
}

func TestAllocationStateReserveRepeatedCalls(t *testing.T) {
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
			store := newTestAllocationState(logger, allCPUs, cpuset.New())
			claimUID := types.UID("claim-uid-1")

			require.NoError(t, store.Reserve(logger, claimUID, oneRequest(tc.firstCPUs), false))
			err := store.Reserve(logger, claimUID, oneRequest(tc.secondCPUs), false)
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

func TestAllocationStateReserveSharedPoolGuard(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3)

	tests := []struct {
		name                string
		claimless           *cpuset.CPUSet
		prepared            cpuset.CPUSet
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
		{
			name:                "rejects the last shared CPU with shared containers",
			prepared:            cpuset.New(0, 1, 2),
			cpus:                cpuset.New(3),
			hasSharedContainers: true,
			wantErr:             true,
		},
		{
			name:                "confined to no CPU: allows the last free CPUs with shared containers",
			claimless:           new(cpuset.New()),
			cpus:                allCPUs,
			hasSharedContainers: true,
		},
		{
			name:                "confined: allows taking every CPU outside the set with shared containers",
			claimless:           new(cpuset.New(0, 1)),
			cpus:                cpuset.New(2, 3),
			hasSharedContainers: true,
		},
		{
			name:                "confined: rejects taking the last CPUs of the set with shared containers",
			claimless:           new(cpuset.New(0, 1)),
			cpus:                cpuset.New(0, 1),
			hasSharedContainers: true,
			wantErr:             true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestAllocationState(logger, allCPUs, cpuset.New())
			if test.claimless != nil {
				store.SetClaimlessCPUs(*test.claimless)
			}
			if !test.prepared.IsEmpty() {
				requirePreparedAllocation(t, logger, store, "earlier", test.prepared)
			}
			free := allCPUs.Difference(test.prepared)
			err := store.Reserve(logger, "claim", oneRequest(test.cpus), test.hasSharedContainers)
			if test.wantErr {
				require.ErrorContains(t, err, "would exhaust the shared CPU pool")
				_, ok := store.GetResourceClaimAllocation("claim")
				require.False(t, ok)
				require.True(t, store.GetSharedCPUs().Equals(free))
			} else {
				require.NoError(t, err)
				require.True(t, store.GetSharedCPUs().Equals(free.Difference(test.cpus)))
			}
		})
	}
}

// An overlapping assignment is refused naming each claim that holds the CPUs,
// from its record, so a device allocated twice or a record the kubelet forgot
// can be found from the refusal alone.
func TestAllocationStateReserveNamesTheHolders(t *testing.T) {
	const see = `; see "CPUs Held by Another Claim" in the driver's troubleshooting guide`
	named := func(namespace, name string, cpus ...int) ClaimRecord {
		return ClaimRecord{Namespace: namespace, Name: name, Requests: []RequestAllocation{{Request: "cpus", CPUs: cpuset.New(cpus...)}}}
	}
	tests := []struct {
		name    string
		setup   func(t *testing.T, s *AllocationState)
		cpus    []int
		wantErr string
	}{
		{
			name: "a holder bound to a container",
			setup: func(t *testing.T, s *AllocationState) {
				require.NoError(t, s.Reserve(testr.New(t), "vm", named("ns", "vm-claim", 0, 1), false))
				_, err := s.Bind(testr.New(t), "pod-uid", "app", []ClaimRequestRef{{ClaimUID: "vm"}}, nil)
				require.NoError(t, err)
			},
			cpus:    []int{1, 2},
			wantErr: `claim "new" has overlapping CPU assignment "1-2": claim ns/vm-claim (UID "vm") holds CPUs 1, bound to container "app" of pod "pod-uid"` + see,
		},
		{
			name: "holders bound to no container, in order of claim UID",
			setup: func(t *testing.T, s *AllocationState) {
				require.NoError(t, s.Reserve(testr.New(t), "b", named("ns", "b-claim", 2), false))
				require.NoError(t, s.Reserve(testr.New(t), "a", named("other", "a-claim", 0), false))
			},
			cpus:    []int{0, 2},
			wantErr: `claim "new" has overlapping CPU assignment "0,2": claim other/a-claim (UID "a") holds CPUs 0, bound to no container; claim ns/b-claim (UID "b") holds CPUs 2, bound to no container` + see,
		},
		{
			name: "a holder restored from upstream's spec has no name",
			setup: func(t *testing.T, s *AllocationState) {
				s.Restore(testr.New(t), "up", ClaimRecord{Requests: []RequestAllocation{{CPUs: cpuset.New(3)}}})
			},
			cpus:    []int{3},
			wantErr: `claim "new" has overlapping CPU assignment "3": claim with UID "up" holds CPUs 3, bound to no container` + see,
		},
		{
			name:    "CPUs no claim holds are refused as before",
			setup:   func(*testing.T, *AllocationState) {},
			cpus:    []int{9},
			wantErr: `claim "new" has overlapping CPU assignment "9"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestAllocationState(testr.New(t), cpuset.New(0, 1, 2, 3), cpuset.New())
			tc.setup(t, s)
			prepared := s.GetPreparedCPUs()

			err := s.Reserve(testr.New(t), "new", named("ns", "new-claim", tc.cpus...), false)
			require.EqualError(t, err, tc.wantErr)
			require.ErrorIs(t, err, ErrClaimOverlap)
			_, recorded := s.GetClaimRecord("new")
			require.False(t, recorded)
			require.True(t, prepared.Equals(s.GetPreparedCPUs()))
		})
	}
}

func TestAllocationStateHoldsExclusiveCPUsOf(t *testing.T) {
	s := newTestAllocationState(testr.New(t), cpuset.New(0, 1, 2, 3), cpuset.New())
	require.NoError(t, s.Reserve(testr.New(t), "claim", ClaimRecord{Requests: []RequestAllocation{
		{Request: "a", CPUs: cpuset.New(0)},
		{Request: "b", CPUs: cpuset.New(1)},
	}}, false))
	s.Restore(testr.New(t), "up", ClaimRecord{Requests: []RequestAllocation{{CPUs: cpuset.New(2)}}})

	tests := []struct {
		name string
		ref  ClaimRequestRef
		want bool
	}{
		{name: "a request of a prepared claim", ref: ClaimRequestRef{ClaimUID: "claim", Request: "b"}, want: true},
		{name: "a prepared claim as a whole", ref: ClaimRequestRef{ClaimUID: "claim"}, want: true},
		{name: "a request the claim was prepared without", ref: ClaimRequestRef{ClaimUID: "claim", Request: "c"}, want: false},
		{name: "a claim restored from upstream's spec, as a whole", ref: ClaimRequestRef{ClaimUID: "up"}, want: true},
		{name: "a request of a claim restored from upstream's spec", ref: ClaimRequestRef{ClaimUID: "up", Request: "a"}, want: false},
		{name: "a claim that is not prepared", ref: ClaimRequestRef{ClaimUID: "other", Request: "a"}, want: false},
		{name: "a shared request", ref: ClaimRequestRef{ClaimUID: "share", Request: "s"}, want: false},
		{name: "a claim of shared requests alone, as a whole", ref: ClaimRequestRef{ClaimUID: "share"}, want: false},
	}
	require.NoError(t, s.Reserve(testr.New(t), "share", ClaimRecord{Requests: []RequestAllocation{
		{Request: "s", CPUs: cpuset.New(0, 1, 2, 3), Role: RoleShared, Devices: []string{"dev"}},
	}}, false))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, s.HoldsExclusiveCPUsOf(tc.ref))
		})
	}
}

// A shared request reserves nothing and runs on its devices' CPUs as last
// set, less every CPU an exclusive request holds, wherever that lies.
func TestAllocationStateSharedRequests(t *testing.T) {
	logger := testr.New(t)
	share := func(devices ...string) ClaimRecord {
		return ClaimRecord{Requests: []RequestAllocation{{Request: "vmm-cpu", CPUs: cpuset.New(0, 1, 2, 3), Role: RoleShared, Devices: devices}}}
	}
	for _, tc := range []struct {
		name      string
		claimless *cpuset.CPUSet
		devices   map[string]cpuset.CPUSet
		share     ClaimRecord
		exclusive cpuset.CPUSet
		wantShare cpuset.CPUSet
		wantWhole cpuset.CPUSet
	}{{
		name:      "its device's CPUs",
		devices:   map[string]cpuset.CPUSet{"dev": cpuset.New(0, 1, 2, 3)},
		share:     share("dev"),
		wantShare: cpuset.New(0, 1, 2, 3),
		wantWhole: cpuset.New(0, 1, 2, 3),
	}, {
		name:      "less the CPUs an exclusive request holds inside the device",
		devices:   map[string]cpuset.CPUSet{"dev": cpuset.New(0, 1, 2, 3)},
		share:     share("dev"),
		exclusive: cpuset.New(2),
		wantShare: cpuset.New(0, 1, 3),
		wantWhole: cpuset.New(0, 1, 3),
	}, {
		name:      "the device as last set, not as recorded",
		devices:   map[string]cpuset.CPUSet{"dev": cpuset.New(0, 1)},
		share:     share("dev"),
		wantShare: cpuset.New(0, 1),
		wantWhole: cpuset.New(0, 1),
	}, {
		name:      "each of its devices",
		devices:   map[string]cpuset.CPUSet{"a": cpuset.New(0, 1), "b": cpuset.New(4, 5)},
		share:     share("a", "b"),
		wantShare: cpuset.New(0, 1, 4, 5),
		wantWhole: cpuset.New(0, 1, 4, 5),
	}, {
		name:      "the CPUs of the containers without claims where its device is gone",
		claimless: new(cpuset.New(4, 5, 6)),
		devices:   map[string]cpuset.CPUSet{"other": cpuset.New(0, 1)},
		share:     share("dev"),
		exclusive: cpuset.New(6),
		wantShare: cpuset.New(4, 5),
		wantWhole: cpuset.New(4, 5),
	}, {
		name:      "no CPU where exclusive requests hold the device",
		devices:   map[string]cpuset.CPUSet{"dev": cpuset.New(2)},
		share:     share("dev"),
		exclusive: cpuset.New(2),
		wantShare: cpuset.New(),
		wantWhole: cpuset.New(),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestAllocationState(logger, cpuset.New(0, 1, 2, 3, 4, 5, 6, 7), cpuset.New())
			if tc.claimless != nil {
				store.SetClaimlessCPUs(*tc.claimless)
			}
			store.SetSharedDevices(tc.devices)
			if !tc.exclusive.IsEmpty() {
				store.Restore(logger, "exclusive", oneRequest(tc.exclusive))
			}
			require.NoError(t, store.Reserve(logger, "share", tc.share, true))
			require.Equalf(t, tc.exclusive.String(), store.GetPreparedCPUs().String(), "a share reserves nothing")
			for name, s := range map[string]*AllocationState{"store": store, "clone": store.Clone()} {
				got, err := s.GetRequestAllocationUnion(ClaimRequestRef{ClaimUID: "share", Request: "vmm-cpu"})
				require.NoError(t, err)
				require.Truef(t, tc.wantShare.Equals(got), "%s: share %s, want %s", name, got, tc.wantShare)
				whole, err := s.GetRequestAllocationUnion(ClaimRequestRef{ClaimUID: "share"})
				require.NoError(t, err)
				require.Truef(t, tc.wantWhole.Equals(whole), "%s: whole claim %s, want %s", name, whole, tc.wantWhole)
				exclusive, err := s.GetExclusiveRequestUnion(ClaimRequestRef{ClaimUID: "share", Request: "vmm-cpu"})
				require.NoError(t, err)
				require.Truef(t, exclusive.IsEmpty(), "%s: a share holds no CPU alone, got %s", name, exclusive)
				record, ok := s.GetClaimRecord("share")
				require.True(t, ok)
				require.Equal(t, tc.share.Requests, record.Requests, "%s: the record keeps the role and the devices", name)
			}
		})
	}
}

func TestAllocationStateRestoreKeepsOverlappingClaimsUnbound(t *testing.T) {
	logger := testr.New(t)
	store := newTestAllocationState(logger, cpuset.New(0, 1, 2, 3, 4), cpuset.New())

	store.Restore(logger, "a", oneRequest(cpuset.New(0, 1)))
	store.Restore(logger, "b", oneRequest(cpuset.New(1, 2)))
	store.Restore(logger, "c", oneRequest(cpuset.New(0, 3)))
	store.Restore(logger, "a", oneRequest(cpuset.New(4)))
	cpus, ok := store.GetResourceClaimAllocation("a")
	require.True(t, ok)
	require.Equal(t, "0-1", cpus.String(), "restoring a recorded claim must not change it")
	require.Equal(t, "0-3", store.GetPreparedCPUs().String())
	require.Equal(t, "4", store.GetSharedCPUs().String())
	require.Error(t, store.Reserve(logger, "d", oneRequest(cpuset.New(2)), false), "a new claim must not take a restored claim's CPU")

	_, err := store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"a": cpuset.New(0, 1)}))
	var overlapErr ClaimOverlapError
	require.ErrorAs(t, err, &overlapErr)
	require.Equal(t, types.UID("a"), overlapErr.ClaimUID)
	require.Equal(t, types.UID("b"), overlapErr.Other, "of the claims a overlaps, the one with the lowest UID is named")
	require.Equal(t, "1", overlapErr.CPUs.String())
	_, err = store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"b": cpuset.New(1, 2)}))
	require.ErrorIs(t, err, ErrClaimOverlap)
	_, bound := store.Owner("b")
	require.False(t, bound)

	store.Release(logger, "a")
	require.Equal(t, "0-3", store.GetPreparedCPUs().String(), "a CPU stays prepared while another claim names it")
	_, err = store.Bind(logger, "pod", "app", nil, wholeClaims(map[types.UID]cpuset.CPUSet{"b": cpuset.New(1, 2)}))
	require.NoError(t, err)
	owner, bound := store.Owner("b")
	require.True(t, bound)
	require.Equal(t, OwnerIdent{PodUID: "pod", ContainerName: "app"}, owner)
	_, bound = store.Owner("a")
	require.False(t, bound, "a released claim has no owner")
}

func TestAllocationStateStoreCacheConsistency(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	store := newTestAllocationState(logger, allCPUs, cpuset.New())

	requirePreparedAllocation(t, logger, store, "claim-1", cpuset.New(0, 1))
	requirePreparedAllocation(t, logger, store, "claim-2", cpuset.New(2, 3))
	requirePreparedAllocation(t, logger, store, "claim-3", cpuset.New(4, 5))

	expectedShared := cpuset.New(6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.Release(logger, "claim-2")
	expectedShared = cpuset.New(2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.Release(logger, "claim-1")
	expectedShared = cpuset.New(0, 1, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
	require.True(t, store.GetSharedCPUs().Equals(expectedShared))

	store.Release(logger, "claim-3")
	require.True(t, store.GetSharedCPUs().Equals(allCPUs))
}

func TestAllocationStateGetReservedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0, 1)
	store := newTestAllocationState(logger, allCPUs, reserved)
	require.True(t, store.GetReservedCPUs().Equals(reserved))
}

func TestAllocationStateGetPreparedCPUs(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	store := newTestAllocationState(logger, allCPUs, cpuset.New())

	require.True(t, store.GetPreparedCPUs().IsEmpty())

	claimUID := types.UID("claim-1")
	cpus := cpuset.New(2, 3)
	requirePreparedAllocation(t, logger, store, claimUID, cpus)
	require.True(t, store.GetPreparedCPUs().Equals(cpus))

	store.Release(logger, claimUID)
	require.True(t, store.GetPreparedCPUs().IsEmpty())
}

func TestAllocationStateSnapshot(t *testing.T) {
	logger := testr.New(t)
	allCPUs := cpuset.New(0, 1, 2, 3, 4, 5, 6, 7)
	reserved := cpuset.New(0, 1)
	store := newTestAllocationState(logger, allCPUs, reserved)

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

	store.Release(logger, "claim-1")
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

	store.Release(logger, "claim-1")
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
			store := NewAllocationState(topo, cpuset.New())
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
