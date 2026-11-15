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

package coreselect_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

// smtCaches returns caches x coresPerCache cores of two threads on one socket
// and NUMA node. Cores are numbered 0..n-1; thread 1 of core c is CPU c+n.
func smtCaches(caches, coresPerCache int) *cpuinfo.CPUTopology {
	cores := caches * coresPerCache
	details := cpuinfo.CPUDetails{}
	for cpu := range cores * 2 {
		core := cpu % cores
		details[cpu] = cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        core,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: core / coresPerCache,
			SiblingCPUID:  (cpu + cores) % (cores * 2),
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: cores * 2, NumCores: cores, NumSockets: 1, NumNUMANodes: 1,
		NumUncoreCache: caches, SMTEnabled: true, CPUDetails: details,
	}
}

// cacheShape is how many free cores of two threads and of one a cache holds.
type cacheShape struct{ two, one int }

// mixedCaches returns one socket and NUMA node whose cache i holds shapes[i]:
// its cores of two threads first, each a pair of consecutive CPUs, then its
// cores of one thread. Every CPU carries its core as its sibling list.
func mixedCaches(shapes ...cacheShape) *cpuinfo.CPUTopology {
	details := cpuinfo.CPUDetails{}
	cpu, coreID := 0, 0
	add := func(cacheID int, cpus ...int) {
		siblings := cpuset.New(cpus...)
		for _, c := range cpus {
			details[c] = cpuinfo.CPUInfo{
				CpuID: c, CoreID: coreID, SocketID: 0, NUMANodeID: 0, UncoreCacheID: cacheID,
				SiblingCPUID: -1, SiblingCPUSet: siblings,
			}
		}
		coreID++
	}
	for cacheID, shape := range shapes {
		for range shape.two {
			add(cacheID, cpu, cpu+1)
			cpu += 2
		}
		for range shape.one {
			add(cacheID, cpu)
			cpu++
		}
	}
	return &cpuinfo.CPUTopology{
		NumCPUs: len(details), NumCores: coreID, NumSockets: 1, NumNUMANodes: 1,
		NumUncoreCache: len(shapes), SMTEnabled: true, CPUDetails: details,
	}
}

func cachesTouched(topo *cpuinfo.CPUTopology, cpus cpuset.CPUSet) int {
	seen := map[int]struct{}{}
	for _, cpu := range cpus.List() {
		seen[topo.CPUDetails[cpu].UncoreCacheID] = struct{}{}
	}
	return len(seen)
}

func requireWholeCores(t *testing.T, topo *cpuinfo.CPUTopology, cpus cpuset.CPUSet) {
	t.Helper()
	require.Equal(t, cpus, topo.CPUDetails.CompleteCores(cpus), "result %s splits a physical core", cpus.String())
}

// twoThreadCoresIn counts the cores of two threads cpus holds.
func twoThreadCoresIn(topo *cpuinfo.CPUTopology, cpus cpuset.CPUSet) int {
	n := 0
	for _, cpu := range cpus.List() {
		if siblings := topo.CPUDetails.SiblingsOf(cpu); siblings.Size() == 2 && siblings.List()[0] == cpu {
			n++
		}
	}
	return n
}

func TestTakeWholeCoresNeverSplitsACore(t *testing.T) {
	topo := smtCaches(2, 4)
	available := topo.CPUDetails.CPUs()

	for _, numCPUs := range []int{2, 4, 6, 8, 10, 16} {
		got, err := coreselect.TakeWholeCores(topo, available, numCPUs)
		require.NoError(t, err, "numCPUs=%d", numCPUs)
		require.Equal(t, numCPUs, got.Size(), "numCPUs=%d returned %s", numCPUs, got.String())
		requireWholeCores(t, topo, got)
	}
}

func TestTakeWholeCoresMinimisesCachesSpanned(t *testing.T) {
	topo := smtCaches(2, 4) // each cache holds 8 CPUs
	available := topo.CPUDetails.CPUs()

	for _, numCPUs := range []int{2, 4, 6, 8} {
		got, err := coreselect.TakeWholeCores(topo, available, numCPUs)
		require.NoError(t, err)
		require.Equal(t, 1, cachesTouched(topo, got), "numCPUs=%d spanned %d caches: %s", numCPUs, cachesTouched(topo, got), got.String())
	}

	got, err := coreselect.TakeWholeCores(topo, available, 12)
	require.NoError(t, err)
	require.Equal(t, 2, cachesTouched(topo, got))
}

func TestTakeWholeCoresFillsPartlyUsedCacheFirst(t *testing.T) {
	topo := smtCaches(2, 4)
	// Cache 0 has one core taken already (CPUs 0 and 8), cache 1 none.
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(0, 8))

	got, err := coreselect.TakeWholeCores(topo, available, 4)
	require.NoError(t, err)
	requireWholeCores(t, topo, got)
	require.Equal(t, 1, cachesTouched(topo, got))
	require.Equal(t, 0, topo.CPUDetails[got.List()[0]].UncoreCacheID, "expected the partly used cache 0, got %s", got.String())

	rest, err := coreselect.TakeWholeCores(topo, available.Difference(got), 8)
	require.NoError(t, err)
	require.Equal(t, 1, cachesTouched(topo, rest), "cache 1 still holds a whole-cache claim")
}

func TestTakeWholeCoresIgnoresHalfCores(t *testing.T) {
	topo := smtCaches(1, 4) // cores 0-3, siblings 4-7
	// CPU 4 is not available, so core 0 cannot be taken at all.
	available := topo.CPUDetails.CPUs().Difference(cpuset.New(4))

	got, err := coreselect.TakeWholeCores(topo, available, 6)
	require.NoError(t, err)
	requireWholeCores(t, topo, got)
	require.False(t, got.Contains(0), "CPU 0's sibling is not available, so CPU 0 cannot be taken")

	require.Equal(t, 7, available.Size())
	_, err = coreselect.TakeWholeCores(topo, available, 8)
	require.ErrorContains(t, err, "not enough whole cores")
}

func TestTakeWholeCoresIsDeterministic(t *testing.T) {
	for _, topo := range []*cpuinfo.CPUTopology{
		smtCaches(4, 4),
		mixedCaches(cacheShape{two: 2, one: 3}, cacheShape{two: 3, one: 1}, cacheShape{one: 4}),
	} {
		available := topo.CPUDetails.CPUs()
		first, err := coreselect.TakeWholeCores(topo, available, 9)
		if topo.NumCores == 16 { // smtCaches: two-thread cores only, take an even amount
			first, err = coreselect.TakeWholeCores(topo, available, 10)
		}
		require.NoError(t, err)
		for range 20 {
			again, err := coreselect.TakeWholeCores(topo, available, first.Size())
			require.NoError(t, err)
			require.Equal(t, first, again, "the take must not depend on map iteration order")
		}
	}
}

func TestTakeWholeCoresEdgeCases(t *testing.T) {
	topo := smtCaches(1, 2)

	got, err := coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), 0)
	require.NoError(t, err)
	require.Equal(t, cpuset.New(), got)

	_, err = coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), -2)
	require.Error(t, err)

	_, err = coreselect.TakeWholeCores(topo, cpuset.New(), 2)
	require.Error(t, err)

	_, err = coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), 3)
	var noSingle *coreselect.NoFreeSingleThreadCoreError
	require.ErrorAs(t, err, &noSingle, "an odd amount cannot be made of two-thread cores")
}

func TestTakeWholeCoresDistinguishesSockets(t *testing.T) {
	// core_id restarts on each socket: socket 0 core 0 is {0,4}, socket 1 core 0 is {2,6}.
	details := cpuinfo.CPUDetails{
		0: {CpuID: 0, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		1: {CpuID: 1, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		2: {CpuID: 2, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		3: {CpuID: 3, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		4: {CpuID: 4, CoreID: 0, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		5: {CpuID: 5, CoreID: 1, SocketID: 0, NUMANodeID: 0, UncoreCacheID: 0},
		6: {CpuID: 6, CoreID: 0, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
		7: {CpuID: 7, CoreID: 1, SocketID: 1, NUMANodeID: 1, UncoreCacheID: 1},
	}
	topo := &cpuinfo.CPUTopology{
		NumCPUs: 8, NumCores: 4, NumSockets: 2, NumNUMANodes: 2, NumUncoreCache: 2,
		SMTEnabled: true, CPUDetails: details,
	}

	socket0 := cpuset.New(0, 1, 4, 5)
	got, err := coreselect.TakeWholeCores(topo, socket0, 4)
	require.NoError(t, err)
	require.True(t, got.IsSubsetOf(socket0), "leaked outside the available set: %s", got.String())
	requireWholeCores(t, topo, got)
}

func TestTakeWholeCoresOnMixedCaches(t *testing.T) {
	tests := []struct {
		name      string
		topo      *cpuinfo.CPUTopology
		available func(*cpuinfo.CPUTopology) cpuset.CPUSet
		numCPUs   int
		want      cpuset.CPUSet
		wantErr   *coreselect.NoFreeSingleThreadCoreError
	}{
		{
			name:    "an odd amount takes one core of one thread",
			topo:    mixedCaches(cacheShape{two: 2, one: 2}),
			numCPUs: 3,
			want:    cpuset.New(0, 1, 4),
		},
		{
			name:    "an even amount takes cores of two threads only",
			topo:    mixedCaches(cacheShape{two: 2, one: 2}),
			numCPUs: 4,
			want:    cpuset.New(0, 1, 2, 3),
		},
		{
			name:    "an amount past the two-thread cores takes one-thread cores for the rest",
			topo:    mixedCaches(cacheShape{two: 1, one: 3}),
			numCPUs: 4,
			want:    cpuset.New(0, 1, 2, 3),
		},
		{
			name:    "an even amount goes to a cache of two-thread cores before one of one-thread cores only",
			topo:    mixedCaches(cacheShape{one: 4}, cacheShape{two: 1}),
			numCPUs: 2,
			want:    cpuset.New(4, 5),
		},
		{
			name:    "an odd amount takes its one-thread core from another cache when its own has none",
			topo:    mixedCaches(cacheShape{two: 2}, cacheShape{one: 1}),
			numCPUs: 3,
			want:    cpuset.New(0, 1, 4),
		},
		{
			name: "an odd amount with no one-thread core free is refused",
			topo: mixedCaches(cacheShape{two: 2, one: 1}),
			available: func(topo *cpuinfo.CPUTopology) cpuset.CPUSet {
				return topo.CPUDetails.CPUs().Difference(cpuset.New(4))
			},
			numCPUs: 3,
			wantErr: &coreselect.NoFreeSingleThreadCoreError{CPUs: 3, FreeTwoThreadCores: 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			available := tc.topo.CPUDetails.CPUs()
			if tc.available != nil {
				available = tc.available(tc.topo)
			}
			got, err := coreselect.TakeWholeCores(tc.topo, available, tc.numCPUs)
			if tc.wantErr != nil {
				var noSingle *coreselect.NoFreeSingleThreadCoreError
				require.ErrorAs(t, err, &noSingle)
				require.Equal(t, tc.wantErr, noSingle)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// fits is the rule an amount must meet on t free cores of two threads and s of one.
func fits(n, t, s int) bool {
	return n >= 1 && n <= 2*t+s && (n%2 == 0 || s >= 1)
}

// TestTakeWholeCoresFitsWhereTheRuleSays takes every amount from every device of
// one or two caches holding up to two free cores of each size per cache, and
// checks the take against the rule and the cores it must use.
func TestTakeWholeCoresFitsWhereTheRuleSays(t *testing.T) {
	var shapes []cacheShape
	for two := range 3 {
		for one := range 3 {
			shapes = append(shapes, cacheShape{two: two, one: one})
		}
	}
	var devices [][]cacheShape
	for _, a := range shapes {
		devices = append(devices, []cacheShape{a})
		for _, b := range shapes {
			devices = append(devices, []cacheShape{a, b})
		}
	}
	for _, device := range devices {
		topo := mixedCaches(device...)
		t2, s := 0, 0
		for _, shape := range device {
			t2 += shape.two
			s += shape.one
		}
		for n := 1; n <= 2*t2+s+1; n++ {
			name := fmt.Sprintf("caches %v, %d CPUs", device, n)
			got, err := coreselect.TakeWholeCores(topo, topo.CPUDetails.CPUs(), n)
			if !fits(n, t2, s) {
				require.Error(t, err, name)
				continue
			}
			require.NoError(t, err, name)
			require.Equal(t, n, got.Size(), name)
			requireWholeCores(t, topo, got)
			require.Equal(t, min(t2, n/2), twoThreadCoresIn(topo, got), "%s: cores of two threads", name)
		}
	}
}

// fitTogether is an exhaustive search for whole cores for every amount at once.
func fitTogether(amounts []int, t, s int) bool {
	if len(amounts) == 0 {
		return true
	}
	n := amounts[0]
	for a := 0; a <= min(t, n/2); a++ {
		if b := n - 2*a; b <= s && fitTogether(amounts[1:], t-a, s-b) {
			return true
		}
	}
	return false
}

// multisets returns every multiset of up to maxLen amounts of at least 1 whose
// sum is at most maxSum, in ascending order.
func multisets(maxLen, maxSum int) [][]int {
	var out [][]int
	var grow func(prefix []int, from, sum int)
	grow = func(prefix []int, from, sum int) {
		if len(prefix) > 0 {
			out = append(out, slices.Clone(prefix))
		}
		if len(prefix) == maxLen {
			return
		}
		for n := from; sum+n <= maxSum; n++ {
			grow(append(prefix, n), n, sum+n)
		}
	}
	grow(nil, 1, 0)
	return out
}

// orders returns every distinct order of amounts.
func orders(amounts []int) [][]int {
	if len(amounts) <= 1 {
		return [][]int{slices.Clone(amounts)}
	}
	var out [][]int
	for i, n := range amounts {
		if i > 0 && amounts[i-1] == n {
			continue
		}
		rest := slices.Concat(amounts[:i:i], amounts[i+1:])
		for _, o := range orders(rest) {
			out = append(out, append([]int{n}, o...))
		}
	}
	return out
}

// TestTakeWholeCoresInEveryOrder takes every multiset of amounts from devices
// of up to four free cores of each size, in every order with no release
// between the takes: every order fits or none does, as an exhaustive search for
// whole cores for all the amounts at once says.
func TestTakeWholeCoresInEveryOrder(t *testing.T) {
	for t2 := range 5 {
		for s := range 5 {
			topo := mixedCaches(cacheShape{two: t2, one: s})
			for _, amounts := range multisets(4, 2*t2+s+1) {
				want := fitTogether(amounts, t2, s)
				for _, order := range orders(amounts) {
					available := topo.CPUDetails.CPUs()
					got := true
					for _, n := range order {
						taken, err := coreselect.TakeWholeCores(topo, available, n)
						if err != nil {
							got = false
							break
						}
						available = available.Difference(taken)
					}
					require.Equalf(t, want, got, "%d cores of 2 threads, %d of 1: amounts %v in order %v", t2, s, amounts, order)
				}
			}
		}
	}
}

// TestTakeWholeCoresAfterARelease follows takes and releases on one device.
func TestTakeWholeCoresAfterARelease(t *testing.T) {
	type step struct {
		take    int // 0 releases the take of index release
		release int
		fits    bool
	}
	tests := []struct {
		name  string
		shape cacheShape
		steps []step
	}{
		{
			name:  "1 then 3 on two cores of two threads and one of one",
			shape: cacheShape{two: 2, one: 1},
			steps: []step{{take: 1, fits: true}, {take: 3, fits: false}},
		},
		{
			name:  "3 then 1 on two cores of two threads and one of one",
			shape: cacheShape{two: 2, one: 1},
			steps: []step{{take: 3, fits: true}, {take: 1, fits: false}},
		},
		{
			name:  "1 then 2 on one core of two threads and two of one",
			shape: cacheShape{two: 1, one: 2},
			steps: []step{{take: 1, fits: true}, {take: 2, fits: true}},
		},
		{
			name:  "2 and 2, the first released, then 1, on one core of two threads and two of one",
			shape: cacheShape{two: 1, one: 2},
			steps: []step{{take: 2, fits: true}, {take: 2, fits: true}, {release: 0}, {take: 1, fits: false}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topo := mixedCaches(tc.shape)
			available := topo.CPUDetails.CPUs()
			var taken []cpuset.CPUSet
			for i, st := range tc.steps {
				if st.take == 0 {
					available = available.Union(taken[st.release])
					continue
				}
				got, err := coreselect.TakeWholeCores(topo, available, st.take)
				if !st.fits {
					var noSingle *coreselect.NoFreeSingleThreadCoreError
					require.Truef(t, errors.As(err, &noSingle), "step %d: want no free core of one thread, got %v", i, err)
					continue
				}
				require.NoErrorf(t, err, "step %d", i)
				taken = append(taken, got)
				available = available.Difference(got)
			}
		})
	}
}
