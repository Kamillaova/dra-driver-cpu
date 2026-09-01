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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

func getPlacements(t *testing.T, d *defragTestDriver, query string) placementsReport {
	t.Helper()
	recorder := httptest.NewRecorder()
	d.ServePlacements(recorder, httptest.NewRequest(http.MethodGet, "/placements"+query, nil))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))

	var report placementsReport
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &report))
	return report
}

func TestServePlacementsReportsWhereClaimsAre(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.nodeName = "worker-1"
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-uid-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.placeClaim(t, "claim-2", cpuset.New(1))

	report := getPlacements(t, d, "")

	require.Equal(t, "worker-1", report.NodeName)
	require.True(t, report.DefragEnabled)
	require.Equal(t, "2-3,5-7", report.SharedCPUs)

	require.Len(t, report.Claims, 2)
	require.Equal(t, claimReport{
		ClaimUID:      "claim-1",
		CPUs:          "0,4",
		PodUID:        "pod-uid-1",
		ContainerName: "ctr-1",
		ContainerID:   "ctr-uid-1",
	}, report.Claims[0])
	// A claim prepared but not yet started has no container to name.
	require.Equal(t, claimReport{ClaimUID: "claim-2", CPUs: "1"}, report.Claims[1])

	require.Len(t, report.NUMANodes, 1)
	node := report.NUMANodes[0]
	require.Equal(t, 0, node.NUMANodeID)
	require.Equal(t, "2-3,5-7", node.FreeCPUs)
	require.Equal(t, 1, node.ExcessUncoreCaches, "claim-1 spans two caches where one would do")
	require.Equal(t, 3, node.LargestAlignableFreeCPUs)
	require.Nil(t, node.Plan, "a plan is only reported for a dry run")

	require.Equal(t, []cacheReport{
		{CacheID: 0, CPUs: "0-3", FreeCPUs: "2-3"},
		{CacheID: 1, CPUs: "4-7", FreeCPUs: "5-7"},
	}, node.Caches)
}

func TestServePlacementsDryRunShowsWhatAPassWouldDo(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-uid-1", "ctr-1", "ctr-uid-1", "claim-1")

	report := getPlacements(t, d, "?dryrun=1")

	require.Len(t, report.NUMANodes, 1)
	plan := report.NUMANodes[0].Plan
	require.NotNil(t, plan)
	require.Equal(t, 1, plan.CurrentCost)
	require.Equal(t, 0, plan.IdealCost)
	require.Equal(t, []moveReport{{ClaimUID: "claim-1", From: "0,4", To: "0-1"}}, plan.Moves)

	// Nothing may have happened: the claim is still where it was, and the runtime
	// was not asked for anything.
	cpus, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 4), cpus)
	require.Empty(t, d.updater.allCalls())

	// The plan a pass would carry out is the plan it does carry out.
	d.defragPass(context.Background())
	moved, _ := d.cpuAllocationStore.GetResourceClaimAllocation("claim-1")
	require.Equal(t, cpuset.New(0, 1), moved)
}

func TestServePlacementsDryRunSaysWhyNothingWouldMove(t *testing.T) {
	// The question this endpoint exists to answer: a claim is split and stays
	// split, and nothing in the metrics says why.
	d := newDefragTestDriver(t, 2, 2)
	d.placeClaim(t, "claim-1", cpuset.New(0, 3))
	d.placeClaim(t, "claim-2", cpuset.New(1, 2))
	d.runContainer(t, "pod-uid-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.runContainer(t, "pod-uid-2", "ctr-2", "ctr-uid-2", "claim-2")

	plan := getPlacements(t, d, "?dryrun=1").NUMANodes[0].Plan
	require.NotNil(t, plan)
	require.Empty(t, plan.Moves)
	require.Positive(t, plan.Blocked)
	require.Contains(t, plan.Reason, "blocked")
}

func TestServePlacementsReportsAMoveInFlight(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	d.placeClaim(t, "claim-1", cpuset.New(0, 4))
	d.runContainer(t, "pod-uid-1", "ctr-1", "ctr-uid-1", "claim-1")
	d.updater.err = errUpdateFailed
	d.defragPass(context.Background())

	report := getPlacements(t, d, "")

	require.Len(t, report.Claims, 1)
	require.Equal(t, "0-1", report.Claims[0].CPUs, "the claim reads as being on its target")
	require.Equal(t, "0,4", report.Claims[0].MovingFrom, "and as still holding what it came from")
}

func TestServePlacementsReportsNodesItCannotMeasure(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	// A CPU with no uncore cache is a node whose spread cannot be reasoned about.
	info := d.topology.cpuTopology.CPUDetails[2]
	info.UncoreCacheID = -1
	d.topology.cpuTopology.CPUDetails[2] = info

	report := getPlacements(t, d, "?dryrun=1")

	require.Empty(t, report.NUMANodes)
	require.Len(t, report.Unmeasurable, 1)
	require.Equal(t, 0, report.Unmeasurable[0].NUMANodeID)
	require.Contains(t, report.Unmeasurable[0].Reason, "no uncore cache")
}

func TestServePlacementsRejectsOtherMethods(t *testing.T) {
	d := newDefragTestDriver(t, 2, 4)
	recorder := httptest.NewRecorder()

	// Read-only by construction: a dry run is a pure function of the snapshot.
	d.ServePlacements(recorder, httptest.NewRequest(http.MethodPost, "/placements", nil))

	require.Equal(t, http.StatusMethodNotAllowed, recorder.Code)
	require.Equal(t, http.MethodGet, recorder.Header().Get("Allow"))
}

func TestServePlacementsReportsAnErrorItCannotSnapshotThrough(t *testing.T) {
	// The report starts from the online CPUs; without them there is no truthful
	// snapshot, so the endpoint must say so rather than serve a partial one.
	d := newDefragTestDriver(t, 2, 4)
	d.sysfs = nil

	recorder := httptest.NewRecorder()
	d.ServePlacements(recorder, httptest.NewRequest(http.MethodGet, "/placements", nil))

	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "cannot report placements")
}
