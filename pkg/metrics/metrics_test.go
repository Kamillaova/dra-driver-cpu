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

package metrics

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestDescriptors(t *testing.T) {
	descriptors := Descriptors()
	require.Len(t, descriptors, 50)

	names := make([]string, 0, len(descriptors))
	for _, desc := range descriptors {
		names = append(names, desc.Name)
		require.Equal(t, "ALPHA", desc.Stability)
		require.NotEmpty(t, desc.Help)
		require.NotNil(t, desc.Labels)
	}

	require.Equal(t, []string{
		"dra_cpu_allocated_cpus",
		"dra_cpu_available_cpus",
		"dra_cpu_reserved_cpus",
		"dra_cpu_resource_claims_active",
		"dra_cpu_prepare_claims_total",
		"dra_cpu_unprepare_claims_total",
		"dra_cpu_prepare_claim_duration_seconds",
		"dra_cpu_unprepare_claim_duration_seconds",
		"dra_cpu_claim_allocated_cpus",
		"dra_cpu_prepare_no_room_total",
		"dra_cpu_prepare_no_witness_total",
		"dra_cpu_nri_synchronize_duration_seconds",
		"dra_cpu_nri_create_container_duration_seconds",
		"dra_cpu_nri_stop_container_duration_seconds",
		"dra_cpu_nri_remove_container_duration_seconds",
		"dra_cpu_synchronize_skipped_claims_total",
		"dra_cpu_misplaced_claims_total",
		"dra_cpu_partition_verified",
		"dra_cpu_defrag_excess_uncore_caches",
		"dra_cpu_defrag_largest_alignable_free_cpus",
		"dra_cpu_defrag_passes_total",
		"dra_cpu_defrag_moves_total",
		"dra_cpu_defrag_blocked_moves_total",
		"dra_cpu_defrag_pass_duration_seconds",
		"dra_cpu_defrag_swap_overlap_seconds",
		"dra_cpu_defrag_partial_batches_total",
		"dra_cpu_defrag_rollbacks_total",
		"dra_cpu_defrag_numa_node_poisoned",
		"dra_cpu_defrag_poisoned_nodes_total",
		"dra_cpu_defrag_poisoned_duration_seconds",
		"dra_cpu_defrag_readback_mismatches_total",
		"dra_cpu_capacity_mirror_floored_devices",
		"dra_cpu_defrag_unpublished_rounds_total",
		"dra_cpu_frontier_admissions_total",
		"dra_cpu_frontier_oldest_obligation_seconds",
		"dra_cpu_repair_rounds_total",
		"dra_cpu_time_to_alignment_seconds",
		"dra_cpu_slice_writes_per_round",
		"dra_cpu_slice_updates_total",
		"dra_cpu_slice_update_bytes_total",
		"dra_cpu_slice_update_conflicts_total",
		"dra_cpu_slice_update_validation_errors_total",
		"dra_cpu_slice_update_rate_limits_total",
		"dra_cpu_slice_update_retry_depth",
		"dra_cpu_store_to_driver_delay_seconds",
		"dra_cpu_store_to_scheduler_delay_seconds",
		"dra_cpu_frontier_unusable_duration_seconds",
		"dra_cpu_slice_zero_commit_refreshes_total",
		"dra_cpu_claims_off_recorded_cache",
		"dra_cpu_capacity_mirror_max_abs_cache_error",
	}, names)
	// By name rather than by position in the list above: a metric added in the
	// middle renumbers every assertion after it, and the name is what a query
	// against these series is written with anyway.
	labelled := map[string][]string{
		"dra_cpu_prepare_claims_total":                  {"result"},
		"dra_cpu_unprepare_claims_total":                {"result"},
		"dra_cpu_prepare_no_room_total":                 {"shape"},
		"dra_cpu_nri_synchronize_duration_seconds":      {"result"},
		"dra_cpu_nri_create_container_duration_seconds": {"result", "cpu_allocation_mode"},
		"dra_cpu_nri_stop_container_duration_seconds":   {"result", "cpu_allocation_mode"},
		"dra_cpu_nri_remove_container_duration_seconds": {"result", "cpu_allocation_mode"},
		"dra_cpu_partition_verified":                    {"partition"},
		"dra_cpu_defrag_largest_alignable_free_cpus":    {"numa_node"},
		"dra_cpu_defrag_passes_total":                   {"result"},
		"dra_cpu_defrag_moves_total":                    {"result"},
		"dra_cpu_defrag_rollbacks_total":                {"result"},
		"dra_cpu_defrag_numa_node_poisoned":             {"numa_node"},
		"dra_cpu_frontier_admissions_total":             {"outcome"},
		"dra_cpu_repair_rounds_total":                   {"advertised_rounds", "actual_rounds"},
	}
	for _, desc := range descriptors {
		want := labelled[desc.Name]
		if want == nil {
			want = []string{}
		}
		require.Equal(t, want, desc.Labels, desc.Name)
	}
}

func TestWriteJSON(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteJSON(&buf))
	require.True(t, bytes.HasSuffix(buf.Bytes(), []byte("\n")))

	var descriptors []Descriptor
	require.NoError(t, json.Unmarshal(buf.Bytes(), &descriptors))
	require.Equal(t, Descriptors(), descriptors)
}

func TestNewRegistersExpectedMetricFamilies(t *testing.T) {
	reg := prometheus.NewRegistry()
	New(reg)

	families, err := reg.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	slices.Sort(names)

	// The per-NUMA-node gauge has no series until a pass reports one, so it has
	// no family to gather yet, unlike the result counters whose label values are
	// all known in advance and created here.
	require.Equal(t, []string{
		"dra_cpu_allocated_cpus",
		"dra_cpu_available_cpus",
		"dra_cpu_capacity_mirror_floored_devices",
		"dra_cpu_capacity_mirror_max_abs_cache_error",
		"dra_cpu_claim_allocated_cpus",
		"dra_cpu_claims_off_recorded_cache",
		"dra_cpu_defrag_blocked_moves_total",
		"dra_cpu_defrag_excess_uncore_caches",
		"dra_cpu_defrag_moves_total",
		"dra_cpu_defrag_partial_batches_total",
		"dra_cpu_defrag_pass_duration_seconds",
		"dra_cpu_defrag_passes_total",
		"dra_cpu_defrag_poisoned_duration_seconds",
		"dra_cpu_defrag_poisoned_nodes_total",
		"dra_cpu_defrag_readback_mismatches_total",
		"dra_cpu_defrag_rollbacks_total",
		"dra_cpu_defrag_swap_overlap_seconds",
		"dra_cpu_defrag_unpublished_rounds_total",
		"dra_cpu_frontier_admissions_total",
		"dra_cpu_frontier_oldest_obligation_seconds",
		"dra_cpu_frontier_unusable_duration_seconds",
		"dra_cpu_misplaced_claims_total",
		"dra_cpu_nri_create_container_duration_seconds",
		"dra_cpu_nri_remove_container_duration_seconds",
		"dra_cpu_nri_stop_container_duration_seconds",
		"dra_cpu_nri_synchronize_duration_seconds",
		"dra_cpu_prepare_claim_duration_seconds",
		"dra_cpu_prepare_claims_total",
		"dra_cpu_prepare_no_room_total",
		"dra_cpu_prepare_no_witness_total",
		"dra_cpu_reserved_cpus",
		"dra_cpu_resource_claims_active",
		"dra_cpu_slice_update_bytes_total",
		"dra_cpu_slice_update_conflicts_total",
		"dra_cpu_slice_update_rate_limits_total",
		"dra_cpu_slice_update_retry_depth",
		"dra_cpu_slice_update_validation_errors_total",
		"dra_cpu_slice_updates_total",
		"dra_cpu_slice_writes_per_round",
		"dra_cpu_slice_zero_commit_refreshes_total",
		"dra_cpu_store_to_driver_delay_seconds",
		"dra_cpu_store_to_scheduler_delay_seconds",
		"dra_cpu_synchronize_skipped_claims_total",
		"dra_cpu_time_to_alignment_seconds",
		"dra_cpu_unprepare_claim_duration_seconds",
		"dra_cpu_unprepare_claims_total",
	}, names)
}

func TestMetricsRecordsCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.SetAllocationState(AllocationState{
		AllocatedCPUs:        2,
		AvailableCPUs:        6,
		ReservedCPUs:         1,
		ActiveResourceClaims: 1,
	})
	m.RecordPrepare(ResultSuccess, 150*time.Millisecond)
	m.RecordPrepare(ResultError, 250*time.Millisecond)
	m.RecordUnprepare(ResultSuccess, 100*time.Millisecond)
	m.RecordClaimAllocatedCPUs(2)

	require.InDelta(t, 2, testutil.ToFloat64(m.allocatedCPUs), 0.01)
	require.InDelta(t, 6, testutil.ToFloat64(m.availableCPUs), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.reservedCPUs), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.activeResourceClaims), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.prepareClaims.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.prepareClaims.WithLabelValues(ResultError.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.unprepareClaims.WithLabelValues(ResultSuccess.String())), 0.01)

	families, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, families)
	require.Equal(t, 1, testutil.CollectAndCount(m.prepareClaimDuration))
	require.Equal(t, 1, testutil.CollectAndCount(m.unprepareClaimDuration))
	require.Equal(t, 1, testutil.CollectAndCount(m.claimAllocatedCPUs))
}

func TestDescriptorsMatchRegisteredCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.SetAllocationState(AllocationState{})
	m.RecordPrepare(ResultSuccess, time.Second)
	m.RecordPrepare(ResultError, time.Second)
	m.RecordUnprepare(ResultSuccess, time.Second)
	m.RecordUnprepare(ResultError, time.Second)
	m.RecordClaimAllocatedCPUs(1)
	m.SetDefragState(DefragState{LargestAlignableFreeCPUs: map[int]int{0: 4}})
	m.RecordDefragPass(ResultSuccess, time.Second)
	m.RecordDefragMoves(ResultSuccess, 1)
	m.RecordDefragBlockedMoves(1)
	m.RecordDefragSwapOverlap(time.Second)
	m.RecordDefragPartialBatch()
	m.RecordDefragRollback(ResultSuccess)
	m.SetDefragNodePoisoned(0, false)
	m.RecordDefragNodePoisoned()
	m.RecordDefragNodeReopened(time.Second)
	m.RecordDefragReadbackMismatch()
	m.SetFlooredCapacityDevices(0)
	m.RecordDefragUnpublishedRound()
	m.RecordSynchronizeSkippedClaim()
	m.RecordMisplacedClaim()
	m.SetPartitionState(map[string]bool{"dataplane": true})
	m.RecordRepairRounds("1", "1")

	families, err := reg.Gather()
	require.NoError(t, err)
	gotNames := make([]string, 0, len(families))
	for _, family := range families {
		gotNames = append(gotNames, family.GetName())
	}

	wantNames := make([]string, 0, len(Descriptors()))
	for _, descriptor := range Descriptors() {
		wantNames = append(wantNames, descriptor.Name)
	}
	require.ElementsMatch(t, wantNames, gotNames)
}

func TestMetricsRejectsUnboundedResultLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.RecordPrepare(Result("timeout"), time.Second)
	m.RecordUnprepare(Result("permission-denied"), time.Second)

	require.InDelta(t, 1, testutil.ToFloat64(m.prepareClaims.WithLabelValues(ResultUnknown.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.unprepareClaims.WithLabelValues(ResultUnknown.String())), 0.01)
	requireMetricLabelValueAbsent(t, reg, "dra_cpu_prepare_claims_total", "result", "timeout")
	requireMetricLabelValueAbsent(t, reg, "dra_cpu_unprepare_claims_total", "result", "permission-denied")
}

func TestNoopRecorder(t *testing.T) {
	recorder := Noop()

	require.NotPanics(t, func() {
		recorder.SetAllocationState(AllocationState{})
		recorder.RecordPrepare(ResultSuccess, time.Second)
		recorder.RecordUnprepare(ResultError, time.Second)
		recorder.RecordClaimAllocatedCPUs(4)
		recorder.SetDefragState(DefragState{LargestAlignableFreeCPUs: map[int]int{0: 4}})
		recorder.RecordDefragPass(ResultSuccess, time.Second)
		recorder.RecordDefragMoves(ResultError, 2)
		recorder.RecordDefragBlockedMoves(3)
		recorder.RecordSynchronizeSkippedClaim()
		recorder.RecordPrepareNoWitness()
		recorder.RecordFrontierAdmissionOutcome("started_split")
		recorder.SetFrontierOldestObligationSeconds(10.0)
		recorder.RecordRepairRounds("1", "1")
		recorder.RecordTimeToAlignment(1.0)
		recorder.RecordSliceWritesPerRound(1)
		recorder.RecordSliceUpdate(100)
		recorder.RecordSliceUpdateConflict()
		recorder.RecordSliceUpdateValidationError()
		recorder.RecordSliceUpdateRateLimit()
		recorder.RecordSliceUpdateRetryDepth(1)
		recorder.RecordStoreToDriverDelay(0.1)
		recorder.RecordStoreToSchedulerDelay(0.1)
		recorder.RecordFrontierUnusableDuration(1.0)
		recorder.RecordSliceZeroCommitRefresh()
		recorder.SetClaimsOffRecordedCache(1)
		recorder.SetMaxAbsCacheError(1)
	})
}

func TestSynchronizeSkippedClaimsMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	require.InDelta(t, 0, testutil.ToFloat64(m.synchronizeSkippedClaims), 0.01)
	m.RecordSynchronizeSkippedClaim()
	m.RecordSynchronizeSkippedClaim()
	require.InDelta(t, 2, testutil.ToFloat64(m.synchronizeSkippedClaims), 0.01)
}

func requireMetricLabelValueAbsent(t *testing.T, reg *prometheus.Registry, metricName, labelName, labelValue string) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != metricName {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				require.False(t, label.GetName() == labelName && label.GetValue() == labelValue,
					"metric %s has unexpected label %s=%q", metricName, labelName, labelValue)
			}
		}
	}
}

func TestDefragMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.SetDefragState(DefragState{
		ExcessUncoreCaches:       3,
		LargestAlignableFreeCPUs: map[int]int{0: 8, 1: 2},
	})
	m.RecordDefragPass(ResultSuccess, 40*time.Millisecond)
	m.RecordDefragMoves(ResultSuccess, 2)
	m.RecordDefragMoves(ResultError, 1)
	m.RecordDefragBlockedMoves(4)

	require.InDelta(t, 3, testutil.ToFloat64(m.defragExcessUncoreCaches), 0.01)
	require.InDelta(t, 8, testutil.ToFloat64(m.defragAlignableFreeCPUs.WithLabelValues("0")), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.defragAlignableFreeCPUs.WithLabelValues("1")), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.defragPasses.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.defragMoves.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.defragMoves.WithLabelValues(ResultError.String())), 0.01)
	require.InDelta(t, 4, testutil.ToFloat64(m.defragBlockedMoves), 0.01)
	require.Equal(t, 1, testutil.CollectAndCount(m.defragPassDurationSecondsHist))

	// A pass that could not measure a node must not leave its last reading
	// standing, or an alert would keep firing on a number nothing is producing.
	m.SetDefragState(DefragState{LargestAlignableFreeCPUs: map[int]int{0: 4}})
	require.InDelta(t, 4, testutil.ToFloat64(m.defragAlignableFreeCPUs.WithLabelValues("0")), 0.01)
	require.Equal(t, 1, testutil.CollectAndCount(m.defragAlignableFreeCPUs))

	// Counters are only touched when there is something to count, so an idle
	// pass does not make a zero look like an observation.
	m.RecordDefragMoves(ResultSuccess, 0)
	m.RecordDefragBlockedMoves(0)
	require.InDelta(t, 2, testutil.ToFloat64(m.defragMoves.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 4, testutil.ToFloat64(m.defragBlockedMoves), 0.01)
}

func TestPrepareNoWitnessMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	require.InDelta(t, 0, testutil.ToFloat64(m.prepareNoWitness), 0.01)
	m.RecordPrepareNoWitness()
	m.RecordPrepareNoWitness()
	require.InDelta(t, 2, testutil.ToFloat64(m.prepareNoWitness), 0.01)
}
func TestPromiseAndSliceMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.RecordFrontierAdmissionOutcome("started_split")
	m.RecordFrontierAdmissionOutcome("waited_prepare")
	m.SetFrontierOldestObligationSeconds(42.5)
	m.RecordRepairRounds("1", "1")
	m.RecordTimeToAlignment(3.2)
	m.RecordSliceWritesPerRound(2)
	m.RecordSliceUpdate(1024)
	m.RecordSliceUpdateConflict()
	m.RecordSliceUpdateValidationError()
	m.RecordSliceUpdateRateLimit()
	m.RecordSliceUpdateRetryDepth(3)
	m.RecordStoreToDriverDelay(0.15)
	m.RecordStoreToSchedulerDelay(0.25)
	m.RecordFrontierUnusableDuration(12.0)
	m.RecordSliceZeroCommitRefresh()
	m.SetClaimsOffRecordedCache(5)
	m.SetMaxAbsCacheError(2)

	require.InDelta(t, 1, testutil.ToFloat64(m.frontierAdmissions.WithLabelValues("started_split")), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.frontierAdmissions.WithLabelValues("waited_prepare")), 0.01)
	require.InDelta(t, 0, testutil.ToFloat64(m.frontierAdmissions.WithLabelValues("finished_aligned")), 0.01)
	require.InDelta(t, 42.5, testutil.ToFloat64(m.frontierOldestObligationSeconds), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.repairRounds.WithLabelValues("1", "1")), 0.01)
	require.Equal(t, 1, testutil.CollectAndCount(m.timeToAlignmentSeconds))
	require.Equal(t, 1, testutil.CollectAndCount(m.sliceWritesPerRound))
	require.InDelta(t, 1, testutil.ToFloat64(m.sliceUpdates), 0.01)
	require.InDelta(t, 1024, testutil.ToFloat64(m.sliceUpdateBytes), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.sliceUpdateConflicts), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.sliceUpdateValidationErrors), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.sliceUpdateRateLimits), 0.01)
	require.Equal(t, 1, testutil.CollectAndCount(m.sliceUpdateRetryDepth))
	require.Equal(t, 1, testutil.CollectAndCount(m.storeToDriverDelaySeconds))
	require.Equal(t, 1, testutil.CollectAndCount(m.storeToSchedulerDelaySeconds))
	require.Equal(t, 1, testutil.CollectAndCount(m.frontierUnusableDurationSeconds))
	require.InDelta(t, 1, testutil.ToFloat64(m.sliceZeroCommitRefreshes), 0.01)
	require.InDelta(t, 5, testutil.ToFloat64(m.claimsOffRecordedCache), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.capacityMirrorMaxAbsCacheError), 0.01)
}
