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
	require.Len(t, descriptors, 22)

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
		"dra_cpu_nri_synchronize_duration_seconds",
		"dra_cpu_nri_create_container_duration_seconds",
		"dra_cpu_nri_stop_container_duration_seconds",
		"dra_cpu_nri_remove_container_duration_seconds",
		"dra_cpu_nri_synchronize_refused_claims_total",
		"dra_cpu_containers_kept_under_older_rule_total",
		"dra_cpu_watchdog_stuck_seconds",
		"dra_cpu_node_allocatable_mapping_dropped_total",
		"dra_cpu_misplaced_claims_total",
		"dra_cpu_partition_verified",
		"dra_cpu_node_reservation_ok",
		"dra_cpu_device_taints_dropped_total",
		"dra_cpu_threaded_containers_total",
	}, names)
	require.Equal(t, []string{"result"}, descriptors[4].Labels)
	require.Equal(t, []string{"result"}, descriptors[5].Labels)
	require.Empty(t, descriptors[13].Labels)
	require.Equal(t, []string{"rule"}, descriptors[14].Labels)
	require.Empty(t, descriptors[15].Labels)
	require.Empty(t, descriptors[16].Labels)
	require.Empty(t, descriptors[17].Labels)
	require.Empty(t, descriptors[19].Labels)
	require.Empty(t, descriptors[20].Labels)
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

	require.Equal(t, []string{
		"dra_cpu_allocated_cpus",
		"dra_cpu_available_cpus",
		"dra_cpu_claim_allocated_cpus",
		"dra_cpu_containers_kept_under_older_rule_total",
		"dra_cpu_device_taints_dropped_total",
		"dra_cpu_misplaced_claims_total",
		"dra_cpu_node_allocatable_mapping_dropped_total",
		"dra_cpu_node_reservation_ok",
		"dra_cpu_nri_create_container_duration_seconds",
		"dra_cpu_nri_remove_container_duration_seconds",
		"dra_cpu_nri_stop_container_duration_seconds",
		"dra_cpu_nri_synchronize_duration_seconds",
		"dra_cpu_nri_synchronize_refused_claims_total",
		"dra_cpu_prepare_claim_duration_seconds",
		"dra_cpu_prepare_claims_total",
		"dra_cpu_reserved_cpus",
		"dra_cpu_resource_claims_active",
		"dra_cpu_unprepare_claim_duration_seconds",
		"dra_cpu_unprepare_claims_total",
		"dra_cpu_watchdog_stuck_seconds",
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
	m.RecordNRISynchronizeRefusedClaim()
	m.RecordNRISynchronizeRefusedClaim()
	require.InDelta(t, 0, testutil.ToFloat64(m.containersKeptUnderOlder.WithLabelValues(string(RulePartialHolder))), 0.01)
	require.Equal(t, 5, testutil.CollectAndCount(m.containersKeptUnderOlder), "every rule is exported before any container is kept")
	m.RecordContainerKeptUnderOlderRule(RulePartialHolder)
	m.SetWatchdogStuck(61500 * time.Millisecond)
	m.RecordNodeAllocatableMappingDropped()
	m.RecordMisplacedClaim()
	m.SetPartitionVerified("vms", true)
	m.SetPartitionVerified("spdk", false)
	require.InDelta(t, 0, testutil.ToFloat64(m.nodeReservationOK), 0.01)
	m.SetNodeReservationOK(true)
	m.RecordDeviceTaintsDropped()
	m.RecordThreadedContainer("place", ResultSuccess)
	m.RecordThreadedContainer("place", ResultRewriteError)
	m.RecordThreadedContainer("place", ResultRewriteError)
	m.RecordThreadedContainer("place", ResultRepairError)

	require.InDelta(t, 1, testutil.ToFloat64(m.threadedContainers.WithLabelValues("place", "success")), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.threadedContainers.WithLabelValues("place", "rewrite_error")), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.threadedContainers.WithLabelValues("place", "repair_error")), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.allocatedCPUs), 0.01)
	require.InDelta(t, 6, testutil.ToFloat64(m.availableCPUs), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.reservedCPUs), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.activeResourceClaims), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.prepareClaims.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.prepareClaims.WithLabelValues(ResultError.String())), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.unprepareClaims.WithLabelValues(ResultSuccess.String())), 0.01)
	require.InDelta(t, 2, testutil.ToFloat64(m.nriSynchronizeRefused), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.containersKeptUnderOlder.WithLabelValues(string(RulePartialHolder))), 0.01)
	require.InDelta(t, 61.5, testutil.ToFloat64(m.watchdogStuck), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.mappingDropped), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.misplacedClaims), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.partitionVerified.WithLabelValues("vms")), 0.01)
	require.InDelta(t, 0, testutil.ToFloat64(m.partitionVerified.WithLabelValues("spdk")), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.nodeReservationOK), 0.01)
	require.InDelta(t, 1, testutil.ToFloat64(m.deviceTaintsDropped), 0.01)

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
	m.SetPartitionVerified("default", true)
	m.RecordThreadedContainer("create", ResultSuccess)

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
		recorder.RecordNRISynchronizeRefusedClaim()
		recorder.RecordContainerKeptUnderOlderRule(RulePartialHolder)
		recorder.SetWatchdogStuck(time.Minute)
		recorder.RecordNodeAllocatableMappingDropped()
		recorder.RecordMisplacedClaim()
		recorder.SetPartitionVerified("vms", true)
		recorder.SetNodeReservationOK(false)
		recorder.RecordDeviceTaintsDropped()
		recorder.RecordThreadedContainer("create", ResultError)
	})
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
