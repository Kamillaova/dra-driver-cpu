package driver

import (
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

func TestMetricsFrontierAdmissionAndAlignment(t *testing.T) {
	logger := testr.New(t)
	reg := prometheus.NewRegistry()
	recorder := cpumetrics.New(reg)

	topo := &cpuinfo.CPUTopology{
		NumCPUs:      8,
		NumCores:     4,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails: map[int]cpuinfo.CPUInfo{
			0: {CoreID: 0, SocketID: 0, NUMANodeID: 0},
			1: {CoreID: 1, SocketID: 0, NUMANodeID: 0},
			2: {CoreID: 2, SocketID: 0, NUMANodeID: 0},
			3: {CoreID: 3, SocketID: 0, NUMANodeID: 0},
			4: {CoreID: 4, SocketID: 0, NUMANodeID: 0},
			5: {CoreID: 5, SocketID: 0, NUMANodeID: 0},
			6: {CoreID: 6, SocketID: 0, NUMANodeID: 0},
			7: {CoreID: 7, SocketID: 0, NUMANodeID: 0},
		},
	}

	allocStore := store.NewCPUAllocation(topo, cpuset.New())
	driver := &CPUDriver{
		driverName:         "dra-driver-cpu.k8s.io",
		cpuAllocationStore: allocStore,
		claimTracker:       store.NewClaimTracker(),
		metrics:            recorder,
		promiseObligations: make(map[types.UID]*promiseObligation),
		topology: deviceTopology{
			cpuTopology: topo,
			onlineCPUs:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
		},
	}

	claimUID := types.UID("claim-split-1")
	driver.promiseObligations[claimUID] = &promiseObligation{
		claimUID:         claimUID,
		prepareTime:      time.Now().Add(-2 * time.Second),
		advertisedRounds: "2",
		actualRounds:     1,
	}
	driver.metricsRecorder().RecordFrontierAdmissionOutcome("started_split")
	driver.refreshObligationMetrics()

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_frontier_admissions_total", map[string]string{"outcome": "started_split"}))
	require.Greater(t, metricValue(t, reg, "dra_cpu_frontier_oldest_obligation_seconds", nil), 1.0)

	move := defrag.Move{
		ClaimUID: claimUID,
		From:     cpuset.New(0, 4),
		To:       cpuset.New(0, 1),
	}
	scope := defragScope{numaNodeID: 0, partition: "default"}

	rec := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "req", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive},
		},
		Alignment: v1alpha1.AlignmentRepairable,
	}
	require.NoError(t, allocStore.ReserveResourceClaimAllocation(logger, claimUID, rec, false))

	driver.checkClaimAlignmentOutcome(logger, scope, move)

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_frontier_admissions_total", map[string]string{"outcome": "finished_aligned"}))
	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_repair_rounds_total", map[string]string{"advertised_rounds": "2", "actual_rounds": "1"}))
	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_time_to_alignment_seconds", nil))
	require.Equal(t, float64(0), metricValue(t, reg, "dra_cpu_frontier_oldest_obligation_seconds", nil))
	require.Empty(t, driver.promiseObligations)
}

func TestMetricsFrontierUnprepareRemainedUnrepaired(t *testing.T) {
	logger := testr.New(t)
	reg := prometheus.NewRegistry()
	recorder := cpumetrics.New(reg)

	topo := &cpuinfo.CPUTopology{
		NumCPUs:      4,
		NumCores:     2,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails: map[int]cpuinfo.CPUInfo{
			0: {CoreID: 0, SocketID: 0, NUMANodeID: 0},
			1: {CoreID: 1, SocketID: 0, NUMANodeID: 0},
			2: {CoreID: 2, SocketID: 0, NUMANodeID: 0},
			3: {CoreID: 3, SocketID: 0, NUMANodeID: 0},
		},
	}

	allocStore := store.NewCPUAllocation(topo, cpuset.New())
	driver := &CPUDriver{
		driverName:         "dra-driver-cpu.k8s.io",
		cpuAllocationStore: allocStore,
		claimTracker:       store.NewClaimTracker(),
		cdiMgr:             newMockCdiMgr(),
		metrics:            recorder,
		promiseObligations: make(map[types.UID]*promiseObligation),
		topology: deviceTopology{
			cpuTopology: topo,
			onlineCPUs:  cpuset.New(0, 1, 2, 3),
		},
	}

	claimUID := types.UID("claim-unrepaired")
	driver.promiseObligations[claimUID] = &promiseObligation{
		claimUID:         claimUID,
		prepareTime:      time.Now(),
		advertisedRounds: "1",
	}
	driver.refreshObligationMetrics()
	require.GreaterOrEqual(t, metricValue(t, reg, "dra_cpu_frontier_oldest_obligation_seconds", nil), 0.0)

	err := driver.unprepareResourceClaim(logger, kubeletplugin.NamespacedObject{UID: claimUID})
	require.NoError(t, err)

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_frontier_admissions_total", map[string]string{"outcome": "remained_unrepaired"}))
	require.Equal(t, float64(0), metricValue(t, reg, "dra_cpu_frontier_oldest_obligation_seconds", nil))
	require.Empty(t, driver.promiseObligations)
}

func TestMetricsMirrorDowngradeGateAndMaxAbsError(t *testing.T) {
	logger := testr.New(t)
	reg := prometheus.NewRegistry()
	recorder := cpumetrics.New(reg)

	topo := &cpuinfo.CPUTopology{
		NumCPUs:      4,
		NumCores:     2,
		NumSockets:   1,
		NumNUMANodes: 1,
		CPUDetails: map[int]cpuinfo.CPUInfo{
			0: {CoreID: 0, SocketID: 0, NUMANodeID: 0},
			1: {CoreID: 1, SocketID: 0, NUMANodeID: 0},
			2: {CoreID: 2, SocketID: 0, NUMANodeID: 0},
			3: {CoreID: 3, SocketID: 0, NUMANodeID: 0},
		},
	}

	allocStore := store.NewCPUAllocation(topo, cpuset.New())
	driver := &CPUDriver{
		driverName:         "dra-driver-cpu.k8s.io",
		cpuAllocationStore: allocStore,
		claimTracker:       store.NewClaimTracker(),
		metrics:            recorder,
		topology: deviceTopology{
			cpuTopology: topo,
			onlineCPUs:  cpuset.New(0, 1, 2, 3),
			deviceNameToCPUs: map[string]cpuset.CPUSet{
				"cache-0": cpuset.New(0, 1),
				"cache-1": cpuset.New(2, 3),
			},
		},
	}

	claimUID := types.UID("claim-off-cache")
	rec := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "req", CPUs: cpuset.New(2, 3), Role: store.RoleExclusive},
		},
		Recorded: map[string]int{
			"cache-0": 2,
		},
	}
	require.NoError(t, allocStore.ReserveResourceClaimAllocation(logger, claimUID, rec, false))

	driver.refreshMirrorMetrics()

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_claims_off_recorded_cache", nil))
	require.Greater(t, metricValue(t, reg, "dra_cpu_capacity_mirror_max_abs_cache_error", nil), 0.0)
}

func TestMetricsFrontierUnusableDurationOnLiftPoison(t *testing.T) {
	logger := testr.New(t)
	reg := prometheus.NewRegistry()
	recorder := cpumetrics.New(reg)

	driver := &CPUDriver{
		driverName:     "dra-driver-cpu.k8s.io",
		metrics:        recorder,
		poisonedNodes:  make(map[int]*poisonedNode),
		pendingRounds:  make(map[defragScope]*defragRound),
		claimTracker:   store.NewClaimTracker(),
		podConfigStore: store.NewPodConfig(),
	}

	driver.poisonedNodes[0] = &poisonedNode{
		since:  time.Now().Add(-100 * time.Millisecond),
		scopes: map[defragScope]struct{}{},
	}

	driver.liftPoison(logger, 0)

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_frontier_unusable_duration_seconds", nil))
	require.False(t, driver.nodeIsPoisoned(0))
}

func TestMetricsSliceWriteAmplificationAndZeroCommit(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := cpumetrics.New(reg)

	recorder.RecordSliceWritesPerRound(3)
	recorder.RecordSliceZeroCommitRefresh()
	recorder.RecordStoreToDriverDelay(0.05)
	recorder.RecordStoreToSchedulerDelay(0.08)

	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_slice_writes_per_round", nil))
	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_slice_zero_commit_refreshes_total", nil))
	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_store_to_driver_delay_seconds", nil))
	require.Equal(t, float64(1), metricValue(t, reg, "dra_cpu_store_to_scheduler_delay_seconds", nil))
}
