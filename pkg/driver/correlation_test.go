package driver

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/cpuset"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/defrag"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
)

func TestClaimCorrelationCDIRoundtrip(t *testing.T) {
	logger := testr.New(t)
	tempDir := t.TempDir()
	mgr, err := NewCdiManager(logger, "dra-driver-cpu.k8s.io", filepath.Join(tempDir, "cdi"))
	require.NoError(t, err)

	numa := 1
	witnessRounds := 2
	record := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "req", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive},
		},
		Relocatable: true,
		Alignment:   v1alpha1.AlignmentRepairable,
		Recorded:    map[string]int{"cache-0": 2},
		Correlation: store.ClaimCorrelation{
			NUMANode:         &numa,
			Partition:        "default",
			FrontierSnapshot: "1",
			WitnessRounds:    &witnessRounds,
			WitnessPlan:      "claim-1:0-1->2-3",
			InitialCPUSet:    "0-1",
			RuntimeOutcome:   "started_split",
		},
	}

	err = mgr.AddDevice(logger, "claim-test-1", "DRA_CPU=0,1", record)
	require.NoError(t, err)
	require.NoError(t, mgr.Refresh())

	readBack, err := mgr.GetDeviceAllocations("claim-test-1")
	require.NoError(t, err)
	require.NotNil(t, readBack.Correlation.NUMANode)
	require.Equal(t, 1, *readBack.Correlation.NUMANode)
	require.Equal(t, "default", readBack.Correlation.Partition)
	require.Equal(t, "1", readBack.Correlation.FrontierSnapshot)
	require.NotNil(t, readBack.Correlation.WitnessRounds)
	require.Equal(t, 2, *readBack.Correlation.WitnessRounds)
	require.Equal(t, "claim-1:0-1->2-3", readBack.Correlation.WitnessPlan)
	require.Equal(t, "0-1", readBack.Correlation.InitialCPUSet)
	require.Equal(t, "started_split", readBack.Correlation.RuntimeOutcome)
}

func TestBuildClaimCorrelation(t *testing.T) {
	topo := &cpuinfo.CPUTopology{
		NumCPUs:      8,
		NumCores:     4,
		NumSockets:   1,
		NumNUMANodes: 2,
		CPUDetails: map[int]cpuinfo.CPUInfo{
			0: {CoreID: 0, SocketID: 0, NUMANodeID: 0},
			1: {CoreID: 1, SocketID: 0, NUMANodeID: 0},
			2: {CoreID: 2, SocketID: 0, NUMANodeID: 0},
			3: {CoreID: 3, SocketID: 0, NUMANodeID: 0},
			4: {CoreID: 4, SocketID: 0, NUMANodeID: 1},
			5: {CoreID: 5, SocketID: 0, NUMANodeID: 1},
			6: {CoreID: 6, SocketID: 0, NUMANodeID: 1},
			7: {CoreID: 7, SocketID: 0, NUMANodeID: 1},
		},
	}
	cp := &CPUDriver{
		topology: deviceTopology{
			cpuTopology: topo,
		},
		publishedFrontier: map[string]string{
			"default/0": "2",
		},
		metrics: cpumetrics.Noop(),
	}

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "claim-1",
			Namespace: "default",
			Name:      "test-claim",
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver: cp.driverName,
							Device: "default-cache-0",
						},
					},
				},
			},
		},
	}

	plan := &defrag.ExactPlan{
		Moves: []defrag.Move{
			{ClaimUID: "claim-1", From: cpuset.New(0, 1), To: cpuset.New(2, 3)},
		},
	}

	corr := cp.buildClaimCorrelation(claim, cpuset.New(0, 1), plan)
	require.NotNil(t, corr.NUMANode)
	require.Equal(t, 0, *corr.NUMANode)
	require.Equal(t, "default", corr.Partition)
	require.Equal(t, "2", corr.FrontierSnapshot)
	require.NotNil(t, corr.WitnessRounds)
	require.Equal(t, 1, *corr.WitnessRounds)
	require.Contains(t, corr.WitnessPlan, "claim-1:0-1->2-3")
	require.Equal(t, "0-1", corr.InitialCPUSet)
}

func TestPublishClaimPlacementStatusWithCorrelation(t *testing.T) {
	numa := 0
	witnessRounds := 1
	rec := store.ClaimRecord{
		Requests: []store.RequestAllocation{
			{Request: "req", CPUs: cpuset.New(0, 1), Role: store.RoleExclusive},
		},
		Correlation: store.ClaimCorrelation{
			NUMANode:         &numa,
			Partition:        "default",
			FrontierSnapshot: "1",
			WitnessRounds:    &witnessRounds,
			WitnessPlan:      "claim-test:0-1->2-3",
			InitialCPUSet:    "0-1",
			RuntimeOutcome:   "aligned",
		},
	}

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
	require.NoError(t, allocStore.ReserveResourceClaimAllocation(testr.New(t), "claim-test", rec, false))

	claim := &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "claim-test",
			Namespace: "default",
			Name:      "test-claim",
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Driver: "dra-driver-cpu.k8s.io",
							Device: "dev-0",
						},
					},
				},
			},
		},
	}

	fakeClient := k8sfake.NewSimpleClientset(claim)
	fakeReader := &fakeClaimReader{
		claims: []*resourceapi.ResourceClaim{claim},
	}

	driver := &CPUDriver{
		driverName:         "dra-driver-cpu.k8s.io",
		kubeClient:         fakeClient,
		claimReader:        fakeReader,
		cpuAllocationStore: allocStore,
		topology: deviceTopology{
			deviceNameToCPUs: map[string]cpuset.CPUSet{
				"dev-0": cpuset.New(0, 1),
			},
		},
		metrics: cpumetrics.Noop(),
	}

	err := driver.doPublishClaimPlacementStatus(context.Background(), testr.New(t), "claim-test")
	require.NoError(t, err)

	updated, err := fakeClient.ResourceV1().ResourceClaims("default").Get(context.Background(), "test-claim", metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, updated.Status.Devices, 1)
	require.NotNil(t, updated.Status.Devices[0].Data)

	var placement v1alpha1.ClaimPlacementStatus
	err = json.Unmarshal(updated.Status.Devices[0].Data.Raw, &placement)
	require.NoError(t, err)
	require.Equal(t, "0-1", placement.CPUSet)
	require.Equal(t, 2, placement.CPUCount)
	require.NotNil(t, placement.NUMANode)
	require.Equal(t, 0, *placement.NUMANode)
	require.Equal(t, "default", placement.Partition)
	require.Equal(t, "1", placement.FrontierSnapshot)
	require.NotNil(t, placement.WitnessRounds)
	require.Equal(t, 1, *placement.WitnessRounds)
	require.Equal(t, "claim-test:0-1->2-3", placement.WitnessPlan)
	require.Equal(t, "0-1", placement.InitialCPUSet)
	require.Equal(t, "aligned", placement.RuntimeOutcome)
}
