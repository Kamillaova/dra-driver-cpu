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
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/coreselect"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

func newCustomFrontierDriver(t *testing.T, infos []cpuinfo.CPUInfo, partitions []devattr.Partition, groupBy string) (*CPUDriver, *mockKubeletPlugin) {
	t.Helper()
	cp, err := New(testr.New(t), Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:   testSysFS(infos),
	}, &Config{
		DriverName:             testDriverName,
		NodeName:               testNodeName,
		KubeletRootDir:         "/var/lib/kubelet",
		CPUDeviceMode:          devattr.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy:       groupBy,
		CachePlacementStrategy: coreselect.Pack,
		CPUPartitions:          partitions,
	})
	require.NoError(t, err)
	plugin := &mockKubeletPlugin{}
	cp.draPlugin = plugin
	cp.cdiMgr = newMockCdiMgr()
	return cp, plugin
}

func fourCacheInfos() []cpuinfo.CPUInfo {
	var infos []cpuinfo.CPUInfo
	for cpu := range 16 {
		infos = append(infos, cpuinfo.CPUInfo{
			CpuID:         cpu,
			CoreID:        cpu,
			SocketID:      0,
			NUMANodeID:    0,
			UncoreCacheID: cpu / 4,
		})
	}
	return infos
}

func TestFrontierAttributeFormatAndScoping(t *testing.T) {
	infos := smtPoolInfos()
	partitions := []devattr.Partition{
		{Name: "helpers", Role: devattr.PARTITION_ROLE_SHARED, CPUs: cpuset.New(0, 8)},
	}
	cp, _ := newCustomFrontierDriver(t, infos, partitions, devattr.GROUP_BY_UNCORE_CACHE)

	slices := cp.refreshDeviceOrder()
	require.NotEmpty(t, slices)

	var exclusiveChecked, poolChecked int
	for _, slice := range slices {
		for _, dev := range slice {
			rAttr, hasRole := dev.Attributes[devattr.AttributeRole]
			require.True(t, hasRole)
			require.NotNil(t, rAttr.StringValue)

			val, hasFrontier := dev.Attributes[devattr.AttributeRepairRounds]
			inputVal, hasInput := dev.Attributes[devattr.AttributeFrontierInput]
			if *rAttr.StringValue == devattr.PARTITION_ROLE_SHARED {
				require.False(t, hasFrontier)
				require.False(t, hasInput)
				poolChecked++
				continue
			}

			require.True(t, hasFrontier)
			require.NotNil(t, val.StringValue)
			require.True(t, hasInput)
			require.NotNil(t, inputVal.StringValue)
			fields := strings.Split(*val.StringValue, ",")
			require.Len(t, fields, api.RepairRoundsFields)
			for _, f := range fields {
				switch f {
				case "1", "2", "3", api.RepairRoundsUnreachable:
				default:
					t.Fatalf("unexpected repair round field: %q", f)
				}
			}
			require.Len(t, *inputVal.StringValue, 64)
			exclusiveChecked++
		}
	}
	require.Positive(t, exclusiveChecked)
	require.Positive(t, poolChecked)

	numaCP, _ := newCustomFrontierDriver(t, infos, nil, devattr.GROUP_BY_NUMA_NODE)
	numaSlices := numaCP.refreshDeviceOrder()
	for _, slice := range numaSlices {
		for _, dev := range slice {
			_, hasFrontier := dev.Attributes[devattr.AttributeRepairRounds]
			require.False(t, hasFrontier)
		}
	}
}

func TestFrontierOneRoundRepair(t *testing.T) {
	infos := fourCacheInfos()
	cp, _ := newCustomFrontierDriver(t, infos, nil, devattr.GROUP_BY_UNCORE_CACHE)

	placeCharged(t, cp, "c0", "cpudevcache000", cpuset.New(0, 1))
	placeCharged(t, cp, "c1", "cpudevcache001", cpuset.New(4, 5))
	placeCharged(t, cp, "c3", "cpudevcache003", cpuset.New(12, 13, 14, 15))

	slices := cp.refreshDeviceOrder()
	require.NotEmpty(t, slices)

	for _, slice := range slices {
		for _, dev := range slice {
			val, hasFrontier := dev.Attributes[devattr.AttributeRepairRounds]
			require.True(t, hasFrontier)
			require.NotNil(t, val.StringValue)
			fields := strings.Split(*val.StringValue, ",")
			require.Len(t, fields, 4)
			require.Equal(t, "1", fields[0])
		}
	}
}

func TestFrontierPoisonedNode(t *testing.T) {
	infos := fourCacheInfos()
	cp, _ := newCustomFrontierDriver(t, infos, nil, devattr.GROUP_BY_UNCORE_CACHE)

	cp.poisonedNodes = map[int]*poisonedNode{
		0: {
			since: time.Now(),
		},
	}

	slices := cp.refreshDeviceOrder()
	require.NotEmpty(t, slices)

	for _, slice := range slices {
		for _, dev := range slice {
			val, hasFrontier := dev.Attributes[devattr.AttributeRepairRounds]
			require.True(t, hasFrontier)
			require.NotNil(t, val.StringValue)
			require.Equal(t, "-,-,-,-", *val.StringValue)

			_, hasInput := dev.Attributes[devattr.AttributeFrontierInput]
			require.False(t, hasInput)
		}
	}
}

func TestFrontierStalenessLifecycle(t *testing.T) {
	infos := fourCacheInfos()
	cp, _ := newCustomFrontierDriver(t, infos, nil, devattr.GROUP_BY_UNCORE_CACHE)

	cp.PublishResources(context.Background())
	require.False(t, cp.publishedSlicesAreStale())

	placeCharged(t, cp, "claim-occupy", "cpudevcache000", cpuset.New(0, 1))
	require.True(t, cp.publishedSlicesAreStale())

	cp.PublishResources(context.Background())
	require.False(t, cp.publishedSlicesAreStale())
}

func TestFrontierInputDigestChangesWithClaims(t *testing.T) {
	infos := fourCacheInfos()
	cp, _ := newCustomFrontierDriver(t, infos, nil, devattr.GROUP_BY_UNCORE_CACHE)

	// Clean state
	slices1 := cp.refreshDeviceOrder()
	var digest1 string
	for _, slice := range slices1 {
		if len(slice) > 0 {
			if v, ok := slice[0].Attributes[devattr.AttributeFrontierInput]; ok {
				digest1 = *v.StringValue
				break
			}
		}
	}
	require.NotEmpty(t, digest1)

	// Place one claim
	placeCharged(t, cp, "claim-1", "cpudevcache000", cpuset.New(0, 1))
	slices2 := cp.refreshDeviceOrder()
	var digest2 string
	for _, slice := range slices2 {
		if len(slice) > 0 {
			if v, ok := slice[0].Attributes[devattr.AttributeFrontierInput]; ok {
				digest2 = *v.StringValue
				break
			}
		}
	}
	require.NotEmpty(t, digest2)
	require.NotEqual(t, digest1, digest2)
}
