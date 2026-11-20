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

package driver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/cpuset"
)

// statusClient is an API client that takes every write of a claim's status,
// for tests whose claims exist only as the kubelet hands them over.
func statusClient() *fake.Clientset {
	client := fake.NewClientset()
	client.PrependReactor("patch", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return action.GetSubresource() == "status", &resourceapi.ResourceClaim{}, nil
	})
	return client
}

// statusWrites returns the claims' status writes the client received.
func statusWrites(t *testing.T, client *fake.Clientset) []k8stesting.PatchActionImpl {
	t.Helper()
	var writes []k8stesting.PatchActionImpl
	for _, action := range client.Actions() {
		patch, ok := action.(k8stesting.PatchActionImpl)
		if ok && patch.GetResource().Resource == "resourceclaims" && patch.GetSubresource() == "status" {
			writes = append(writes, patch)
		}
	}
	return writes
}

// writtenStatus decodes a status write and returns its entries' data by
// "device/shareID", the share left empty where the entry has none.
func writtenStatus(t *testing.T, write k8stesting.PatchActionImpl) (*resourceapi.ResourceClaim, map[string]map[string]any) {
	t.Helper()
	var claim resourceapi.ResourceClaim
	require.NoError(t, json.Unmarshal(write.GetPatch(), &claim))
	entries := map[string]map[string]any{}
	for _, entry := range claim.Status.Devices {
		require.NotNilf(t, entry.Data, "entry of device %s has no data", entry.Device)
		var data map[string]any
		require.NoError(t, json.Unmarshal(entry.Data.Raw, &data))
		key := entry.Device + "/"
		if entry.ShareID != nil {
			key += *entry.ShareID
		}
		require.NotContainsf(t, entries, key, "two entries for %s", key)
		entries[key] = data
	}
	return &claim, entries
}

func placement(partition, cpus string) map[string]any {
	data := map[string]any{"apiVersion": "v1alpha1", "kind": "ClaimPlacementStatus", "partition": partition}
	if cpus != "" {
		data["cpuset"] = cpus
	}
	return data
}

func withShareIDs(claim *resourceapi.ResourceClaim, ids ...types.UID) *resourceapi.ResourceClaim {
	for i, id := range ids {
		if id != "" {
			claim.Status.Allocation.Devices.Results[i].ShareID = new(id)
		}
	}
	return claim
}

// Prepare writes one status entry per result of the claim on this driver's
// devices, keyed by its device and share as the API server keys them: each
// names the device's partition, and an exclusive request's names the CPUs it
// holds on that device, a share's none.
func TestPrepareWritesTheClaimStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		claim   func() *resourceapi.ResourceClaim
		want    map[string]map[string]any
		wantErr string
	}{{
		name: "a share, a flagged request and an owned request",
		claim: func() *resourceapi.ResourceClaim {
			claim := claimWithResults("claim",
				claimResult{request: "vmm-cpu", consumed: "0", device: "cpudevnuma000"},
				claimResult{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"},
				claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})
			withShareIDs(claim, "share-1", "share-2", "share-3")
			return withConfig(claim, resourceapi.AllocationConfigSourceClaim, []string{"cpus"}, exclusiveParams)
		},
		want: map[string]map[string]any{
			"cpudevnuma000/share-1":     placement("default", ""),
			"cpudevnuma000/share-2":     placement("default", "2-3"),
			"cpudevnuma000-vms/share-3": placement("vms", "1,5"),
		},
	}, {
		name: "X3: two requests of one claim on one device, two entries",
		claim: func() *resourceapi.ResourceClaim {
			claim := claimWithResults("claim",
				claimResult{request: "a", written: "1", consumed: "1", device: "cpudevnuma000"},
				claimResult{request: "b", consumed: "0", device: "cpudevnuma000"})
			withShareIDs(claim, "share-a", "share-b")
			return withConfig(claim, resourceapi.AllocationConfigSourceClaim, []string{"a"}, exclusiveParams)
		},
		want: map[string]map[string]any{
			"cpudevnuma000/share-a": placement("default", "2"),
			"cpudevnuma000/share-b": placement("default", ""),
		},
	}, {
		name: "a result without a share",
		claim: func() *resourceapi.ResourceClaim {
			return claimWithResults("claim", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})
		},
		want: map[string]map[string]any{"cpudevnuma000-vms/": placement("vms", "1,5")},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := allowingDriver(t, true, false, nil)
			client := statusClient()
			d.kubeClient = client
			claim := tc.claim()
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			require.NoError(t, results["claim"].Err)

			writes := statusWrites(t, client)
			require.Len(t, writes, 1, "one write per Prepare")
			require.Equal(t, types.ApplyPatchType, writes[0].GetPatchType())
			require.Equal(t, testDriverName, writes[0].GetPatchOptions().FieldManager)
			require.True(t, *writes[0].GetPatchOptions().Force, "the driver owns its entries")
			written, entries := writtenStatus(t, writes[0])
			require.Equal(t, claim.UID, written.UID, "the write is for the claim prepared")
			require.Equal(t, tc.want, entries)
			for _, entry := range written.Status.Devices {
				require.Equal(t, testDriverName, entry.Driver)
				require.Equal(t, testNodeName, entry.Pool)
			}
		})
	}
}

// On a node without partitions every device is owned and named "default", and
// a result without a share, as individual mode's devices give, has an entry
// without one.
func TestPrepareWritesTheClaimStatusInIndividualMode(t *testing.T) {
	client := statusClient()
	d, err := New(testr.New(t), Providers{K8SClient: client, CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mixedCoreCPUInfos}, SysFS: partitionSysFS()}, &Config{
		DriverName:    testDriverName,
		NodeName:      testNodeName,
		ReservedCPUs:  cpuset.New(),
		CPUDeviceMode: device.CPU_DEVICE_MODE_INDIVIDUAL,
	})
	require.NoError(t, err)
	d.cdiMgr = newMockCdiMgr()
	d.hasSynchronized = true
	claim := testClaimWithResults("claim", []resourceapi.DeviceRequestAllocationResult{
		{Driver: testDriverName, Pool: testNodeName, Device: "cpudev002", Request: "cpus"},
		{Driver: testDriverName, Pool: testNodeName, Device: "cpudev003", Request: "cpus"},
	})
	results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results[claim.UID].Err)
	writes := statusWrites(t, client)
	require.Len(t, writes, 1)
	_, entries := writtenStatus(t, writes[0])
	require.Equal(t, map[string]map[string]any{
		"cpudev002/": placement("default", cpuset.New(d.topology.NameToCPUID["cpudev002"]).String()),
		"cpudev003/": placement("default", cpuset.New(d.topology.NameToCPUID["cpudev003"]).String()),
	}, entries)
}

// A failed write fails Prepare and keeps the record, so the kubelet's next
// attempt replays it and writes again; every replay writes.
func TestPrepareFailsWhenTheStatusWriteFails(t *testing.T) {
	d := allowingDriver(t, true, false, nil)
	client := fake.NewClientset()
	failing := true
	client.PrependReactor("patch", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failing {
			return true, nil, errors.New("etcdserver: request timed out")
		}
		return true, &resourceapi.ResourceClaim{}, nil
	})
	d.kubeClient = client
	claim := claimWithResults("claim", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})

	results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.EqualError(t, results["claim"].Err, "claim vms/claim: status not written: etcdserver: request timed out")
	record, ok := d.allocationState.GetClaimRecord("claim")
	require.True(t, ok, "the record stays for the retry")

	failing = false
	for attempt := range 2 {
		results, err = d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
		require.NoError(t, err)
		require.NoError(t, results["claim"].Err, "attempt %d", attempt)
		again, ok := d.allocationState.GetClaimRecord("claim")
		require.True(t, ok)
		require.Equal(t, record, again, "the retry replays the record")
	}
	require.Len(t, statusWrites(t, client), 3, "the failed write and one write per replay")
}

// A Prepare that arrives with its context expired fails at once, makes no
// call to the API server, and keeps the record an earlier attempt wrote.
func TestPrepareWithAnExpiredContext(t *testing.T) {
	d := allowingDriver(t, true, false, nil)
	client := statusClient()
	d.kubeClient = client
	claim := claimWithResults("claim", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})
	results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, results["claim"].Err)
	client.ClearActions()

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	results, err = d.PrepareResourceClaims(expired, []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.ErrorIs(t, results["claim"].Err, context.Canceled)
	require.Empty(t, client.Actions(), "no call to the API server")
	_, ok := d.allocationState.GetClaimRecord("claim")
	require.True(t, ok, "the record an earlier attempt wrote stays")
}

// The write runs once Prepare has released stateMu, so an API server that
// stalls it holds no NRI hook.
func TestAStalledStatusWriteLeavesCreateContainerAnswering(t *testing.T) {
	d := allowingDriver(t, true, false, nil)
	client := fake.NewClientset()
	entered, release := make(chan struct{}), make(chan struct{})
	client.PrependReactor("patch", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		close(entered)
		<-release
		return true, &resourceapi.ResourceClaim{}, nil
	})
	d.kubeClient = client
	claim := claimWithResults("claim", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})

	prepared := make(chan error, 1)
	go func() {
		results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim})
		if err == nil {
			err = results["claim"].Err
		}
		prepared <- err
	}()
	select {
	case <-entered:
	case err := <-prepared:
		t.Fatalf("Prepare returned without writing the status: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Prepare never wrote the status")
	}

	created := make(chan error, 1)
	go func() {
		pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "vms"}
		_, _, err := d.CreateContainer(context.Background(), pod, &api.Container{Id: "plain", Name: "plain", PodSandboxId: pod.Id})
		created <- err
	}()
	select {
	case err := <-created:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("CreateContainer waited on the stalled status write")
	}
	close(release)
	require.NoError(t, <-prepared)
}
