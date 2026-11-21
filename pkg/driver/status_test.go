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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/cpuset"
)

// statusClient is an API client that takes every update of a claim's status,
// for tests whose claims exist only as the kubelet hands them over.
func statusClient() *fake.Clientset {
	client := fake.NewClientset()
	client.PrependReactor("update", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(k8stesting.UpdateAction)
		if !ok || action.GetSubresource() != "status" {
			return false, nil, nil
		}
		return true, update.GetObject(), nil
	})
	return client
}

// statusWrites returns the claims' status updates the client received.
func statusWrites(t *testing.T, client *fake.Clientset) []k8stesting.UpdateActionImpl {
	t.Helper()
	var writes []k8stesting.UpdateActionImpl
	for _, action := range client.Actions() {
		update, ok := action.(k8stesting.UpdateActionImpl)
		if ok && update.GetResource().Resource == "resourceclaims" && update.GetSubresource() == "status" {
			writes = append(writes, update)
		}
	}
	return writes
}

// writtenStatus returns the claim a status update wrote and its entries'
// data by "device/shareID", the share left empty where the entry has none.
func writtenStatus(t *testing.T, write k8stesting.UpdateActionImpl) (*resourceapi.ResourceClaim, map[string]map[string]any) {
	t.Helper()
	claim, ok := write.GetObject().(*resourceapi.ResourceClaim)
	require.True(t, ok, "the update writes a claim")
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
	return claim, entries
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
		name: "two requests of one claim on one device, two entries",
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
			require.Equal(t, testDriverName, writes[0].UpdateOptions.FieldManager)
			written, entries := writtenStatus(t, writes[0])
			require.Equal(t, claim.UID, written.UID, "the write is for the claim prepared")
			require.Equal(t, claim.ResourceVersion, written.ResourceVersion, "the write is at the resourceVersion the claim was read at")
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

// The write is one update of the claim the kubelet handed over, at the
// resourceVersion it was read at, that replaces this driver's entries, keeps
// every other driver's as it is, and encodes each entry's data with its keys
// sorted; a claim that already carries the entries is not written. On a
// conflict the claim is read again, and must still be the one prepared, with
// the allocation prepared, before the update is retried a bounded number of
// times.
func TestPrepareUpdatesTheClaimStatus(t *testing.T) {
	ours := resourceapi.AllocatedDeviceStatus{
		Driver: testDriverName, Pool: testNodeName, Device: "cpudevnuma000-vms",
		Data: &runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1alpha1","cpuset":"1,5","kind":"ClaimPlacementStatus","partition":"vms"}`)},
	}
	other := resourceapi.AllocatedDeviceStatus{
		Driver: "gpu.example.com", Pool: "node-1", Device: "gpu0",
		Data: &runtime.RawExtension{Raw: []byte(`{"z": 1, "a": "<é>"}`)},
	}
	stale := resourceapi.AllocatedDeviceStatus{
		Driver: testDriverName, Pool: testNodeName, Device: "cpudevnuma000-vms",
		Data: &runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1alpha1","cpuset":"7","kind":"ClaimPlacementStatus","partition":"vms"}`)},
	}
	conflict := apierrors.NewConflict(schema.GroupResource{Group: "resource.k8s.io", Resource: "resourceclaims"}, "claim", errors.New("the object has been modified"))
	readAgain := func(change func(*resourceapi.ResourceClaim)) func(*resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
		return func(handed *resourceapi.ResourceClaim) *resourceapi.ResourceClaim {
			fresh := handed.DeepCopy()
			fresh.ResourceVersion = "2"
			fresh.Status.Devices = []resourceapi.AllocatedDeviceStatus{other}
			if change != nil {
				change(fresh)
			}
			return fresh
		}
	}
	for _, tc := range []struct {
		name        string
		handed      []resourceapi.AllocatedDeviceStatus
		noVersion   bool
		conflicts   int
		fresh       func(handed *resourceapi.ResourceClaim) *resourceapi.ResourceClaim
		wantErr     string
		wantGets    int
		wantUpdates int
		wantVersion string
		wantDevices []resourceapi.AllocatedDeviceStatus
	}{{
		name:        "another driver's entry kept as it is, ours added with sorted keys",
		handed:      []resourceapi.AllocatedDeviceStatus{other},
		wantUpdates: 1,
		wantVersion: "1",
		wantDevices: []resourceapi.AllocatedDeviceStatus{other, ours},
	}, {
		name:        "an entry of ours that differs replaced",
		handed:      []resourceapi.AllocatedDeviceStatus{stale, other},
		wantUpdates: 1,
		wantVersion: "1",
		wantDevices: []resourceapi.AllocatedDeviceStatus{other, ours},
	}, {
		name:   "equal entries write nothing",
		handed: []resourceapi.AllocatedDeviceStatus{other, ours},
	}, {
		name:        "a conflict reads the claim again and retries on what it holds now",
		conflicts:   1,
		fresh:       readAgain(nil),
		wantGets:    1,
		wantUpdates: 2,
		wantVersion: "2",
		wantDevices: []resourceapi.AllocatedDeviceStatus{other, ours},
	}, {
		name:        "a claim read again that carries the entries is not written",
		conflicts:   1,
		fresh:       readAgain(func(fresh *resourceapi.ResourceClaim) { fresh.Status.Devices = append(fresh.Status.Devices, ours) }),
		wantGets:    1,
		wantUpdates: 1,
	}, {
		name:        "a claim replaced since it was read fails Prepare",
		conflicts:   1,
		fresh:       readAgain(func(fresh *resourceapi.ResourceClaim) { fresh.UID = "replacement" }),
		wantErr:     "claim vms/claim: status not written: the claim was replaced by one with UID replacement",
		wantGets:    1,
		wantUpdates: 1,
	}, {
		name:      "an allocation changed since it was read fails Prepare",
		conflicts: 1,
		fresh: readAgain(func(fresh *resourceapi.ResourceClaim) {
			fresh.Status.Allocation.Devices.Results[0].Device = "cpudevnuma001-vms"
		}),
		wantErr:     "claim vms/claim: status not written: its allocation changed since it was prepared",
		wantGets:    1,
		wantUpdates: 1,
	}, {
		name:        "conflicts on every update fail Prepare after five",
		conflicts:   -1,
		fresh:       readAgain(nil),
		wantErr:     `claim vms/claim: status not written: Operation cannot be fulfilled on resourceclaims.resource.k8s.io "claim": the object has been modified`,
		wantGets:    4,
		wantUpdates: 5,
	}, {
		name:        "a claim without a resourceVersion is read first",
		noVersion:   true,
		fresh:       readAgain(nil),
		wantGets:    1,
		wantUpdates: 1,
		wantVersion: "2",
		wantDevices: []resourceapi.AllocatedDeviceStatus{other, ours},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := allowingDriver(t, true, false, nil)
			claim := claimWithResults("claim", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})
			claim.Status.Devices = tc.handed
			if tc.noVersion {
				claim.ResourceVersion = ""
			}
			client := fake.NewClientset()
			conflicts := tc.conflicts
			client.PrependReactor("update", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
				update, ok := action.(k8stesting.UpdateAction)
				if !ok || action.GetSubresource() != "status" {
					return false, nil, nil
				}
				if conflicts != 0 {
					conflicts--
					return true, nil, conflict
				}
				return true, update.GetObject(), nil
			})
			client.PrependReactor("get", "resourceclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
				if tc.fresh == nil {
					return false, nil, nil
				}
				return true, tc.fresh(claim), nil
			})
			d.kubeClient = client

			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{claim.DeepCopy()})
			require.NoError(t, err)
			if tc.wantErr != "" {
				require.EqualError(t, results["claim"].Err, tc.wantErr)
			} else {
				require.NoError(t, results["claim"].Err)
			}
			gets := 0
			for _, action := range client.Actions() {
				if action.GetVerb() == "get" && action.GetResource().Resource == "resourceclaims" {
					gets++
				}
			}
			require.Equal(t, tc.wantGets, gets, "reads of the claim")
			writes := statusWrites(t, client)
			require.Len(t, writes, tc.wantUpdates, "updates of the claim's status")
			if tc.wantDevices == nil {
				return
			}
			written, _ := writtenStatus(t, writes[len(writes)-1])
			require.Equal(t, testDriverName, writes[len(writes)-1].UpdateOptions.FieldManager)
			require.Equal(t, tc.wantVersion, written.ResourceVersion)
			require.Equal(t, claim.UID, written.UID)
			require.Equal(t, claim.Status.Allocation, written.Status.Allocation)
			require.Equal(t, tc.wantDevices, written.Status.Devices)
		})
	}
}

// A failed write fails Prepare and keeps the record, so the kubelet's next
// attempt replays it and writes again.
func TestPrepareFailsWhenTheStatusWriteFails(t *testing.T) {
	d := allowingDriver(t, true, false, nil)
	client := fake.NewClientset()
	failing := true
	client.PrependReactor("update", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		if failing {
			return true, nil, errors.New("etcdserver: request timed out")
		}
		return true, update.GetObject(), nil
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
	client.PrependReactor("update", "resourceclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(k8stesting.UpdateAction)
		if !ok {
			return false, nil, nil
		}
		close(entered)
		<-release
		return true, update.GetObject(), nil
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
