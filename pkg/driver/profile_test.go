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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/cpuset"
)

// profiledDriver is a driver whose configuration declares the profile r7625,
// as cmd/dracpu builds it, not yet started.
func profiledDriver(t *testing.T, client kubernetes.Interface, onDisk map[types.UID]cpuset.CPUSet) *CPUDriver {
	t.Helper()
	declared := driverconfig.Default()
	declared.Profiles = map[string]driverconfig.Profile{
		"r7625": {CPUPartitions: []driverconfig.CPUPartition{{Name: device.DefaultPartitionName}}},
	}
	require.NoError(t, declared.Validate())
	infos := mockCPUInfos_SingleSocket_4CPUS_HT
	config := &Config{
		DriverName: testDriverName, NodeName: testNodeName,
		CPUDeviceMode: device.CPU_DEVICE_MODE_GROUPED, CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
		ProfileLabel: driverconfig.ProfileLabel,
	}
	config.SelectProfile = func(name string) (*Config, error) {
		if _, err := declared.WithProfile(name); err != nil {
			return nil, err
		}
		selected := *config
		selected.ProfileLabel, selected.SelectProfile = "", nil
		return &selected, nil
	}
	driver, err := New(testr.New(t), Providers{
		CPUInfo:   &cpuinfo.MockCPUInfoProvider{CPUInfos: infos},
		SysFS:     testSysFS(infos),
		K8SClient: client,
	}, config)
	require.NoError(t, err)
	driver.cdiMgr = newMockCdiMgrWithAllocations(onDisk)
	driver.profileReadInterval = time.Millisecond
	return driver
}

func labelledNode(labels map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNodeName, Labels: labels}}
}

func windowPod(id string, hostNetwork bool) *api.PodSandbox {
	namespaces := []*api.LinuxNamespace{{Type: "ipc"}, {Type: "uts"}}
	if !hostNetwork {
		namespaces = append(namespaces, &api.LinuxNamespace{Type: "network", Path: "/var/run/netns/cni-" + id})
	}
	return &api.PodSandbox{Id: id, Uid: id + "-uid", Name: id, Namespace: "default", Linux: &api.LinuxPodSandbox{Namespaces: namespaces}}
}

func windowContainer(id string, pod *api.PodSandbox, env ...string) *api.Container {
	return &api.Container{Id: id, Name: id, PodSandboxId: pod.GetId(), State: api.ContainerState_CONTAINER_RUNNING, Env: env}
}

func keptIDs(driver *CPUDriver) []string {
	driver.stateMu.Lock()
	defer driver.stateMu.Unlock()
	var ids []string
	for _, k := range driver.window.kept {
		ids = append(ids, k.ctr.GetId())
	}
	return ids
}

// Before the profile is read the driver has no stores: only a host-network
// pod's container without claims starts, on the runtime's cpuset, and every
// container the driver sees running is kept for the judgment after the read.
func TestUnconfiguredWindowHooks(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	hostPod := windowPod("host", true)
	otherPod := windowPod("other", false)
	claimEnv := fmt.Sprintf("%s_claim=0,2", cdiEnvVarPrefix)

	for _, tc := range []struct {
		name     string
		pod      *api.PodSandbox
		ctr      *api.Container
		wantErr  string
		wantKept []string
	}{{
		name:     "a host-network pod's container without claims starts on the runtime's cpuset",
		pod:      hostPod,
		ctr:      windowContainer("kube-proxy", hostPod),
		wantKept: []string{"listed", "kube-proxy"},
	}, {
		name:     "another pod's container without claims is refused",
		pod:      otherPod,
		ctr:      windowContainer("app", otherPod),
		wantErr:  `cannot create container "app" in pod "other-uid": until the driver has read the node's dra.cpu/profile label only the containers without claims of host-network pods start; the kubelet creates it again, and the driver's log says why the label is not read yet`,
		wantKept: []string{"listed"},
	}, {
		name:     "a container naming claims is refused, even in a host-network pod",
		pod:      hostPod,
		ctr:      windowContainer("vm", hostPod, claimEnv),
		wantErr:  `cannot create container "vm" in pod "host-uid": it names claims, which the driver serves only once it has read the node's dra.cpu/profile label; the kubelet creates it again, and the driver's log says why the label is not read yet`,
		wantKept: []string{"listed"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			driver := profiledDriver(t, fake.NewClientset(), nil)
			updates, err := driver.Synchronize(ctx, []*api.PodSandbox{hostPod}, []*api.Container{windowContainer("listed", hostPod, claimEnv)})
			require.NoError(t, err)
			require.Empty(t, updates, "Synchronize before the profile read")

			adjust, updates, err := driver.CreateContainer(ctx, tc.pod, tc.ctr)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, &api.ContainerAdjustment{}, adjust, "the runtime's own cpuset")
				require.Empty(t, updates)
			}
			require.Equal(t, tc.wantKept, keptIDs(driver))
		})
	}
}

// A container that stops or is removed in the window is no longer judged after
// the read, and a reconnect's Synchronize replaces the kept list.
func TestUnconfiguredWindowForgets(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	pod := windowPod("host", true)
	for _, tc := range []struct {
		name     string
		act      func(*CPUDriver) error
		wantKept []string
	}{{
		name: "StopContainer drops it",
		act: func(d *CPUDriver) error {
			_, err := d.StopContainer(ctx, pod, windowContainer("a", pod))
			return err
		},
		wantKept: []string{"b"},
	}, {
		name:     "RemoveContainer drops it",
		act:      func(d *CPUDriver) error { return d.RemoveContainer(ctx, pod, windowContainer("a", pod)) },
		wantKept: []string{"b"},
	}, {
		name: "a reconnect's Synchronize replaces the list",
		act: func(d *CPUDriver) error {
			_, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{windowContainer("c", pod)})
			return err
		},
		wantKept: []string{"c"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			driver := profiledDriver(t, fake.NewClientset(), nil)
			_, err := driver.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{windowContainer("a", pod), windowContainer("b", pod)})
			require.NoError(t, err)
			require.NoError(t, tc.act(driver))
			require.Equal(t, tc.wantKept, keptIDs(driver))
		})
	}
}

// With the API server unreachable the driver stays in the window and answers
// NRI from it; once the label is read it builds its state, judges the kept
// containers, and a container refused in the window starts.
func TestConfigureFromProfileAfterAnUnreachableAPIServer(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	client := fake.NewClientset(labelledNode(map[string]string{driverconfig.ProfileLabel: "r7625"}))
	var reachable atomic.Bool
	var reads atomic.Int32
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		reads.Add(1)
		if !reachable.Load() {
			return true, nil, errors.New("dial tcp 10.96.0.1:443: connect: connection refused")
		}
		return false, nil, nil
	})
	held := cpuset.New(0, 2)
	driver := profiledDriver(t, client, map[types.UID]cpuset.CPUSet{"existing": held})

	done := make(chan error, 1)
	go func() { done <- driver.configureFromProfile(ctx, make(chan error)) }()

	hostPod := windowPod("host", true)
	claimPod := windowPod("claim", false)
	running := windowContainer("workload", claimPod, fmt.Sprintf("%s_existing=%s", cdiEnvVarPrefix, held))
	_, err := driver.Synchronize(ctx, []*api.PodSandbox{claimPod}, []*api.Container{running})
	require.NoError(t, err)
	_, _, err = driver.CreateContainer(ctx, hostPod, windowContainer("kube-proxy", hostPod))
	require.NoError(t, err)
	otherPod := windowPod("other", false)
	refused := windowContainer("app", otherPod)
	_, _, err = driver.CreateContainer(ctx, otherPod, refused)
	require.Error(t, err)

	require.Eventually(t, func() bool { return reads.Load() >= 3 }, 5*time.Second, time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("configureFromProfile returned %v before the label was read", err)
	default:
	}

	reachable.Store(true)
	require.NoError(t, <-done)

	owner, bound := driver.allocationState.Owner("existing")
	require.True(t, bound, "the kept container holding a claim is bound to it")
	require.Equal(t, types.UID(claimPod.GetUid()), owner.PodUID)
	require.Contains(t, driver.podState.GetContainersWithSharedCPUs(), types.UID("kube-proxy"),
		"the host-network container admitted in the window is known as one without claims")

	adjust, _, err := driver.CreateContainer(ctx, otherPod, refused)
	require.NoError(t, err, "the container refused in the window starts after the read")
	require.Equal(t, driver.allocationState.GetSharedCPUs().String(), adjust.GetLinux().GetResources().GetCpu().GetCpus())
}

// The label is read once NRI has synchronized, so the containers the runtime
// lists are kept before the driver can leave the window.
func TestConfigureFromProfileReadsAfterTheFirstSynchronize(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	client := fake.NewClientset(labelledNode(map[string]string{driverconfig.ProfileLabel: "r7625"}))
	var reads atomic.Int32
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		reads.Add(1)
		return false, nil, nil
	})
	driver := profiledDriver(t, client, nil)

	done := make(chan error, 1)
	go func() { done <- driver.configureFromProfile(ctx, make(chan error)) }()
	require.Never(t, func() bool { return reads.Load() > 0 }, 100*time.Millisecond, time.Millisecond,
		"the label was read before the first Synchronize")

	_, err := driver.Synchronize(ctx, nil, nil)
	require.NoError(t, err)
	require.NoError(t, <-done)
	require.Equal(t, int32(1), reads.Load())
}

// No implicit profile: an unlabelled node and a label naming a profile the
// configuration does not declare, default among them, stop the driver, which
// stays unconfigured and so never registers.
func TestConfigureFromProfileRefusesTheNode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		labels  map[string]string
		wantErr string
	}{{
		name:    "no label",
		wantErr: `node has no dra.cpu/profile label; label it with one of the declared profiles ["r7625"]`,
	}, {
		name:    "the label default, no profile of that name declared",
		labels:  map[string]string{driverconfig.ProfileLabel: "default"},
		wantErr: `node label dra.cpu/profile=default names no declared profile; declared: ["r7625"]`,
	}, {
		name:    "a label naming no declared profile",
		labels:  map[string]string{driverconfig.ProfileLabel: "x3d"},
		wantErr: `node label dra.cpu/profile=x3d names no declared profile; declared: ["r7625"]`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			driver := profiledDriver(t, fake.NewClientset(labelledNode(tc.labels)), nil)
			_, err := driver.Synchronize(ctx, nil, nil)
			require.NoError(t, err)

			require.EqualError(t, driver.configureFromProfile(ctx, make(chan error)), tc.wantErr)
			require.NotNil(t, driver.window, "the driver left the window")
			require.Nil(t, driver.allocationState, "the driver built its state")
		})
	}
}

// nodeGetFailures makes the first n reads of a node fail with err and lets the
// rest through to the tracker, and reports how many reads were attempted.
func nodeGetFailures(client *fake.Clientset, n int32, err error) *atomic.Int32 {
	var attempts atomic.Int32
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		if attempts.Add(1) <= n {
			return true, nil, err
		}
		return false, nil, nil
	})
	return &attempts
}

// The label read waits out every error: a node whose network is not up yet
// refuses the read for its first seconds (B96), and a 403 is an ordering race
// with the binding that grants the read. Only the driver's own shutdown, or an
// NRI plugin that failed for good, ends the wait.
func TestAwaitProfileLabel(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, testNodeName, errors.New("RBAC"))
	nriFailed := make(chan error, 1)
	nriFailed <- errors.New("connection to the runtime lost")
	for _, tc := range []struct {
		name      string
		failures  int32
		err       error
		nriErr    chan error
		timeout   time.Duration
		wantReads int32
		wantErr   string
	}{{
		name:      "a refused connection is retried",
		failures:  2,
		err:       errors.New("dial tcp 10.96.0.1:443: connect: connection refused"),
		wantReads: 3,
	}, {
		name:      "a forbidden reply is waited out",
		failures:  1,
		err:       forbidden,
		wantReads: 2,
	}, {
		name:     "a cancelled context ends the wait",
		failures: 1 << 30,
		err:      errors.New("connection refused"),
		timeout:  20 * time.Millisecond,
		wantErr:  context.DeadlineExceeded.Error(),
	}, {
		name:      "an NRI plugin that failed for good ends the wait",
		failures:  1 << 30,
		err:       errors.New("connection refused"),
		nriErr:    nriFailed,
		wantReads: 1,
		wantErr:   "NRI plugin failed before the node's dra.cpu/profile label was read: connection to the runtime lost",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			if tc.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tc.timeout)
				defer cancel()
			}
			nriErr := tc.nriErr
			if nriErr == nil {
				nriErr = make(chan error)
			}
			client := fake.NewClientset(labelledNode(map[string]string{driverconfig.ProfileLabel: "r7625"}))
			attempts := nodeGetFailures(client, tc.failures, tc.err)
			driver := profiledDriver(t, client, nil)

			name, err := driver.awaitProfileLabel(ctx, nriErr)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, "r7625", name)
			}
			if tc.wantReads > 0 {
				require.Equal(t, tc.wantReads, attempts.Load())
			}
		})
	}
}

// Each read carries its own deadline, so an API server that accepts the
// connection and never answers holds the read up for that long only.
func TestReadProfileLabelHasItsOwnDeadline(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"kind":"Node","apiVersion":"v1","metadata":{"name":%q,"labels":{%q:"r7625"}}}`, testNodeName, driverconfig.ProfileLabel)
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	driver := profiledDriver(t, client, nil)
	driver.profileReadTimeout = 50 * time.Millisecond

	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	name, err := driver.awaitProfileLabel(ctx, make(chan error))
	require.NoError(t, err)
	require.Equal(t, "r7625", name)
	require.Equal(t, int32(2), requests.Load(), "the hung read was abandoned at its deadline and read again")
}
