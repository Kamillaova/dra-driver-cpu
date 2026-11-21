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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/containerd/nri/pkg/net/multiplex"
	"github.com/containerd/nri/pkg/stub"
	"github.com/containerd/ttrpc"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

// reconnectingNRIRuntime speaks NRI's multiplexed ttrpc protocol and can drop
// the active connection without restarting the CPUDriver.
type reconnectingNRIRuntime struct {
	mu          sync.Mutex
	connection  net.Conn
	syncResults chan error
	// pods and containers are what each Synchronize lists, and syncReplies,
	// where set, receives each Synchronize's reply.
	pods        []*api.PodSandbox
	containers  []*api.Container
	syncReplies chan *api.SynchronizeResponse
}

func (r *reconnectingNRIRuntime) dial(string) (net.Conn, error) {
	pluginConn, runtimeConn := net.Pipe()
	r.mu.Lock()
	r.connection = runtimeConn
	r.mu.Unlock()
	go r.serve(runtimeConn)
	return pluginConn, nil
}

func (r *reconnectingNRIRuntime) disconnect() {
	r.mu.Lock()
	conn := r.connection
	r.connection = nil
	r.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (r *reconnectingNRIRuntime) serve(conn net.Conn) {
	mux := multiplex.Multiplex(conn, multiplex.WithBlockedRead())
	defer mux.Close()

	pluginConn, err := mux.Open(multiplex.PluginServiceConn)
	if err != nil {
		return
	}
	plugin := api.NewPluginClient(ttrpc.NewClient(pluginConn))
	runtimeListener, err := mux.Listen(multiplex.RuntimeServiceConn)
	if err != nil {
		return
	}

	server, err := ttrpc.NewServer()
	if err != nil {
		return
	}
	api.RegisterRuntimeService(server, &reconnectingNRIRuntimeService{plugin: plugin, syncResults: r.syncResults, pods: r.pods, containers: r.containers, syncReplies: r.syncReplies})
	// Register both logical connections before accepting incoming frames.
	mux.Unblock()
	_ = server.Serve(context.Background(), runtimeListener)
}

type reconnectingNRIRuntimeService struct {
	plugin      api.PluginService
	syncResults chan error
	pods        []*api.PodSandbox
	containers  []*api.Container
	syncReplies chan *api.SynchronizeResponse
}

func (s *reconnectingNRIRuntimeService) RegisterPlugin(ctx context.Context, _ *api.RegisterPluginRequest) (*api.Empty, error) {
	go func() {
		requestContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if _, err := s.plugin.Configure(requestContext, &api.ConfigureRequest{
			RuntimeName:         "test-runtime",
			RuntimeVersion:      "1.0",
			RegistrationTimeout: 1000,
			RequestTimeout:      1000,
		}); err != nil {
			s.syncResults <- err
			return
		}
		reply, err := s.plugin.Synchronize(requestContext, &api.SynchronizeRequest{Pods: s.pods, Containers: s.containers})
		if err == nil && s.syncReplies != nil {
			s.syncReplies <- reply
		}
		s.syncResults <- err
	}()
	return &api.Empty{}, nil
}

func (s *reconnectingNRIRuntimeService) UpdateContainers(context.Context, *api.UpdateContainersRequest) (*api.UpdateContainersResponse, error) {
	return &api.UpdateContainersResponse{}, nil
}

func TestNRIRuntimeReconnectRetainsPreparedClaim(t *testing.T) {
	d, _ := newMetricsTestDriver(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &reconnectingNRIRuntime{syncResults: make(chan error, 4)}
	plugin, err := stub.New(d,
		stub.WithPluginName(d.driverName),
		stub.WithPluginIdx("00"),
		stub.WithDialer(runtime.dial),
		// An OnClose callback prevents the stub from exiting the test process.
		stub.WithOnClose(func() {}),
	)
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() {
		runDone <- runNRIPluginWithRetry(ctx, plugin, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	}()
	t.Cleanup(func() {
		cancel()
		plugin.Stop()
		runtime.disconnect()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Error("NRI plugin runner did not stop")
		}
	})
	requireNoNRIError(t, runtime.syncResults)
	require.True(t, d.hasSynchronized, "initial NRI synchronization did not complete")

	claim := individualMetricsClaim("pending", "cpudev0")
	prepared, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
	require.NoError(t, err)
	require.NoError(t, prepared[claim.UID].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
	container := &api.Container{
		Id: "old-container", Name: "app", PodSandboxId: pod.Id,
		Env: []string{"DRA_CPUSET_pending=0"},
	}
	_, _, err = d.CreateContainer(ctx, pod, container)
	require.NoError(t, err)
	_, err = d.StopContainer(ctx, pod, container)
	require.NoError(t, err)

	// Drop only the NRI connection; the CPUDriver and its stores remain alive.
	runtime.disconnect()
	requireNoNRIError(t, runtime.syncResults)

	gotCPUs, found := d.allocationState.GetResourceClaimAllocation(claim.UID)
	require.True(t, found, "reconnect discarded the prepared allocation")
	require.True(t, cpuset.New(0).Equals(gotCPUs))
	require.True(t, d.allocationState.IsOwner(claim.UID, types.UID(pod.Uid), container.Name), "reconnect discarded claim ownership")

	otherPod := &api.PodSandbox{Id: "other-sandbox", Uid: "other-pod"}
	_, _, err = d.CreateContainer(ctx, otherPod, &api.Container{
		Id: "other-container", Name: container.Name, Env: []string{"DRA_CPUSET_pending=0"},
	})
	require.Error(t, err, "another Pod must not take the retained claim")
	container.Id = "replacement-container"
	adjustment, _, err := d.CreateContainer(ctx, pod, container)
	require.NoError(t, err)
	require.Equal(t, "0", adjustment.Linux.Resources.Cpu.Cpus)
}

func TestPrepareRefusedWhileNRIReconnects(t *testing.T) {
	d, _ := newMetricsTestDriver(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &reconnectingNRIRuntime{syncResults: make(chan error, 4)}
	redialing := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	dials := 0
	plugin, err := stub.New(d,
		stub.WithPluginName(d.driverName),
		stub.WithPluginIdx("00"),
		stub.WithDialer(func(path string) (net.Conn, error) {
			dials++
			if dials == 2 {
				close(redialing)
				<-release
			}
			return runtime.dial(path)
		}),
		stub.WithOnClose(func() {}),
	)
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() {
		runDone <- runNRIPluginWithRetry(ctx, nriConnection{driver: d, plugin: plugin}, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	}()
	t.Cleanup(func() {
		cancel()
		releaseOnce()
		plugin.Stop()
		runtime.disconnect()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Error("NRI plugin runner did not stop")
		}
	})
	requireNoNRIError(t, runtime.syncResults)

	held := individualMetricsClaim("held", "cpudev0")
	prepared, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{held})
	require.NoError(t, err)
	require.NoError(t, prepared[held.UID].Err)

	runtime.disconnect()
	<-redialing

	pending := individualMetricsClaim("pending", "cpudev1")
	prepared, err = d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{pending})
	require.NoError(t, err)
	require.ErrorIs(t, prepared[pending.UID].Err, errNRIDisconnected)
	_, found := d.allocationState.GetResourceClaimAllocation(pending.UID)
	require.False(t, found, "a refused claim must not hold CPUs")
	require.NotContains(t, d.cdiMgr.(*mockCdiMgr).devices, getCDIDeviceName(pending.UID))

	unprepared, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: held.UID}})
	require.NoError(t, err)
	require.NoError(t, unprepared[held.UID], "Unprepare is never refused")

	releaseOnce()
	requireNoNRIError(t, runtime.syncResults)
	prepared, err = d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{pending})
	require.NoError(t, err)
	require.NoError(t, prepared[pending.UID].Err)
}

func TestReadyNeedsRegistrationAndNRI(t *testing.T) {
	infos := mockCPUInfos_SingleSocket_4CPUS_HT
	d, err := New(testr.New(t), Providers{K8SClient: statusClient(), CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: infos}, SysFS: testSysFS(infos)}, &Config{
		DriverName: testDriverName, NodeName: testNodeName,
		CPUDeviceMode: device.CPU_DEVICE_MODE_GROUPED, CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
	})
	require.NoError(t, err)
	d.cdiMgr = newMockCdiMgr()
	var registered atomic.Bool
	d.draPlugin = &mockKubeletPlugin{statusFunc: func(int32) *registerapi.RegistrationStatus {
		return &registerapi.RegistrationStatus{PluginRegistered: registered.Load()}
	}}
	require.False(t, d.Ready(), "neither registered nor connected to NRI")
	registered.Store(true)
	require.False(t, d.Ready(), "registered, NRI not connected yet")

	ctx, cancel := context.WithCancel(t.Context())
	runtime := &reconnectingNRIRuntime{syncResults: make(chan error, 4)}
	redialing := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	dials := 0
	plugin, err := stub.New(d,
		stub.WithPluginName(d.driverName),
		stub.WithPluginIdx("00"),
		stub.WithDialer(func(path string) (net.Conn, error) {
			dials++
			if dials == 2 {
				close(redialing)
				<-release
			}
			return runtime.dial(path)
		}),
		stub.WithOnClose(func() {}),
	)
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() {
		runDone <- runNRIPluginWithRetry(ctx, nriConnection{driver: d, plugin: plugin}, maxAttempts, testRetryInitialBackoff, testRetryMaxBackoff, testRetryHealthyRun)
	}()
	t.Cleanup(func() {
		cancel()
		releaseOnce()
		plugin.Stop()
		runtime.disconnect()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Error("NRI plugin runner did not stop")
		}
	})
	requireNoNRIError(t, runtime.syncResults)
	require.True(t, d.Ready(), "registered and synchronized with NRI")

	registered.Store(false)
	require.False(t, d.Ready(), "the kubelet refused the plugin")
	registered.Store(true)

	runtime.disconnect()
	<-redialing
	require.False(t, d.Ready(), "NRI is reconnecting")

	releaseOnce()
	requireNoNRIError(t, runtime.syncResults)
	require.True(t, d.Ready(), "NRI synchronized again")
}

func requireNoNRIError(t *testing.T, results <-chan error) {
	t.Helper()
	select {
	case err := <-results:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for NRI synchronization")
	}
}
