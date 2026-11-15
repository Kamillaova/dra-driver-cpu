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
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/stretchr/testify/require"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/utils/cpuset"
)

// fakeRuntime records the CPUs each unsolicited update gives each container,
// and runs during the first update whatever the test has the runtime do then.
type fakeRuntime struct {
	sent   []map[string]string
	during func()
}

func (f *fakeRuntime) UpdateContainers(updates []*api.ContainerUpdate) ([]*api.ContainerUpdate, error) {
	cpus := map[string]string{}
	for _, update := range updates {
		cpus[update.GetContainerId()] = update.GetLinux().GetResources().GetCpu().GetCpus()
	}
	f.sent = append(f.sent, cpus)
	if during := f.during; during != nil {
		f.during = nil
		during()
	}
	return nil, nil
}

func TestReconcileSharedContainersAfterUnprepare(t *testing.T) {
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "pod", Namespace: "ns"}
	tests := []struct {
		name         string
		safe         bool
		disconnected bool
		// during runs on the runtime while the first update is on its way.
		during   func(t *testing.T, d *CPUDriver)
		wantSent []map[string]string
		// wantSync is the shared container's CPUs in the reconnect's Synchronize reply.
		wantSync string
	}{
		{
			name:     "assumeUnsolicitedUpdatesSafe on",
			safe:     true,
			wantSent: []map[string]string{{"shared": "0-3"}},
		},
		{
			name: "assumeUnsolicitedUpdatesSafe off",
			safe: false,
		},
		{
			name:         "NRI disconnected: the reconnect's Synchronize delivers it",
			safe:         true,
			disconnected: true,
			wantSync:     "0-3",
		},
		{
			name: "a claim's first container narrows while the update is on its way",
			safe: true,
			during: func(t *testing.T, d *CPUDriver) {
				require.NoError(t, d.allocationState.Reserve(testr.New(t), "new", oneRequest(cpuset.New(3)), false))
				ctr := &api.Container{Id: "new-ctr", PodSandboxId: pod.Id, Name: "app", Env: []string{fmt.Sprintf("%s_new_cpus=%s", cdiEnvVarPrefix, cdiEnvDynamicValue)}}
				_, updates, err := d.CreateContainer(context.Background(), pod, ctr)
				require.NoError(t, err)
				require.Equal(t, "0-2", updates[0].GetLinux().GetResources().GetCpu().GetCpus(), "the container's reply narrows the shared container")
			},
			wantSent: []map[string]string{{"shared": "0-3"}, {"shared": "0-2"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				logger := testr.New(t)
				var infos []cpuinfo.CPUInfo
				for cpuID := range 4 {
					infos = append(infos, cpuinfo.CPUInfo{CpuID: cpuID, CoreID: cpuID, SocketID: 0, NUMANodeID: 0})
				}
				topo, err := (&cpuinfo.MockCPUInfoProvider{CPUInfos: infos}).GetCPUTopology(logger)
				require.NoError(t, err)
				allocationState := store.NewAllocationState(topo, cpuset.New())
				require.NoError(t, allocationState.Reserve(logger, "old", oneRequest(cpuset.New(2, 3)), false))
				podState := store.NewPodState()
				podState.SetContainerState("shared-pod", store.NewContainerState("shared-ctr", "shared"))
				runtime := &fakeRuntime{}
				d := &CPUDriver{
					podState:        podState,
					allocationState: allocationState,
					metrics:         cpumetrics.Noop(),
					cdiMgr:          newMockCdiMgr(),
					hasSynchronized: true,
				}
				d.nriDisconnected.Store(tc.disconnected)
				if tc.safe {
					d.reconcileTrigger = make(chan struct{}, 1)
					d.containerUpdater = runtime
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					go d.runReconcileWorker(ctx)
				}
				if tc.during != nil {
					runtime.during = func() { tc.during(t, d) }
				}

				results, err := d.UnprepareResourceClaims(t.Context(), []kubeletplugin.NamespacedObject{{UID: "old"}})
				require.NoError(t, err)
				require.NoError(t, results["old"])
				synctest.Wait()

				require.Equal(t, tc.wantSent, runtime.sent)
				if tc.wantSync == "" {
					return
				}
				sandbox := &api.PodSandbox{Id: "shared-sandbox", Uid: "shared-pod", Name: "shared-pod", Namespace: "ns"}
				updates, err := d.Synchronize(t.Context(), []*api.PodSandbox{sandbox}, []*api.Container{{Id: "shared", PodSandboxId: sandbox.Id, Name: "shared-ctr"}})
				require.NoError(t, err)
				require.Len(t, updates, 1)
				require.Equal(t, tc.wantSync, updates[0].GetLinux().GetResources().GetCpu().GetCpus())
				require.Empty(t, runtime.sent, "nothing is sent while NRI is disconnected")
			})
		})
	}
}

func TestRequestReconcileCoalesces(t *testing.T) {
	d := &CPUDriver{reconcileTrigger: make(chan struct{}, 1)}
	d.requestReconcile()
	d.requestReconcile()
	require.Len(t, d.reconcileTrigger, 1)
	(&CPUDriver{}).requestReconcile()
}
