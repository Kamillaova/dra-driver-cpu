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
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	registerapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	"k8s.io/utils/cpuset"
)

// watchdogTestDriver returns a driver the kubelet has registered, not yet
// synchronized with NRI, and the run context its watchdog cancels.
func watchdogTestDriver(t *testing.T) (*CPUDriver, *prometheus.Registry, context.Context) {
	t.Helper()
	d, reg := newMetricsTestDriver(t)
	d.initialSync = make(chan struct{})
	d.draPlugin = &mockKubeletPlugin{statusFunc: func(int32) *registerapi.RegistrationStatus {
		return &registerapi.RegistrationStatus{PluginRegistered: true}
	}}
	ctx, cancel := context.WithCancelCause(t.Context())
	t.Cleanup(func() { cancel(nil) })
	d.cancelRun = cancel
	return d, reg, ctx
}

type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) logger() logr.Logger {
	return funcr.New(func(_, args string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.lines = append(l.lines, args)
	}, funcr.Options{})
}

// matching returns the lines carrying msg.
func (l *logLines) matching(msg string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []string
	for _, line := range l.lines {
		if strings.Contains(line, fmt.Sprintf("%q=%q", "msg", msg)) {
			found = append(found, line)
		}
	}
	return found
}

func stuckSeconds(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	return metricValue(t, reg, "dra_cpu_watchdog_stuck_seconds", nil)
}

var watchdogStalls = []struct {
	name  string
	stall func(d *CPUDriver) (end func())
	stuck string
}{
	{name: "stateMu held", stall: func(d *CPUDriver) func() { d.stateMu.Lock(); return d.stateMu.Unlock }, stuck: "stateMu"},
	{name: "a handler running", stall: func(d *CPUDriver) func() { return d.watchdog.track("PrepareResourceClaims") }, stuck: "PrepareResourceClaims"},
}

func TestWatchdogReportsAStallUntilItClears(t *testing.T) {
	for _, tc := range watchdogStalls {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, reg, ctx := watchdogTestDriver(t)
				_, err := d.Synchronize(t.Context(), nil, nil)
				require.NoError(t, err)
				var logs logLines
				go d.runWatchdog(ctxlog.NewContext(ctx, logs.logger()))

				end := tc.stall(d)
				time.Sleep(60 * time.Second)
				synctest.Wait()
				require.True(t, d.Ready(), "a stall of 60s is not past the threshold")
				require.Zero(t, stuckSeconds(t, reg))

				time.Sleep(time.Second)
				synctest.Wait()
				require.False(t, d.Ready(), "past 60s the driver reports not ready")
				require.Equal(t, 61.0, stuckSeconds(t, reg))
				stuck := logs.matching("driver stuck, reporting not ready")
				require.Len(t, stuck, 1)
				require.Contains(t, stuck[0], tc.stuck)
				require.Contains(t, stuck[0], "goroutine ")

				time.Sleep(118 * time.Second)
				synctest.Wait()
				require.False(t, d.Ready())
				require.Equal(t, 179.0, stuckSeconds(t, reg))
				require.Len(t, logs.matching("driver stuck, reporting not ready"), 1, "one goroutine dump per stall")

				end()
				time.Sleep(time.Second)
				synctest.Wait()
				require.True(t, d.Ready(), "ready again once the stall clears")
				require.Zero(t, stuckSeconds(t, reg))
				require.Len(t, logs.matching("driver no longer stuck"), 1)
				require.NoError(t, ctx.Err(), "a stall that clears before 180s stops nothing")
			})
		})
	}
}

func TestWatchdogStopsTheDriverPastTheLimit(t *testing.T) {
	for _, tc := range watchdogStalls {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, _, ctx := watchdogTestDriver(t)
				var logs logLines
				returned := make(chan struct{})
				go func() {
					defer close(returned)
					d.runWatchdog(ctxlog.NewContext(ctx, logs.logger()))
				}()

				end := tc.stall(d)
				defer end()
				time.Sleep(180 * time.Second)
				synctest.Wait()
				require.NoError(t, ctx.Err(), "a stall of 180s is not past the limit")

				time.Sleep(time.Second)
				synctest.Wait()
				require.ErrorIs(t, context.Cause(ctx), ErrFatal, "past 180s the run context is cancelled with a fatal cause")
				require.ErrorContains(t, context.Cause(ctx), tc.stuck)
				require.True(t, isClosed(returned), "the watchdog returns once it has cancelled the run")
			})
		})
	}
}

// A hook that stalls while it holds stateMu is timed twice, as a handler and
// as the hold.
func TestWatchdogTimesAStalledHook(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, reg, ctx := watchdogTestDriver(t)
		var logs logLines
		go d.runWatchdog(ctxlog.NewContext(ctx, logs.logger()))
		cdi := &pausedRefreshCDI{
			mockCdiMgr: newMockCdiMgrWithAllocations(map[types.UID]cpuset.CPUSet{"running": cpuset.New(1)}),
			entered:    make(chan struct{}),
			release:    make(chan struct{}),
		}
		d.cdiMgr = cdi
		pod := &api.PodSandbox{Id: "sandbox", Uid: "pod"}
		container := &api.Container{Id: "ctr", Name: "app", PodSandboxId: pod.Id, Env: []string{cdiEnvVarPrefix + "_running=1"}}
		release := sync.OnceFunc(func() { close(cdi.release) })
		defer release()
		synced := make(chan error, 1)
		go func() {
			_, err := d.Synchronize(t.Context(), []*api.PodSandbox{pod}, []*api.Container{container})
			synced <- err
		}()
		<-cdi.entered

		time.Sleep(61 * time.Second)
		synctest.Wait()
		require.Equal(t, 61.0, stuckSeconds(t, reg))
		stuck := logs.matching("driver stuck, reporting not ready")
		require.Len(t, stuck, 1)
		require.Contains(t, stuck[0], `"stuck"=["Synchronize" "stateMu"]`)

		release()
		require.NoError(t, <-synced)
		time.Sleep(time.Second)
		synctest.Wait()
		require.Zero(t, stuckSeconds(t, reg))
		require.True(t, d.Ready(), "ready once the hook returned")
	})
}
