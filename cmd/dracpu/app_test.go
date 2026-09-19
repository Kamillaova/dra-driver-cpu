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

package main

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestRunUsesConfigSysFSOverlay: run() must build the sysfs overlay from cfg, not the pre-merge driverFlags.
func TestRunUsesConfigSysFSOverlay(t *testing.T) {
	cfg := driverconfig.Default()
	cfg.SysFSOverlay = filepath.Join(t.TempDir(), "does-not-exist.yaml")

	err := run(testr.New(t), cfg)
	if err == nil {
		t.Fatal("run() succeeded, want error reading missing sysfs overlay")
	}
	if !strings.Contains(err.Error(), "read sysfs overlay") {
		t.Fatalf("run() error = %v, want sysfs overlay read error (cfg.SysFSOverlay was ignored)", err)
	}
}

// TestWaitForShutdownHTTPErrorIsFatal: a bind or serve failure on httpErr
// must be returned, not swallowed. Left unreported, the process keeps
// running with nothing listening on the port, and the only way to notice is
// the liveness probe's own failure threshold.
func TestWaitForShutdownHTTPErrorIsFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: 5 * time.Second}

	asyncErr := make(chan error)
	httpErr := make(chan error, 1)
	httpErr <- errors.New("listen tcp: address already in use")

	err := waitForShutdown(ctx, cancel, testr.New(t), server, asyncErr, httpErr)
	if err == nil {
		t.Fatal("waitForShutdown() succeeded, want the HTTP bind error")
	}
	if !strings.Contains(err.Error(), "HTTP server error") || !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("waitForShutdown() error = %v, want it to wrap the HTTP bind error", err)
	}
}

// TestWaitForShutdownNRIErrorIsFatal pins the pre-existing asyncErr path
// alongside the new httpErr one, so a future change cannot silently prefer
// one channel over the other.
func TestWaitForShutdownNRIErrorIsFatal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: 5 * time.Second}

	asyncErr := make(chan error, 1)
	httpErr := make(chan error)
	asyncErr <- errors.New("NRI plugin failed for 5 times to be restarted")

	err := waitForShutdown(ctx, cancel, testr.New(t), server, asyncErr, httpErr)
	if err == nil {
		t.Fatal("waitForShutdown() succeeded, want the NRI driver error")
	}
	if !strings.Contains(err.Error(), "NRI driver error") {
		t.Fatalf("waitForShutdown() error = %v, want it to wrap the NRI driver error", err)
	}
}

// TestWaitForShutdownContextDoneIsClean: a normal signal-driven shutdown with
// neither channel firing must return no error.
func TestWaitForShutdownContextDoneIsClean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server := &http.Server{Addr: "127.0.0.1:0", ReadHeaderTimeout: 5 * time.Second}

	asyncErr := make(chan error)
	httpErr := make(chan error)

	err := waitForShutdown(ctx, cancel, testr.New(t), server, asyncErr, httpErr)
	if err != nil {
		t.Fatalf("waitForShutdown() error = %v, want nil on a clean signal-driven shutdown", err)
	}
}

// nodeGetFailures makes the first n reads of a node fail with err and lets the
// rest through to the tracker, and reports how many reads were attempted.
func nodeGetFailures(client *fake.Clientset, n int, err error) *int {
	attempts := 0
	client.PrependReactor("get", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		attempts++
		if attempts <= n {
			return true, nil, err
		}

		return false, nil, nil
	})

	return &attempts
}

// TestAwaitNodeWaitsForTheApiserver: a node read that is refused because the
// node's network is not up yet must be retried, not turned into an exit. This is
// the whole of B96: the driver used to die here and every claim already
// allocated on the node stayed unprepared until it won the race.
func TestAwaitNodeWaitsForTheApiserver(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "c1-4"}}
	client := fake.NewSimpleClientset(node)
	attempts := nodeGetFailures(client, 2, errors.New("dial tcp 10.96.0.1:443: connect: connection refused"))

	got, err := awaitNode(context.Background(), testr.New(t), client, "c1-4", time.Millisecond)
	if err != nil {
		t.Fatalf("awaitNode() error = %v, want it to wait for the apiserver", err)
	}
	if got.Name != "c1-4" {
		t.Fatalf("awaitNode() node = %q, want c1-4", got.Name)
	}
	if *attempts != 3 {
		t.Fatalf("awaitNode() made %d reads, want 3", *attempts)
	}
}

// TestAwaitNodeWaitsOutAForbiddenReply: a 403 is an ordering race between this
// driver and the binding that grants it the read, both of which arrive in the
// same manifest bundle. An earlier draft treated it as a verdict and returned;
// that is the crashloop B96 describes, reached by a different route.
func TestAwaitNodeWaitsOutAForbiddenReply(t *testing.T) {
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "c1-4"}}
	client := fake.NewSimpleClientset(node)
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "c1-4", errors.New("RBAC"))
	attempts := nodeGetFailures(client, 1, forbidden)

	if _, err := awaitNode(context.Background(), testr.New(t), client, "c1-4", time.Millisecond); err != nil {
		t.Fatalf("awaitNode() error = %v, want a forbidden reply to be waited out", err)
	}
	if *attempts != 2 {
		t.Fatalf("awaitNode() made %d reads, want 2", *attempts)
	}
}

// TestAwaitNodeStopsWhenTheContextIsCancelled: waiting forever is bounded by the
// process's own shutdown and nothing else, so a terminating driver must not hang.
func TestAwaitNodeStopsWhenTheContextIsCancelled(t *testing.T) {
	client := fake.NewSimpleClientset()
	nodeGetFailures(client, 1<<30, errors.New("connection refused"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := awaitNode(ctx, testr.New(t), client, "c1-4", time.Millisecond); err == nil {
		t.Fatal("awaitNode() returned no error, want the cancelled context reported")
	}
}
