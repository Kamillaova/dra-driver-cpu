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

	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
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
	server := &http.Server{Addr: "127.0.0.1:0"}

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
	server := &http.Server{Addr: "127.0.0.1:0"}

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
	server := &http.Server{Addr: "127.0.0.1:0"}

	asyncErr := make(chan error)
	httpErr := make(chan error)

	err := waitForShutdown(ctx, cancel, testr.New(t), server, asyncErr, httpErr)
	if err != nil {
		t.Fatalf("waitForShutdown() error = %v, want nil on a clean signal-driven shutdown", err)
	}
}
