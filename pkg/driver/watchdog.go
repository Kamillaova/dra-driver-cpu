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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
)

const (
	// watchdogStuckAfter is how long stateMu may stay held, or a handler run,
	// before the driver reports itself stuck.
	watchdogStuckAfter = 60 * time.Second
	// watchdogExitAfter is how long before the driver stops itself. It lies far
	// above the longest hold Prepare's own work needs.
	watchdogExitAfter = 180 * time.Second
	watchdogInterval  = time.Second
)

// timedMutex is a sync.Mutex that records since when it is held, so the
// watchdog can time a hold without taking the lock.
type timedMutex struct {
	mu       sync.Mutex
	heldFrom atomic.Pointer[time.Time]
}

func (m *timedMutex) Lock() {
	m.mu.Lock()
	now := time.Now()
	m.heldFrom.Store(&now)
}

func (m *timedMutex) Unlock() {
	m.heldFrom.Store(nil)
	m.mu.Unlock()
}

// watchdog records when each NRI and DRA handler in progress started.
type watchdog struct {
	mu       sync.Mutex
	nextID   uint64
	handlers map[uint64]handlerRun
	// stuck is set while a hold or a handler is past watchdogStuckAfter.
	stuck atomic.Bool
}

type handlerRun struct {
	name  string
	start time.Time
}

// track records that the handler name has started; the returned function
// records that it has returned.
func (w *watchdog) track(name string) (returned func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.handlers == nil {
		w.handlers = make(map[uint64]handlerRun)
	}
	w.nextID++
	id := w.nextID
	w.handlers[id] = handlerRun{name: name, start: time.Now()}
	return func() {
		w.mu.Lock()
		delete(w.handlers, id)
		w.mu.Unlock()
	}
}

// longestStall returns how long the longest hold of lock or run of a handler
// has lasted, and the sorted names of those past watchdogStuckAfter.
func (w *watchdog) longestStall(lock *timedMutex, now time.Time) (time.Duration, []string) {
	var longest time.Duration
	var stuck []string
	note := func(name string, since time.Time) {
		stalled := now.Sub(since)
		longest = max(longest, stalled)
		if stalled > watchdogStuckAfter {
			stuck = append(stuck, name)
		}
	}
	if held := lock.heldFrom.Load(); held != nil {
		note("stateMu", *held)
	}
	w.mu.Lock()
	for _, run := range w.handlers {
		note(run.name, run.start)
	}
	w.mu.Unlock()
	slices.Sort(stuck)
	return longest, slices.Compact(stuck)
}

// runWatchdog checks every watchdogInterval how long stateMu has been held
// and each handler has been running. Past watchdogStuckAfter the driver
// reports itself stuck, through the gauge, one goroutine dump and Ready,
// until the stall clears; past watchdogExitAfter it cancels the run context
// so that the driver stops and the process exits non-zero. It makes no API
// call, so an unreachable API server cannot set it off.
func (cp *CPUDriver) runWatchdog(ctx context.Context) {
	logger := ctxlog.FromContext(ctx)
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		longest, stuck := cp.watchdog.longestStall(&cp.stateMu, time.Now())
		if len(stuck) == 0 {
			if cp.watchdog.stuck.Swap(false) {
				cp.metrics.SetWatchdogStuck(0)
				logger.Info("driver no longer stuck")
			}
			continue
		}
		cp.metrics.SetWatchdogStuck(longest)
		if !cp.watchdog.stuck.Swap(true) {
			logger.Info("driver stuck, reporting not ready", "for", longest, "stuck", stuck, "goroutines", goroutineDump())
		}
		if longest > watchdogExitAfter {
			err := fmt.Errorf("%w: stuck for %s in %s", ErrFatal, longest, strings.Join(stuck, ", "))
			logger.Error(err, "watchdog stopping the driver")
			cp.cancelRun(err)
			return
		}
	}
}

// goroutineDump returns the stacks of every goroutine.
func goroutineDump() string {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}
