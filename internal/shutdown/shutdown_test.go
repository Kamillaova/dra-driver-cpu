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

package shutdown

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBound(t *testing.T) {
	fatal := errors.New("DRA gRPC server failed")
	for _, tc := range []struct {
		name string
		// cancel cancels the run context the way a signal or HandleError does:
		// the first through its parent, the second through its cause.
		cancel func(signal context.CancelFunc, fail context.CancelCauseFunc)
		// stopTakes is how long the shutdown takes; zero holds it for good.
		stopTakes time.Duration
		wantExit  bool
	}{
		{
			name:     "a signal, the shutdown held",
			cancel:   func(signal context.CancelFunc, _ context.CancelCauseFunc) { signal() },
			wantExit: true,
		},
		{
			name:     "a fatal error, the shutdown held",
			cancel:   func(_ context.CancelFunc, fail context.CancelCauseFunc) { fail(fatal) },
			wantExit: true,
		},
		{
			name:      "a signal, the shutdown finishing in time",
			cancel:    func(signal context.CancelFunc, _ context.CancelCauseFunc) { signal() },
			stopTakes: Timeout - time.Second,
		},
		{
			name:      "a fatal error, the shutdown finishing in time",
			cancel:    func(_ context.CancelFunc, fail context.CancelCauseFunc) { fail(fatal) },
			stopTakes: Timeout - time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				signalCtx, signal := context.WithCancel(t.Context())
				defer signal()
				ctx, fail := context.WithCancelCause(signalCtx)
				defer fail(nil)
				exits := make(chan time.Time, 1)
				finished := Bound(ctx, func() { exits <- time.Now() })
				defer finished()

				// The timer starts at the cancel, not at Bound.
				time.Sleep(time.Hour)
				tc.cancel(signal, fail)
				cancelled := time.Now()
				if tc.stopTakes > 0 {
					time.Sleep(tc.stopTakes)
					finished()
				}
				time.Sleep(time.Hour)

				select {
				case exited := <-exits:
					require.True(t, tc.wantExit, "exited although the shutdown finished in time")
					require.Equal(t, Timeout, exited.Sub(cancelled))
				default:
					require.False(t, tc.wantExit, "no exit although the shutdown was held")
				}
			})
		})
	}
}
