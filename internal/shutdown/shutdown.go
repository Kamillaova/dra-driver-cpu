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
	"sync"
	"time"
)

// Timeout bounds a graceful shutdown. It leaves room for the steps the
// shutdown waits for and stays below the 30s the kubelet waits before it
// removes the ResourceSlices of a driver that unregistered.
const Timeout = 10 * time.Second

// Bound calls exit if the returned function has not been called within
// Timeout of ctx being cancelled, whatever cancelled it.
func Bound(ctx context.Context, exit func()) (finished func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(Timeout)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			exit()
		}
	}()
	return sync.OnceFunc(func() { close(done) })
}
