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

package probes

import (
	"net/http"
	"sync/atomic"
)

// Probes serves the kubelet's liveness probe on /livez and its readiness
// probe on /healthz.
type Probes struct {
	ready atomic.Pointer[func() bool]
}

// Register adds /livez, which answers 200 as long as the server serves, and
// /healthz, which answers 503 until SetReady is called and then 200 only
// while its check passes.
func (p *Probes) Register(mux *http.ServeMux) {
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if ready := p.ready.Load(); ready == nil || !(*ready)() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// SetReady sets the check /healthz answers with.
func (p *Probes) SetReady(ready func() bool) {
	p.ready.Store(&ready)
}
