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
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProbes(t *testing.T) {
	var p Probes
	mux := http.NewServeMux()
	p.Register(mux)
	get := func(path string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	require.Equal(t, http.StatusOK, get("/livez"), "liveness answers before the driver has started")
	require.Equal(t, http.StatusServiceUnavailable, get("/healthz"), "readiness waits for the driver")

	var ready atomic.Bool
	p.SetReady(ready.Load)
	require.Equal(t, http.StatusServiceUnavailable, get("/healthz"))
	ready.Store(true)
	require.Equal(t, http.StatusOK, get("/healthz"))
	ready.Store(false)
	require.Equal(t, http.StatusServiceUnavailable, get("/healthz"), "readiness follows the check after it first passed")
	require.Equal(t, http.StatusOK, get("/livez"), "liveness does not follow readiness")
}
