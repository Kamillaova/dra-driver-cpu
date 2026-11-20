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

package allocation

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/cpuset"
)

func TestParseOpaqueConfig(t *testing.T) {
	testCases := []struct {
		name          string
		rawJSON       string
		expected      Config
		expectedError string
	}{
		{
			name:     "valid config",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5"}}`,
			expected: Config{CPUs: cpuset.New(2, 3, 4, 5), HasCPUs: true},
		},
		{
			name:     "an empty cpuset names no CPUs",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": ""}}`,
			expected: Config{},
		},
		{
			name:     "no cpuConfig",
			rawJSON:  `{"apiVersion": "v1alpha1"}`,
			expected: Config{},
		},
		{
			name:     "a limit written as a string",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": "500m"}}`,
			expected: Config{Limit: new(v1alpha1.Limit("500m"))},
		},
		{
			name:     "a limit written as a number keeps its text",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": 0.50}}`,
			expected: Config{Limit: new(v1alpha1.Limit("0.50"))},
		},
		{
			name:     "no limit",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": "max", "cpuset": "2"}}`,
			expected: Config{CPUs: cpuset.New(2), HasCPUs: true, Limit: new(v1alpha1.LimitMax)},
		},
		{
			name:     "a value that is no quantity is left to the caller",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": ""}}`,
			expected: Config{Limit: new(v1alpha1.Limit(""))},
		},
		{
			name:     "a null limit is no limit",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": null}}`,
			expected: Config{},
		},
		{
			name:          "invalid - a limit of another type",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"limit": true}}`,
			expectedError: `failed to unmarshal opaque config: limit true is neither a quantity nor "max"`,
		},
		{
			name:          "invalid - invalid cpuset format",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "abc"}}`,
			expectedError: "opaque config: failed to parse cpuset \"abc\"",
		},
		{
			name:          "invalid - unsupported version",
			rawJSON:       `{"apiVersion": "v1beta1", "cpuConfig": {"cpuset": "0-3"}}`,
			expectedError: "unsupported opaque config apiVersion: \"v1beta1\"",
		},
		{
			name:          "invalid - missing version",
			rawJSON:       `{"cpuConfig": {"cpuset": "0-3"}}`,
			expectedError: "unsupported opaque config apiVersion: \"\"",
		},
		{
			name:          "invalid - malformed json",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig":`,
			expectedError: "failed to unmarshal opaque config",
		},
		{
			name:          "invalid - unknown key in cpuConfig",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5", "limt": "500m"}}`,
			expectedError: `opaque config: strict decoding error: unknown field "cpuConfig.limt"`,
		},
		{
			name:          "invalid - unknown top-level key",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5"}, "memConfig": {}}`,
			expectedError: `opaque config: strict decoding error: unknown field "memConfig"`,
		},
		{
			name:          "invalid - cpuset in another casing",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"CPUSet": "2-5"}}`,
			expectedError: `opaque config: strict decoding error: unknown field "cpuConfig.CPUSet"`,
		},
		{
			name:          "invalid - apiVersion in another casing",
			rawJSON:       `{"APIVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5"}}`,
			expectedError: `opaque config: strict decoding error: unknown field "APIVersion"`,
		},
		{
			name:          "invalid - cpuset written twice",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "abc", "cpuset": "2-5"}}`,
			expectedError: `opaque config: strict decoding error: duplicate field "cpuConfig.cpuset"`,
		},
		{
			name:          "invalid - every problem named",
			rawJSON:       `{"apiVersion": "v1alpha1", "apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5", "Cpuset": "2-5"}}`,
			expectedError: `opaque config: strict decoding error: duplicate field "apiVersion", unknown field "cpuConfig.Cpuset"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseOpaqueConfig([]byte(tc.rawJSON))
			if tc.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				assert.Equal(t, Config{}, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
