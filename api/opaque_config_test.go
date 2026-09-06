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

package api

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/cpuset"
)

// bestEffort is the placement a configuration that says nothing about it gets.
var bestEffort = ClaimPlacement{Alignment: v1alpha1.AlignmentBestEffort}

func TestParseOpaqueConfig(t *testing.T) {
	testCases := []struct {
		name          string
		rawJSON       string
		expected      ClaimConfig
		expectedError string
	}{
		{
			name:    "valid config",
			rawJSON: `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5"}}`,
			expected: ClaimConfig{
				ClaimPlacement: bestEffort,
				CPUs:           cpuset.New(2, 3, 4, 5),
				HasCPUs:        true,
			},
		},
		{
			name:     "no cpuset",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": ""}}`,
			expected: ClaimConfig{ClaimPlacement: bestEffort},
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
			name:     "relocatable defaults to false",
			rawJSON:  `{"apiVersion": "v1alpha1", "cpuConfig": {}}`,
			expected: ClaimConfig{ClaimPlacement: bestEffort},
		},
		{
			name:    "relocatable stated",
			rawJSON: `{"apiVersion": "v1alpha1", "cpuConfig": {"relocatable": true}}`,
			expected: ClaimConfig{ClaimPlacement: ClaimPlacement{
				Relocatable: true,
				Alignment:   v1alpha1.AlignmentBestEffort,
			}},
		},
		{
			name:    "alignment stated explicitly is distinguishable from the default",
			rawJSON: `{"apiVersion": "v1alpha1", "cpuConfig": {"alignment": "BestEffort"}}`,
			expected: ClaimConfig{ClaimPlacement: ClaimPlacement{
				Alignment:    v1alpha1.AlignmentBestEffort,
				AlignmentSet: true,
			}},
		},
		{
			name:    "repairable with relocatable",
			rawJSON: `{"apiVersion": "v1alpha1", "cpuConfig": {"alignment": "Repairable", "relocatable": true}}`,
			expected: ClaimConfig{ClaimPlacement: ClaimPlacement{
				Relocatable:  true,
				Alignment:    v1alpha1.AlignmentRepairable,
				AlignmentSet: true,
			}},
		},
		{
			// Repair moves the claim's own CPUs, so a claim that refuses moves
			// cannot ask for it.
			name:          "invalid - repairable without relocatable",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"alignment": "Repairable"}}`,
			expectedError: "cpuConfig.alignment \"Repairable\" requires cpuConfig.relocatable",
		},
		{
			name:          "invalid - unknown alignment",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"alignment": "Strict"}}`,
			expectedError: "unsupported cpuConfig.alignment \"Strict\"",
		},
		{
			// The named CPUs are what such a claim is for; permitting the driver
			// to leave them contradicts naming them.
			name:          "invalid - cpuset with relocatable",
			rawJSON:       `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5", "relocatable": true}}`,
			expectedError: "cpuConfig.cpuset \"2-5\" cannot be combined with cpuConfig.relocatable",
		},
		{
			// Accepted here: whether the claim's requests leave the allocator a
			// choice of shape is not visible to this package.
			name:    "cpuset with alignment parses",
			rawJSON: `{"apiVersion": "v1alpha1", "cpuConfig": {"cpuset": "2-5", "alignment": "BestEffort"}}`,
			expected: ClaimConfig{
				ClaimPlacement: ClaimPlacement{
					Alignment:    v1alpha1.AlignmentBestEffort,
					AlignmentSet: true,
				},
				CPUs:    cpuset.New(2, 3, 4, 5),
				HasCPUs: true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ParseOpaqueConfig([]byte(tc.rawJSON))
			if tc.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				assert.Equal(t, ClaimConfig{}, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}
