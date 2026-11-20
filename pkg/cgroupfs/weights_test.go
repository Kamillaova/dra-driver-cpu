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

package cgroupfs_test

import (
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/stretchr/testify/require"
)

func TestWeights(t *testing.T) {
	testCases := []struct {
		name     string
		shared   [][]int64
		expected []uint64
	}{
		{
			name:     "a VM's vCPUs on owned CPUs and its housekeeping on 250m",
			shared:   [][]int64{{}, {250}},
			expected: []uint64{1, 10000},
		},
		{
			name:     "one shared group alone",
			shared:   [][]int64{{100}},
			expected: []uint64{10000},
		},
		{
			name:     "two shared groups of 256 and 512 shares",
			shared:   [][]int64{{250}, {500}},
			expected: []uint64{5000, 10000},
		},
		{
			name:     "three shared groups of 256, 512 and 1024 shares",
			shared:   [][]int64{{250}, {500}, {1000}},
			expected: []uint64{2500, 5000, 10000},
		},
		{
			name:     "a group of several shared requests weighs their sum",
			shared:   [][]int64{{250, 250}, {1000}},
			expected: []uint64{5000, 10000},
		},
		{
			name:     "two claims of one container scale together",
			shared:   [][]int64{{}, {250}, {1000}},
			expected: []uint64{1, 2500, 10000},
		},
		{
			name:     "an amount of 0 counts as 2 shares",
			shared:   [][]int64{{0}, {1000}},
			expected: []uint64{19, 10000},
		},
		{
			name:     "a group of several amounts of 0 counts as 2 shares",
			shared:   [][]int64{{0, 0}, {1000}},
			expected: []uint64{19, 10000},
		},
		{
			name:     "a group far below the largest weighs at least 1",
			shared:   [][]int64{{0}, {256000}},
			expected: []uint64{1, 10000},
		},
		{
			name:     "amounts above the kubelet's maximum shares weigh alike",
			shared:   [][]int64{{300000}, {600000}},
			expected: []uint64{10000, 10000},
		},
		{
			name:     "only owned CPUs",
			shared:   [][]int64{{}, {}},
			expected: []uint64{1, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, cgroupfs.Weights(tc.shared))
		})
	}
}
