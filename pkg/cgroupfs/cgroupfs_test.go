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

func TestDir(t *testing.T) {
	testCases := []struct {
		name        string
		cgroupsPath string
		expected    string
		expectError bool
	}{
		{
			name:        "the cgroupfs driver reports the path itself",
			cgroupsPath: "/kubepods/burstable/pod9c26/2f5a",
			expected:    "kubepods/burstable/pod9c26/2f5a",
		},
		{
			name:        "and it may already be relative",
			cgroupsPath: "kubepods/besteffort/pod9c26/2f5a",
			expected:    "kubepods/besteffort/pod9c26/2f5a",
		},
		{
			name:        "the systemd driver reports slice, prefix and name",
			cgroupsPath: "kubepods-burstable-pod9c26.slice:cri-containerd:2f5a",
			expected:    "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod9c26.slice/cri-containerd-2f5a.scope",
		},
		{
			name:        "a one-level slice expands to itself",
			cgroupsPath: "kubepods.slice:cri-containerd:2f5a",
			expected:    "kubepods.slice/cri-containerd-2f5a.scope",
		},
		{
			name:        "the root slice is the tree root",
			cgroupsPath: "-.slice:cri-containerd:2f5a",
			expected:    "cri-containerd-2f5a.scope",
		},
		{
			name:        "a container the runtime placed nowhere",
			cgroupsPath: "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := cgroupfs.Dir(tc.cgroupsPath)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, dir)
		})
	}
}
