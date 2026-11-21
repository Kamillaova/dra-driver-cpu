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
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/stretchr/testify/require"
)

func TestFindProcess(t *testing.T) {
	const scope = "system.slice/cri-containerd-2f5a.scope"
	testCases := []struct {
		name        string
		tree        fstest.MapFS
		dir         string
		expected    string
		expectError error
	}{
		{
			name: "the process in the cgroup the runtime reports",
			tree: fstest.MapFS{
				scope + "/cgroup.procs": &fstest.MapFile{Data: []byte("4242\n")},
			},
			dir:      scope,
			expected: scope,
		},
		{
			name: "the process in crun's sub-cgroup below it",
			tree: fstest.MapFS{
				scope + "/cgroup.procs":           &fstest.MapFile{},
				scope + "/container/cgroup.procs": &fstest.MapFile{Data: []byte("17\n4242\n")},
			},
			dir:      scope,
			expected: scope + "/container",
		},
		{
			name: "a pid that only begins like the process's",
			tree: fstest.MapFS{
				scope + "/cgroup.procs": &fstest.MapFile{Data: []byte("42420\n")},
			},
			dir: scope,
		},
		{
			name: "the process elsewhere in the tree",
			tree: fstest.MapFS{
				scope + "/cgroup.procs":                      &fstest.MapFile{},
				"system.slice/other.scope/cgroup.procs":      &fstest.MapFile{Data: []byte("4242\n")},
				"system.slice/cri-containerd-2f5b.scope/x/y": &fstest.MapFile{},
			},
			dir: scope,
		},
		{
			name: "a cgroup the tree does not show",
			tree: fstest.MapFS{
				"kubepods/cgroup.procs": &fstest.MapFile{Data: []byte("4242\n")},
			},
			dir:         scope,
			expectError: fs.ErrNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			found, err := cgroupfs.FindProcess(tc.tree, tc.dir, 4242)
			if tc.expectError != nil {
				require.ErrorIs(t, err, tc.expectError)
				require.ErrorContains(t, err, "find pid 4242 below "+scope)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, found)
		})
	}
}

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
