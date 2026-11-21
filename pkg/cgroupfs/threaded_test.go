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
	"errors"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

// fakeTree is a cgroup2 tree in memory. mkdir makes a cgroup's core files, a
// controller's files appear in the children once their parent enables it in
// cgroup.subtree_control (cgroup-v2.rst:486-491), and a write to a file the
// tree lacks fails, as on the kernel's tree.
type fakeTree struct {
	fstest.MapFS
	owners  map[string]cgroupfs.Owner
	refused map[string]error
}

var controllerFiles = map[string][]string{
	"cpu":    {"cpu.max", "cpu.weight"},
	"cpuset": {"cpuset.cpus"},
}

func newFakeTree(t *testing.T, dirs ...string) *fakeTree {
	t.Helper()
	tree := &fakeTree{MapFS: fstest.MapFS{}, owners: map[string]cgroupfs.Owner{}, refused: map[string]error{}}
	for _, dir := range dirs {
		var parent string
		for _, part := range strings.Split(dir, "/") {
			parent = path.Join(parent, part)
			if err := tree.Mkdir(parent, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
				t.Fatal(err)
			}
		}
	}
	return tree
}

func (f *fakeTree) Mkdir(name string, perm fs.FileMode) error {
	if _, ok := f.MapFS[name]; ok {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrExist}
	}
	if parent := path.Dir(name); parent != "." && !f.isDir(parent) {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrNotExist}
	}
	f.MapFS[name] = &fstest.MapFile{Mode: fs.ModeDir | perm}
	for _, file := range []string{"cgroup.procs", "cgroup.threads", "cgroup.subtree_control"} {
		f.MapFS[path.Join(name, file)] = &fstest.MapFile{}
	}
	f.MapFS[path.Join(name, "cgroup.type")] = &fstest.MapFile{Data: []byte("domain")}
	for _, controller := range f.enabled(path.Dir(name)) {
		for _, file := range controllerFiles[controller] {
			f.MapFS[path.Join(name, file)] = &fstest.MapFile{}
		}
	}
	return nil
}

func (f *fakeTree) WriteFile(name string, data []byte) error {
	file, ok := f.MapFS[name]
	if !ok || file.Mode.IsDir() {
		return &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if err := f.refused[name]; err != nil {
		return &fs.PathError{Op: "write", Path: name, Err: err}
	}
	if path.Base(name) == "cgroup.subtree_control" {
		return f.enable(path.Dir(name), string(data))
	}
	file.Data = slices.Clone(data)
	return nil
}

func (f *fakeTree) Chown(name string, uid, gid int) error {
	if _, ok := f.MapFS[name]; !ok {
		return &fs.PathError{Op: "chown", Path: name, Err: fs.ErrNotExist}
	}
	f.owners[name] = cgroupfs.Owner{UID: uid, GID: gid}
	return nil
}

func (f *fakeTree) enable(dir, value string) error {
	enabled := f.enabled(dir)
	for _, token := range strings.Fields(value) {
		controller, ok := strings.CutPrefix(token, "+")
		if !ok || controllerFiles[controller] == nil {
			return &fs.PathError{Op: "write", Path: path.Join(dir, "cgroup.subtree_control"), Err: fs.ErrInvalid}
		}
		if !slices.Contains(enabled, controller) {
			enabled = append(enabled, controller)
		}
	}
	slices.Sort(enabled)
	f.MapFS[path.Join(dir, "cgroup.subtree_control")].Data = []byte(strings.Join(enabled, " "))
	for name, file := range f.MapFS {
		if !file.Mode.IsDir() || path.Dir(name) != dir {
			continue
		}
		for _, controller := range enabled {
			for _, cf := range controllerFiles[controller] {
				if _, ok := f.MapFS[path.Join(name, cf)]; !ok {
					f.MapFS[path.Join(name, cf)] = &fstest.MapFile{}
				}
			}
		}
	}
	return nil
}

func (f *fakeTree) enabled(dir string) []string {
	file, ok := f.MapFS[path.Join(dir, "cgroup.subtree_control")]
	if !ok {
		return nil
	}
	return strings.Fields(string(file.Data))
}

func (f *fakeTree) isDir(name string) bool {
	file, ok := f.MapFS[name]
	return ok && file.Mode.IsDir()
}

var errRefused = errors.New("operation not supported")

func TestBuild(t *testing.T) {
	const (
		cgroupfsRoot = "kubepods/burstable/pod9c26/2f5a"
		systemdRoot  = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod9c26.slice/cri-containerd-2f5a.scope"
	)
	vcpu := cgroupfs.Group{Name: "vcpu", CPUs: cpuset.New(16, 144), Period: 100000, Weight: 1}
	vmm := cgroupfs.Group{
		Name:   "vmm-cpu",
		CPUs:   cpuset.New(0, 1, 2, 3, 4, 5, 128, 129, 130, 131, 132, 133),
		Quota:  50000,
		Period: 100000,
		Weight: 10000,
	}
	owner := cgroupfs.Owner{UID: 2038693884, GID: 2038693884}
	built := map[string]string{
		"cgroup.subtree_control": "cpu cpuset",
		"cgroup.type":            "domain",
		"vcpu/cgroup.type":       "threaded",
		"vcpu/cpuset.cpus":       "16,144",
		"vcpu/cpu.max":           "max 100000",
		"vcpu/cpu.weight":        "1",
		"vmm-cpu/cgroup.type":    "threaded",
		"vmm-cpu/cpuset.cpus":    "0-5,128-133",
		"vmm-cpu/cpu.max":        "50000 100000",
		"vmm-cpu/cpu.weight":     "10000",
	}
	handedOver := []string{
		"cgroup.procs",
		"vcpu/cgroup.procs",
		"vcpu/cgroup.threads",
		"vmm-cpu/cgroup.procs",
		"vmm-cpu/cgroup.threads",
	}

	testCases := []struct {
		name        string
		root        string
		cgroupsPath string
		setup       func(t *testing.T, tree *fakeTree)
		expectError string
		expectIs    error
	}{
		{
			name:        "a fresh thread root",
			root:        cgroupfsRoot,
			cgroupsPath: "/kubepods/burstable/pod9c26/2f5a",
		},
		{
			name:        "a thread root the systemd driver names",
			root:        systemdRoot,
			cgroupsPath: "kubepods-burstable-pod9c26.slice:cri-containerd:2f5a",
		},
		{
			name:        "a tree built before is built again",
			root:        cgroupfsRoot,
			cgroupsPath: "/kubepods/burstable/pod9c26/2f5a",
			setup: func(t *testing.T, tree *fakeTree) {
				require.NoError(t, cgroupfs.Build(tree, cgroupfsRoot, []cgroupfs.Group{vcpu, vmm}, owner))
			},
		},
		{
			name:        "a tree lacking a child is completed",
			root:        cgroupfsRoot,
			cgroupsPath: "/kubepods/burstable/pod9c26/2f5a",
			setup: func(t *testing.T, tree *fakeTree) {
				require.NoError(t, cgroupfs.Build(tree, cgroupfsRoot, []cgroupfs.Group{vcpu}, owner))
			},
		},
		{
			name:        "a thread root that is gone",
			root:        cgroupfsRoot,
			cgroupsPath: "/kubepods/burstable/pod9c26/gone",
			expectError: "create cgroup kubepods/burstable/pod9c26/gone/vcpu",
			expectIs:    fs.ErrNotExist,
		},
		{
			name:        "a write the kernel refuses",
			root:        cgroupfsRoot,
			cgroupsPath: "/kubepods/burstable/pod9c26/2f5a",
			setup: func(t *testing.T, tree *fakeTree) {
				tree.refused[cgroupfsRoot+"/vmm-cpu/cgroup.type"] = errRefused
			},
			expectError: `write "threaded" to kubepods/burstable/pod9c26/2f5a/vmm-cpu/cgroup.type`,
			expectIs:    errRefused,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := newFakeTree(t, tc.root)
			if tc.setup != nil {
				tc.setup(t, tree)
			}
			dir, err := cgroupfs.Dir(tc.cgroupsPath)
			require.NoError(t, err)

			err = cgroupfs.Build(tree, dir, []cgroupfs.Group{vcpu, vmm}, owner)
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				require.ErrorIs(t, err, tc.expectIs)
				return
			}
			require.NoError(t, err)
			for file, value := range built {
				require.Equal(t, value, string(tree.MapFS[path.Join(tc.root, file)].Data), file)
			}
			var wantOwned []string
			for _, file := range handedOver {
				wantOwned = append(wantOwned, path.Join(tc.root, file))
			}
			require.ElementsMatch(t, wantOwned, slices.Collect(maps.Keys(tree.owners)))
			for file, got := range tree.owners {
				require.Equal(t, owner, got, file)
			}
		})
	}
}

// Repair makes a tree's missing groups and sets every group again, and hands
// over nothing: the groups it made are the caller's to hand over.
func TestRepair(t *testing.T) {
	const root = "kubepods/burstable/pod9c26/2f5a"
	vcpu := cgroupfs.Group{Name: "vcpu", CPUs: cpuset.New(16, 144), Period: 100000, Weight: 1}
	vmm := cgroupfs.Group{Name: "vmm-cpu", CPUs: cpuset.New(0, 1), Quota: 25000, Period: 100000, Weight: 10000}
	started := cgroupfs.Owner{UID: 2038693884, GID: 2038693884}
	now := cgroupfs.Owner{UID: 65532, GID: 65532}
	testCases := []struct {
		name       string
		built      []cgroupfs.Group
		expectMade []string
	}{
		{name: "a whole tree", built: []cgroupfs.Group{vcpu, vmm}},
		{name: "a tree missing one group", built: []cgroupfs.Group{vcpu}, expectMade: []string{"vmm-cpu"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := newFakeTree(t, root)
			require.NoError(t, cgroupfs.Build(tree, root, tc.built, started))
			tree.MapFS[root+"/vcpu/cpuset.cpus"].Data = []byte("0-63")
			ownedBefore := maps.Clone(tree.owners)

			made, err := cgroupfs.Repair(tree, root, []cgroupfs.Group{vcpu, vmm})
			require.NoError(t, err)
			require.Equal(t, tc.expectMade, made)
			require.Equal(t, ownedBefore, tree.owners, "Repair hands over nothing")
			require.Equal(t, "16,144", string(tree.MapFS[root+"/vcpu/cpuset.cpus"].Data), "a changed setting is set again")
			require.Equal(t, "25000 100000", string(tree.MapFS[root+"/vmm-cpu/cpu.max"].Data))
			require.Equal(t, "threaded", string(tree.MapFS[root+"/vmm-cpu/cgroup.type"].Data))

			require.NoError(t, cgroupfs.HandOver(tree, root, made, now))
			for _, name := range made {
				require.Equal(t, now, tree.owners[path.Join(root, name, "cgroup.procs")], name)
				require.Equal(t, now, tree.owners[path.Join(root, name, "cgroup.threads")], name)
			}
			require.Equal(t, started, tree.owners[root+"/vcpu/cgroup.threads"], "a group that was there keeps its owner")
		})
	}
}

func TestSetCPUs(t *testing.T) {
	const root = "kubepods/burstable/pod9c26/2f5a"
	testCases := []struct {
		name        string
		dir         string
		expectError string
		expectIs    error
	}{
		{
			name: "a group's CPUs are rewritten",
			dir:  root + "/vmm-cpu",
		},
		{
			name:        "a cgroup whose parent has no cpuset controller",
			dir:         root + "/vmm-cpu/inner",
			expectError: "kubepods/burstable/pod9c26/2f5a/vmm-cpu/inner/cpuset.cpus",
			expectIs:    fs.ErrNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := newFakeTree(t, root)
			require.NoError(t, cgroupfs.Build(tree, root, []cgroupfs.Group{{Name: "vmm-cpu", CPUs: cpuset.New(0, 1), Period: 100000, Weight: 1}}, cgroupfs.Owner{}))
			require.NoError(t, tree.Mkdir(root+"/vmm-cpu/inner", 0o755))

			err := cgroupfs.SetCPUs(tree, tc.dir, cpuset.New(2, 3, 4, 5))
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				require.ErrorIs(t, err, tc.expectIs)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "2-5", string(tree.MapFS[path.Join(tc.dir, "cpuset.cpus")].Data))
		})
	}
}

func TestMoveProcess(t *testing.T) {
	const root = "kubepods/burstable/pod9c26/2f5a"
	testCases := []struct {
		name        string
		refused     error
		expectError string
	}{
		{
			name: "the process is moved",
		},
		{
			name:        "a move the kernel refuses",
			refused:     errRefused,
			expectError: `write "4242" to kubepods/burstable/pod9c26/2f5a/vmm-cpu/cgroup.procs`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := newFakeTree(t, root+"/vmm-cpu")
			if tc.refused != nil {
				tree.refused[root+"/vmm-cpu/cgroup.procs"] = tc.refused
			}

			err := cgroupfs.MoveProcess(tree, root+"/vmm-cpu", 4242)
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
				require.ErrorIs(t, err, tc.refused)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "4242", string(tree.MapFS[root+"/vmm-cpu/cgroup.procs"].Data))
		})
	}
}
