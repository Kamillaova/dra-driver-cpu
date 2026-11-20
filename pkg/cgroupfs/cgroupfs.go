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

// Package cgroupfs reads and writes the host's cgroup2 tree directly, below
// the cgroup a runtime made for a container.
package cgroupfs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"k8s.io/utils/cpuset"
)

// Root is where the host's cgroup2 tree is mounted into the driver.
const Root = "/sys/fs/cgroup"

// cpusetFile is the CPUs the kernel is really running the cgroup's tasks on,
// which is the cpuset after intersection with every ancestor's.
const cpusetFile = "cpuset.cpus.effective"

// FS is the cgroup2 tree, narrowed to the calls the driver makes.
type FS interface {
	fs.FS
	Mkdir(name string, perm fs.FileMode) error
	WriteFile(name string, data []byte) error
	Chown(name string, uid, gid int) error
}

// Host opens the host cgroup2 tree, honoring HOST_ROOT when set.
func Host() (FS, error) {
	root, err := os.OpenRoot(path.Join(os.Getenv("HOST_ROOT"), Root))
	if err != nil {
		return nil, fmt.Errorf("open the host cgroup2 tree: %w", err)
	}
	return &hostFS{FS: root.FS(), root: root}, nil
}

type hostFS struct {
	fs.FS
	root *os.Root
}

func (h *hostFS) Mkdir(name string, perm fs.FileMode) error {
	return h.root.Mkdir(name, perm)
}

// WriteFile opens the file without O_CREAT or O_TRUNC: the kernel makes every
// file of a cgroup, and each takes its value in one write.
func (h *hostFS) WriteFile(name string, data []byte) error {
	f, err := h.root.OpenFile(name, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	return errors.Join(err, f.Close())
}

func (h *hostFS) Chown(name string, uid, gid int) error {
	return h.root.Chown(name, uid, gid)
}

// CPUSet reads the CPUs a container is really confined to. cgroupsPath is what
// the runtime reported for it over NRI.
func CPUSet(fsys fs.FS, cgroupsPath string) (cpuset.CPUSet, error) {
	dir, err := Dir(cgroupsPath)
	if err != nil {
		return cpuset.New(), err
	}
	raw, err := fs.ReadFile(fsys, path.Join(dir, cpusetFile))
	if err != nil {
		return cpuset.New(), fmt.Errorf("read %s of %q: %w", cpusetFile, cgroupsPath, err)
	}
	cpus, err := cpuset.Parse(strings.TrimSpace(string(raw)))
	if err != nil {
		return cpuset.New(), fmt.Errorf("parse %s of %q: %w", cpusetFile, cgroupsPath, err)
	}
	return cpus, nil
}

// Dir turns the cgroups path a runtime reports into a directory under the
// cgroup2 root, in the form io/fs wants: relative and unrooted.
//
// A runtime using the cgroupfs driver reports the path itself. One using the
// systemd driver reports "slice:prefix:name", and systemd's own naming rules say
// where that lands: a unit goes in <slice>/<prefix>-<name>.scope, and a slice
// name expands on its dashes into the hierarchy that leads to it, so
// kubepods-burstable-podX.slice is kubepods.slice/kubepods-burstable.slice/
// kubepods-burstable-podX.slice. The root slice "-.slice" is the tree root
// itself and expands to nothing.
func Dir(cgroupsPath string) (string, error) {
	if cgroupsPath == "" {
		return "", errors.New("the runtime reported no cgroup for this container")
	}
	slice, prefix, name, systemd := cutSystemd(cgroupsPath)
	if !systemd {
		return path.Clean("/" + cgroupsPath)[1:], nil
	}
	return path.Join(expandSlice(slice), prefix+"-"+name+".scope"), nil
}

func cutSystemd(cgroupsPath string) (slice, prefix, name string, ok bool) {
	parts := strings.Split(cgroupsPath, ":")
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func expandSlice(slice string) string {
	if slice == "" || slice == "-.slice" {
		return ""
	}
	stem := strings.TrimSuffix(slice, ".slice")
	var dir string
	var prefix string
	for _, part := range strings.Split(stem, "-") {
		if prefix == "" {
			prefix = part
		} else {
			prefix += "-" + part
		}
		dir = path.Join(dir, prefix+".slice")
	}
	return dir
}
