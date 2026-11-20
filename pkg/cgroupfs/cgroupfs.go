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
	"slices"
	"strconv"
	"strings"
	"syscall"
)

// Root is where the host's cgroup2 tree is mounted into the driver.
const Root = "/sys/fs/cgroup"

// FS is the cgroup2 tree, narrowed to the calls the driver makes.
type FS interface {
	fs.FS
	Mkdir(name string, perm fs.FileMode) error
	WriteFile(name string, data []byte) error
	Chown(name string, uid, gid int) error
}

// HostRoot is where Host finds the host cgroup2 tree, honoring HOST_ROOT when
// set.
func HostRoot() string {
	return path.Join(os.Getenv("HOST_ROOT"), Root)
}

// Host opens the host cgroup2 tree at HostRoot.
func Host() (FS, error) {
	root, err := os.OpenRoot(HostRoot())
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

// FindProcess returns the cgroup of dir's subtree, dir included, whose
// cgroup.procs lists pid, or "" where none does. No name below dir is assumed:
// a runtime may hold the process in a cgroup of its own below the one it
// reports, as crun does under the systemd driver.
func FindProcess(fsys fs.FS, dir string, pid int) (string, error) {
	want := strconv.Itoa(pid)
	var found string
	err := fs.WalkDir(fsys, dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		procs, err := fs.ReadFile(fsys, path.Join(name, procsFile))
		// A threaded cgroup's cgroup.procs cannot be read: its processes
		// belong to the thread root (cgroup-v2.rst:904-905).
		if errors.Is(err, syscall.EOPNOTSUPP) {
			return nil
		}
		if err != nil {
			return err
		}
		if slices.Contains(strings.Fields(string(procs)), want) {
			found = name
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("find pid %d below %s: %w", pid, dir, err)
	}
	return found, nil
}

// FindThreadRoot returns the cgroup of dir's subtree, dir included, whose
// cgroup.type reads "domain threaded", the kernel's mark of a thread root
// (cgroup-v2.rst:869-870), with a threaded child named one of names, or ""
// where none does. A threaded cgroup is never one, nor anything below it.
func FindThreadRoot(fsys fs.FS, dir string, names []string) (string, error) {
	var found string
	err := fs.WalkDir(fsys, dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		kind, err := cgroupType(fsys, name)
		if err != nil {
			return err
		}
		switch kind {
		case "threaded":
			return fs.SkipDir
		case "domain threaded":
		default:
			return nil
		}
		for _, child := range names {
			childKind, err := cgroupType(fsys, path.Join(name, child))
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			if childKind == "threaded" {
				found = name
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("find the thread root below %s: %w", dir, err)
	}
	return found, nil
}

func cgroupType(fsys fs.FS, dir string) (string, error) {
	kind, err := fs.ReadFile(fsys, path.Join(dir, typeFile))
	return strings.TrimSpace(string(kind)), err
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
