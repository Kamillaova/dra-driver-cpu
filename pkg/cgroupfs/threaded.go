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

package cgroupfs

import (
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strconv"

	"k8s.io/utils/cpuset"
)

// The values the driver's cgroupThreadedMode setting takes besides false, its
// default.
const (
	ThreadedModeCreate = "create"
	ThreadedModePlace  = "place"
)

const (
	typeFile           = "cgroup.type"
	procsFile          = "cgroup.procs"
	threadsFile        = "cgroup.threads"
	subtreeControlFile = "cgroup.subtree_control"
	cpusFile           = "cpuset.cpus"
	maxFile            = "cpu.max"
	weightFile         = "cpu.weight"
)

// Group is one threaded child of a thread root.
type Group struct {
	Name string
	CPUs cpuset.CPUSet
	// Quota is the microseconds the group may run each Period; 0 is no limit.
	Quota  int64
	Period int64
	Weight uint64
}

// Owner is the user and group, as the host sees them, that place a
// container's threads.
type Owner struct {
	UID, GID int
}

// Build makes groups the threaded children of the thread root dir, sets their
// CPUs, limits and weights, and hands the files that move processes and
// threads into them to owner. Every step tolerates having been done, so Build
// also completes a tree that lacks a child.
func Build(fsys FS, dir string, groups []Group, owner Owner) error {
	if _, err := Repair(fsys, dir, groups); err != nil {
		return err
	}
	// The thread root's cgroup.threads stays root's: no thread belongs there,
	// and one moved there would leave its group's limit.
	if err := handOver(fsys, path.Join(dir, procsFile), owner); err != nil {
		return err
	}
	names := make([]string, 0, len(groups))
	for _, g := range groups {
		names = append(names, g.Name)
	}
	return HandOver(fsys, dir, names, owner)
}

// Repair makes the groups of the thread root dir that are missing and sets
// every group's CPUs, limit and weight again. It returns the names of the
// groups it made, whose files nobody has been handed yet.
func Repair(fsys FS, dir string, groups []Group) ([]string, error) {
	var made []string
	for _, g := range groups {
		child := path.Join(dir, g.Name)
		err := fsys.Mkdir(child, 0o755)
		if err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("create cgroup %s: %w", child, err)
		}
		if err == nil {
			made = append(made, g.Name)
		}
		// A threaded cgroup takes "threaded" again as a no-op (linux v7.2
		// kernel/cgroup/cgroup.c:3653-3655).
		if err := write(fsys, child, typeFile, "threaded"); err != nil {
			return nil, err
		}
	}
	// Threaded controllers may be enabled while the thread root still holds
	// the process (cgroup.c:3514-3526), and enabling them again is a no-op
	// (:3583-3585).
	if err := write(fsys, dir, subtreeControlFile, "+cpu +cpuset"); err != nil {
		return nil, err
	}
	for _, g := range groups {
		child := path.Join(dir, g.Name)
		if err := SetCPUs(fsys, child, g.CPUs); err != nil {
			return nil, err
		}
		if err := write(fsys, child, maxFile, cpuMax(g.Quota, g.Period)); err != nil {
			return nil, err
		}
		if err := write(fsys, child, weightFile, strconv.FormatUint(g.Weight, 10)); err != nil {
			return nil, err
		}
	}
	return made, nil
}

// HandOver hands the files that move processes and threads into the groups
// names of the thread root dir to owner.
func HandOver(fsys FS, dir string, names []string, owner Owner) error {
	for _, name := range names {
		child := path.Join(dir, name)
		if err := handOver(fsys, path.Join(child, threadsFile), owner); err != nil {
			return err
		}
		if err := handOver(fsys, path.Join(child, procsFile), owner); err != nil {
			return err
		}
	}
	return nil
}

// SetCPUs writes the cpuset of the cgroup dir.
func SetCPUs(fsys FS, dir string, cpus cpuset.CPUSet) error {
	return write(fsys, dir, cpusFile, cpus.String())
}

// MoveProcess moves every thread of the process pid into the cgroup dir.
func MoveProcess(fsys FS, dir string, pid int) error {
	return write(fsys, dir, procsFile, strconv.Itoa(pid))
}

func cpuMax(quota, period int64) string {
	p := strconv.FormatInt(period, 10)
	if quota <= 0 {
		return "max " + p
	}
	return strconv.FormatInt(quota, 10) + " " + p
}

func write(fsys FS, dir, file, value string) error {
	name := path.Join(dir, file)
	if err := fsys.WriteFile(name, []byte(value)); err != nil {
		return fmt.Errorf("write %q to %s: %w", value, name, err)
	}
	return nil
}

func handOver(fsys FS, name string, owner Owner) error {
	if err := fsys.Chown(name, owner.UID, owner.GID); err != nil {
		return fmt.Errorf("chown %s to %d:%d: %w", name, owner.UID, owner.GID, err)
	}
	return nil
}
