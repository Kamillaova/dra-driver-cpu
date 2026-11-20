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

package driver

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// cgroupLinkMount is where a threaded container finds the link to its
	// thread root, as cgroupLinkMount/cgroupLinkName.
	cgroupLinkMount = "/var/run/dracpu"
	cgroupLinkName  = "cgroup"
	cgroupLinksDir  = "cgroup-links"
	// crun runs the init in this path below the container's cgroup (crun
	// 1.30.1 src/libcrun/cgroup.c:89-101,475).
	delegateCgroupAnnotation = "run.oci.delegate-cgroup"
)

// containerID is the form containerd gives a container's id.
var containerID = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (cp *CPUDriver) cgroupLinksDir() string {
	return filepath.Join(pluginDataDir(cp.kubeletRootDir, cp.driverName), cgroupLinksDir)
}

// makeCgroupLinkDir makes the host directory a threaded container's
// /var/run/dracpu shows, named by its id, and returns the read-only mount that
// shows it.
func (cp *CPUDriver) makeCgroupLinkDir(id string) (string, *api.Mount, error) {
	dir := filepath.Join(cp.cgroupLinksDir(), id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", nil, fmt.Errorf("make %s: %w", dir, err)
	}
	// The container's user crosses the mount's root as another user, and the
	// umask may have taken the bits it needs.
	if err := os.Chmod(dir, 0o755); err != nil {
		return "", nil, fmt.Errorf("make %s: %w", dir, err)
	}
	// runc binds a mount only from its options (v1.5.1
	// libcontainer/specconv/spec_linux.go:648-661).
	return dir, &api.Mount{Destination: cgroupLinkMount, Type: "bind", Source: dir, Options: []string{"rbind", "ro"}}, nil
}

// createdUnseen reports whether a threaded container was created while the
// driver was away, so that CreateContainer neither judged it nor made its link
// directory, which it makes last. A container whose id is not a container id
// never gets one, and counts as seen.
func (cp *CPUDriver) createdUnseen(ctr *api.Container) (bool, error) {
	if !containerID.MatchString(ctr.GetId()) {
		return false, nil
	}
	_, err := os.Lstat(filepath.Join(cp.cgroupLinksDir(), ctr.GetId()))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("container %q: look for its link directory: %w", ctr.GetName(), err)
	}
	return false, nil
}

// linkThreadRoot points a threaded container's /var/run/dracpu/cgroup at its
// thread root, as the container resolves it. A container whose creation the
// driver missed has no directory and gets no link.
func (cp *CPUDriver) linkThreadRoot(logger logr.Logger, ctr *api.Container, root string) error {
	if !containerID.MatchString(ctr.GetId()) {
		logger.Info("no link at /var/run/dracpu/cgroup: the runtime's id for the container is not a container id")
		return nil
	}
	dir := filepath.Join(cp.cgroupLinksDir(), ctr.GetId())
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		logger.Info("no link at /var/run/dracpu/cgroup: the driver did not see the container's creation, so the container has no /var/run/dracpu")
		return nil
	} else if err != nil {
		return fmt.Errorf("container %q: link its thread root: %w", ctr.GetName(), err)
	}
	target, ok := cgroupLinkTarget(ctr, root)
	if !ok {
		logger.Info("no link at /var/run/dracpu/cgroup: the container joined another cgroup namespace, whose root its /sys/fs/cgroup shows")
		return nil
	}
	if err := writeLink(dir, target); err != nil {
		return fmt.Errorf("container %q: link its thread root: %w", ctr.GetName(), err)
	}
	return nil
}

// cgroupLinkTarget returns the thread root root, a directory of the host's
// cgroup2 tree, as the container ctr sees it, or false where it cannot.
func cgroupLinkTarget(ctr *api.Container, root string) (string, bool) {
	namespaces := ctr.GetLinux().GetNamespaces()
	i := slices.IndexFunc(namespaces, func(ns *api.LinuxNamespace) bool { return ns.GetType() == "cgroup" })
	switch {
	// A container sharing the host's cgroup namespace sees the host's tree.
	case i < 0:
		return path.Join(cgroupfs.Root, root), true
	// One joining another container's sees that one's root.
	case namespaces[i].GetPath() != "":
		return "", false
	}
	if delegate := delegatePath(ctr.GetAnnotations()[delegateCgroupAnnotation]); delegate != "" &&
		(root == delegate || strings.HasSuffix(root, "/"+delegate)) {
		return path.Join(cgroupfs.Root, delegate), true
	}
	return cgroupfs.Root, true
}

// delegatePath is run.oci.delegate-cgroup's value as crun appends it to the
// container's cgroup: without its leading and trailing slashes, and with its
// empty and "." components dropped, which crun passes on and the kernel
// resolves away.
func delegatePath(value string) string {
	var parts []string
	for _, part := range strings.Split(value, "/") {
		if part != "" && part != "." {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, "/")
}

// writeLink points dir/cgroup at target, replacing a wrong link through a
// temporary name, so that a reader never finds the name missing.
func writeLink(dir, target string) error {
	link := filepath.Join(dir, cgroupLinkName)
	if current, err := os.Readlink(link); err == nil && current == target {
		return nil
	}
	temporary := filepath.Join(dir, "."+cgroupLinkName+".new")
	if err := os.Remove(temporary); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Symlink(target, temporary); err != nil {
		return err
	}
	return os.Rename(temporary, link)
}

// removeCgroupLinkDir removes a container's link directory, a missing one
// counting as removed. A failure is logged, and the next sweep removes it.
func (cp *CPUDriver) removeCgroupLinkDir(logger logr.Logger, id string) {
	if !threadedModeOn(cp.cgroupThreadedMode) || !containerID.MatchString(id) {
		return
	}
	if err := os.RemoveAll(filepath.Join(cp.cgroupLinksDir(), id)); err != nil {
		logger.Error(err, "the container's link directory stays until the driver synchronizes with the runtime again")
	}
}

// sweepCgroupLinks removes the link directory of every container the runtime
// does not list, following no link. No container is created while a plugin
// synchronizes, so none of the directories removed is one being made. A
// failure is logged and counted and fails nothing: NRI closes a plugin whose
// Synchronize fails.
func (cp *CPUDriver) sweepCgroupLinks(logger logr.Logger, containers []*api.Container) {
	if !threadedModeOn(cp.cgroupThreadedMode) {
		return
	}
	dir := cp.cgroupLinksDir()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		logger.Error(err, "not removing the link directories of containers that are gone")
		cp.metrics.RecordThreadedContainer(cp.cgroupThreadedMode, cpumetrics.ResultLinkError)
		return
	}
	listed := sets.New[string]()
	for _, ctr := range containers {
		listed.Insert(ctr.GetId())
	}
	for _, entry := range entries {
		if !entry.IsDir() || !containerID.MatchString(entry.Name()) || listed.Has(entry.Name()) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			logger.Error(err, "the link directory of a container that is gone stays until the driver synchronizes with the runtime again", "containerID", entry.Name())
			cp.metrics.RecordThreadedContainer(cp.cgroupThreadedMode, cpumetrics.ResultLinkError)
		}
	}
}
