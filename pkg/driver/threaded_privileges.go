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
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
)

// ProcessProbe reads, from the driver's own process, what the start checks of
// cgroupThreadedMode need.
type ProcessProbe interface {
	// MountInfo returns /proc/self/mountinfo.
	MountInfo() ([]byte, error)
	// EffectiveCapabilities returns the CapEff mask of /proc/self/status.
	EffectiveCapabilities() (uint64, error)
	// SharesInitCgroupNamespace reports whether the driver runs in the cgroup
	// namespace of PID 1.
	SharesInitCgroupNamespace() (bool, error)
	// EnterInitCgroupNamespace opens the cgroup namespace of PID 1 and enters
	// it, on a thread that is then discarded.
	EnterInitCgroupNamespace() error
}

// Capability numbers, linux v7.2 include/uapi/linux/capability.h:237,281.
const (
	capSysPtrace = 19
	capSysAdmin  = 21
)

func threadedModeOn(mode string) bool {
	return mode == cgroupfs.ThreadedModeCreate || mode == cgroupfs.ThreadedModePlace
}

// setUpThreadedMode runs the checks create and place make before the driver
// serves anything, and opens the host's cgroup2 tree.
func (cp *CPUDriver) setUpThreadedMode(logger logr.Logger, providers Providers, mode string) error {
	mount := cgroupfs.HostRoot()
	hostNamespace, err := checkThreadedPrivileges(providers.EnsureProcess(), mode, mount)
	if err != nil {
		return err
	}
	cgroups, err := providers.EnsureCgroupFS()
	if err != nil {
		return err
	}
	cp.cgroupThreadedMode = mode
	cp.cgroups = cgroups
	cp.cgroupHostNamespace = hostNamespace
	logger.Info("checked the privileges cgroupThreadedMode needs", "mode", mode, "mount", mount, "enterHostCgroupNamespace", hostNamespace)
	return nil
}

// checkThreadedPrivileges checks that the mount at mountPoint is the read-write
// cgroup2 tree the mode writes, and under place that the driver can move
// another container's process, naming the mount's state and the missing
// capabilities in one refusal. It reports whether such a move has to enter the
// cgroup namespace of PID 1: under nsdelegate a migration is allowed only
// between cgroups below the writer's cgroup namespace root (linux v7.2
// kernel/cgroup/cgroup.c:5374-5377).
func checkThreadedPrivileges(probe ProcessProbe, mode, mountPoint string) (bool, error) {
	mountinfo, err := probe.MountInfo()
	if err != nil {
		return false, fmt.Errorf("`cgroupThreadedMode: %s`: read the driver's mounts: %w", mode, err)
	}
	mount, found := findMount(mountinfo, mountPoint)
	if !found {
		return false, fmt.Errorf("`cgroupThreadedMode: %s` needs the host's cgroup v2 hierarchy mounted read-write at %s: nothing is mounted at %s; install the driver with the chart, which mounts it for the mode, mount the host's /sys/fs/cgroup read-write in its pod, or set `cgroupThreadedMode: false`",
			mode, mountPoint, mountPoint)
	}
	if mount.fsType != "cgroup2" {
		return false, fmt.Errorf("`cgroupThreadedMode: %s` needs the host's cgroup v2 hierarchy mounted read-write at %s: %s is %s, not cgroup2; run the node on cgroup v2, or set `cgroupThreadedMode: false`",
			mode, mountPoint, mountPoint, mount.fsType)
	}
	readOnly := !slices.Contains(mount.options, "rw")
	enter := mode == cgroupfs.ThreadedModePlace && slices.Contains(mount.superOptions, "nsdelegate")
	if enter {
		shared, err := probe.SharesInitCgroupNamespace()
		switch {
		case err == nil:
			enter = !shared
		// Reading PID 1's namespace is a ptrace access, as entering it is, so a
		// refusal leaves the answer to the attempt below.
		case !errors.Is(err, syscall.EACCES):
			return false, fmt.Errorf("`cgroupThreadedMode: place`: compare the driver's cgroup namespace with PID 1's: %w", err)
		}
	}
	if !enter {
		if readOnly {
			return false, fmt.Errorf("`cgroupThreadedMode: %s` needs the host's cgroup v2 hierarchy mounted read-write at %s: %s is mounted read-only; install the driver with the chart, which mounts it for the mode, mount the host's /sys/fs/cgroup read-write in its pod, or set `cgroupThreadedMode: false`",
				mode, mountPoint, mountPoint)
		}
		return false, nil
	}
	if err := enterHostCgroupNamespace(probe, mountPoint, readOnly); err != nil {
		return false, err
	}
	return true, nil
}

// enterHostCgroupNamespace enters PID 1's cgroup namespace once, as a move
// does, the capability bits read beforehand only to name what is missing when
// the attempt is refused.
func enterHostCgroupNamespace(probe ProcessProbe, mountPoint string, readOnly bool) error {
	caps, err := probe.EffectiveCapabilities()
	if err != nil {
		return fmt.Errorf("`cgroupThreadedMode: place`: read the driver's capabilities: %w", err)
	}
	enterErr := probe.EnterInitCgroupNamespace()
	refused := errors.Is(enterErr, syscall.EACCES) || errors.Is(enterErr, syscall.EPERM)
	if enterErr != nil && !refused {
		return fmt.Errorf("`cgroupThreadedMode: place`: enter the host's cgroup namespace: %w", enterErr)
	}
	var missing string
	if refused {
		missing = missingCapabilities(caps)
	}
	switch {
	case missing != "" && readOnly:
		return fmt.Errorf("`cgroupThreadedMode: place` needs the host's cgroup v2 hierarchy mounted read-write at %s, CAP_SYS_PTRACE and CAP_SYS_ADMIN: %s is mounted read-only; the driver lacks %s; install the driver with the chart, which sets these for the mode, fix its pod, or set `cgroupThreadedMode: false`",
			mountPoint, mountPoint, missing)
	case missing != "":
		return fmt.Errorf("`cgroupThreadedMode: place` needs the host's cgroup v2 hierarchy mounted read-write at %s, CAP_SYS_PTRACE and CAP_SYS_ADMIN: the driver lacks %s; install the driver with the chart, which sets these for the mode, fix its pod, or set `cgroupThreadedMode: false`",
			mountPoint, missing)
	case readOnly:
		return fmt.Errorf("`cgroupThreadedMode: place` needs the host's cgroup v2 hierarchy mounted read-write at %s, CAP_SYS_PTRACE and CAP_SYS_ADMIN: %s is mounted read-only; install the driver with the chart, which mounts it for the mode, mount the host's /sys/fs/cgroup read-write in its pod, or set `cgroupThreadedMode: false`",
			mountPoint, mountPoint)
	case refused:
		return fmt.Errorf("`cgroupThreadedMode: place` needs to enter the host's cgroup namespace: it was refused though the driver holds CAP_SYS_PTRACE and CAP_SYS_ADMIN (%w); run the driver in the host's user namespace, under an AppArmor profile that allows it",
			enterErr)
	}
	return nil
}

// missingCapabilities names those of the two capabilities entering PID 1's
// cgroup namespace needs that caps lacks. Opening it is a ptrace read of PID 1,
// which needs CAP_SYS_PTRACE where PID 1 holds capabilities the driver lacks
// (linux v7.2 fs/proc/namespaces.c:42-63, security/commoncap.c:164-186), and
// entering it needs CAP_SYS_ADMIN (kernel/cgroup/namespace.c:92-99).
func missingCapabilities(caps uint64) string {
	var missing []string
	for _, c := range []struct {
		bit  uint
		name string
	}{{capSysPtrace, "CAP_SYS_PTRACE"}, {capSysAdmin, "CAP_SYS_ADMIN"}} {
		if caps&(1<<c.bit) == 0 {
			missing = append(missing, c.name)
		}
	}
	return strings.Join(missing, " and ")
}

type mountEntry struct {
	options      []string
	fsType       string
	superOptions []string
}

// findMount returns the mount at mountPoint in a mountinfo file: per-mount
// options in the sixth field, the filesystem type and the superblock options
// after the separator (linux v7.2 Documentation/filesystems/proc.rst:1968-1983).
// A read-only bind of a read-write cgroup2 shows rw only among the superblock
// options. Where several mounts are stacked there the last is taken, the one
// mounted over the others: containerd mounts its own read-only cgroup2 at
// /sys/fs/cgroup before a pod's mounts (v2.4.0
// internal/cri/opts/spec_linux_opts.go).
func findMount(mountinfo []byte, mountPoint string) (mountEntry, bool) {
	var entry mountEntry
	var found bool
	for line := range strings.Lines(string(mountinfo)) {
		fields := strings.Fields(line)
		separator := slices.Index(fields, "-")
		if separator < 6 || len(fields) < separator+4 {
			continue
		}
		if unescapeMountinfo(fields[4]) != mountPoint {
			continue
		}
		entry = mountEntry{
			options:      strings.Split(fields[5], ","),
			fsType:       fields[separator+1],
			superOptions: strings.Split(fields[separator+3], ","),
		}
		found = true
	}
	return entry, found
}

// unescapeMountinfo undoes the three-digit octal escapes the kernel writes for
// a space, tab, newline or backslash in a path.
func unescapeMountinfo(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}
	var b strings.Builder
	for i := 0; i < len(field); i++ {
		if field[i] == '\\' && i+4 <= len(field) {
			if c, err := strconv.ParseUint(field[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(c))
				i += 3
				continue
			}
		}
		b.WriteByte(field[i])
	}
	return b.String()
}

// checkOwnCgroupLocked looks, at the first Synchronize under create and place,
// for the driver's own container among the runtime's and for its process in
// the cgroup2 tree at or below that container's cgroup. Found, it shows that
// the driver sees the runtime's pid namespace and the host's tree as the
// runtime names them. Callers hold stateMu.
func (cp *CPUDriver) checkOwnCgroupLocked(logger logr.Logger, containers []*api.Container) error {
	if !threadedModeOn(cp.cgroupThreadedMode) || cp.ownCgroupFound {
		return nil
	}
	pid := os.Getpid()
	i := slices.IndexFunc(containers, func(ctr *api.Container) bool {
		return int64(ctr.GetPid()) == int64(pid)
	})
	if i < 0 {
		return fmt.Errorf("`cgroupThreadedMode: %s` needs the host's pid namespace: the runtime lists no container running the driver's pid %d; install the driver with the chart, which sets this for the mode, or add `hostPID: true` to its pod",
			cp.cgroupThreadedMode, pid)
	}
	cgroupsPath := containers[i].GetLinux().GetCgroupsPath()
	dir, err := cgroupfs.Dir(cgroupsPath)
	var found string
	if err == nil {
		found, err = cgroupfs.FindProcess(cp.cgroups, dir, pid)
	}
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("`cgroupThreadedMode: %s` needs the host's /sys/fs/cgroup: it does not show the driver's cgroup %q; install the driver with the chart, which mounts it for the mode, or mount the host's /sys/fs/cgroup in its pod",
			cp.cgroupThreadedMode, cgroupsPath)
	case err != nil:
		return fmt.Errorf("`cgroupThreadedMode: %s`: %w", cp.cgroupThreadedMode, err)
	case found == "":
		return fmt.Errorf("`cgroupThreadedMode: %s` needs the host's /sys/fs/cgroup: it does not show the driver's pid %d in its cgroup %q; install the driver with the chart, which mounts it for the mode, or mount the host's /sys/fs/cgroup in its pod",
			cp.cgroupThreadedMode, pid, cgroupsPath)
	}
	cp.ownCgroupFound = true
	logger.V(2).Info("found the driver's process in the host's cgroup2 tree", "cgroup", found, "pid", pid)
	return nil
}

type hostProcess struct{}

func (hostProcess) MountInfo() ([]byte, error) {
	return os.ReadFile("/proc/self/mountinfo")
}

func (hostProcess) EffectiveCapabilities() (uint64, error) {
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(status)) {
		value, ok := strings.CutPrefix(line, "CapEff:")
		if !ok {
			continue
		}
		caps, err := strconv.ParseUint(strings.TrimSpace(value), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("parse CapEff of /proc/self/status: %w", err)
		}
		return caps, nil
	}
	return 0, errors.New("/proc/self/status has no CapEff line")
}

func (hostProcess) SharesInitCgroupNamespace() (bool, error) {
	self, err := os.Stat("/proc/self/ns/cgroup")
	if err != nil {
		return false, err
	}
	pid1, err := os.Stat("/proc/1/ns/cgroup")
	if err != nil {
		return false, err
	}
	return os.SameFile(self, pid1), nil
}
